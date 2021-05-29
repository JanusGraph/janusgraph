// Copyright 2018 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.configuration.builder;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.janusgraph.core.JanusGraphConfigurationException;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;
import org.janusgraph.diskstorage.configuration.backend.KCVSConfiguration;
import org.janusgraph.diskstorage.configuration.backend.builder.KCVSConfigurationBuilder;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.JanusGraphConstants;
import org.janusgraph.graphdb.configuration.validator.CompatibilityValidator;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.util.system.LoggerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ALLOW_STALE_CONFIG;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ALLOW_UPGRADE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDS_STORE_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INITIAL_JANUSGRAPH_VERSION;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INITIAL_STORAGE_VERSION;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOCK_LOCAL_MEDIATOR_GROUP;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS;

/**
 * Builder to build {@link ReadConfiguration} instance of global configuration
 */
public class ReadConfigurationBuilder {

    private static final Logger log = LoggerFactory.getLogger(ReadConfigurationBuilder.class);

    static final String BACKLEVEL_STORAGE_VERSION_EXCEPTION = "The storage version on the client or server is lower than the storage version of the graph: graph storage version %s vs. client storage version %s when opening graph %s.";
    static final String INCOMPATIBLE_STORAGE_VERSION_EXCEPTION = "Storage version is incompatible with current client: graph storage version %s vs. client storage version %s when opening graph %s.";

    public ReadConfiguration buildGlobalConfiguration(ReadConfiguration localConfig,
                                                      BasicConfiguration localBasicConfiguration,
                                                      ModifiableConfiguration overwrite,
                                                      KeyColumnValueStoreManager storeManager,
                                                      ModifiableConfigurationBuilder modifiableConfigurationBuilder,
                                                      KCVSConfigurationBuilder kcvsConfigurationBuilder){
        //Read out global configuration
        try (KCVSConfiguration keyColumnValueStoreConfiguration =
                 kcvsConfigurationBuilder.buildStandaloneGlobalConfiguration(storeManager,localBasicConfiguration)){

            // If lock prefix is unspecified, specify it now
            if (!localBasicConfiguration.has(LOCK_LOCAL_MEDIATOR_GROUP)) {
                overwrite.set(LOCK_LOCAL_MEDIATOR_GROUP, storeManager.getName());
            }

            //Freeze global configuration if not already frozen!
            ModifiableConfiguration globalWrite = modifiableConfigurationBuilder.buildGlobalWrite(keyColumnValueStoreConfiguration);

            if (!globalWrite.isFrozen()) {
                //Copy over global configurations
                globalWrite.setAll(getGlobalSubset(localBasicConfiguration.getAll()));

                setupJanusGraphVersion(globalWrite);
                setupStorageVersion(globalWrite);
                setupTimestampProvider(globalWrite, localBasicConfiguration, storeManager);

                globalWrite.freezeConfiguration();
            } else {

                String graphName = localConfig.get(GRAPH_NAME.toStringWithoutRoot(), String.class);
                final boolean upgradeAllowed = isUpgradeAllowed(globalWrite, localBasicConfiguration);

                if (upgradeAllowed) {
                    setupUpgradeConfiguration(graphName, globalWrite);
                } else {
                    checkJanusGraphStorageVersionEquality(globalWrite, graphName);
                }

                checkJanusGraphVersion(globalWrite, localBasicConfiguration, keyColumnValueStoreConfiguration, overwrite);
                checkOptionsWithDiscrepancies(globalWrite, localBasicConfiguration, overwrite);
            }
            return keyColumnValueStoreConfiguration.asReadConfiguration();
        }
    }

    private void setupUpgradeConfiguration(String graphName, ModifiableConfiguration globalWrite){
        // If the graph doesn't have a storage version set it and update version
        if (!globalWrite.has(INITIAL_STORAGE_VERSION)) {
            janusGraphVersionsWithDisallowedUpgrade(globalWrite);
            log.info("graph.storage-version has been upgraded from 1 to {} and graph.janusgraph-version has been upgraded from {} to {} on graph {}",
                JanusGraphConstants.STORAGE_VERSION, globalWrite.get(INITIAL_JANUSGRAPH_VERSION), JanusGraphConstants.VERSION, graphName);
            return;
        }
        int storageVersion = Integer.parseInt(JanusGraphConstants.STORAGE_VERSION);
        int initialStorageVersion = Integer.parseInt(globalWrite.get(INITIAL_STORAGE_VERSION));
        // If the storage version of the client or server opening the graph is lower than the graph's storage version throw an exception
        if (initialStorageVersion > storageVersion) {
            throw new JanusGraphException(String.format(BACKLEVEL_STORAGE_VERSION_EXCEPTION, globalWrite.get(INITIAL_STORAGE_VERSION), JanusGraphConstants.STORAGE_VERSION, graphName));
        }
        // If the graph has a storage version, but it's lower than the client or server opening the graph upgrade the version and storage version
        if (initialStorageVersion < storageVersion) {
            janusGraphVersionsWithDisallowedUpgrade(globalWrite);
            log.info("graph.storage-version has been upgraded from {} to {} and graph.janusgraph-version has been upgraded from {} to {} on graph {}",
                globalWrite.get(INITIAL_STORAGE_VERSION), JanusGraphConstants.STORAGE_VERSION, globalWrite.get(INITIAL_JANUSGRAPH_VERSION), JanusGraphConstants.VERSION, graphName);
        } else {
            log.warn("Warning graph.allow-upgrade is currently set to true on graph {}. Please set graph.allow-upgrade to false in your properties file.", graphName);
        }
    }

    private void janusGraphVersionsWithDisallowedUpgrade(ModifiableConfiguration globalWrite){
        globalWrite.set(INITIAL_JANUSGRAPH_VERSION, JanusGraphConstants.VERSION);
        globalWrite.set(TITAN_COMPATIBLE_VERSIONS, JanusGraphConstants.VERSION);
        globalWrite.set(INITIAL_STORAGE_VERSION, JanusGraphConstants.STORAGE_VERSION);
        globalWrite.set(ALLOW_UPGRADE, false);
    }

    private void setupJanusGraphVersion(ModifiableConfiguration globalWrite){
        Preconditions.checkArgument(!globalWrite.has(INITIAL_JANUSGRAPH_VERSION), "Database has already been initialized but not frozen");
        globalWrite.set(INITIAL_JANUSGRAPH_VERSION, JanusGraphConstants.VERSION);
    }

    private void setupStorageVersion(ModifiableConfiguration globalWrite){
        Preconditions.checkArgument(!globalWrite.has(INITIAL_STORAGE_VERSION),"Database has already been initialized but not frozen");
        globalWrite.set(INITIAL_STORAGE_VERSION,JanusGraphConstants.STORAGE_VERSION);
    }

    private void setupTimestampProvider(ModifiableConfiguration globalWrite, BasicConfiguration localBasicConfiguration,
                                        KeyColumnValueStoreManager storeManager){
        /* If the configuration does not explicitly set a timestamp provider and
         * the storage backend both supports timestamps and has a preference for
         * a specific timestamp provider, then apply the backend's preference.
         */
        if (!localBasicConfiguration.has(TIMESTAMP_PROVIDER)) {
            StoreFeatures f = storeManager.getFeatures();
            final TimestampProviders backendPreference;
            if (f.hasTimestamps() && null != (backendPreference = f.getPreferredTimestamps())) {
                globalWrite.set(TIMESTAMP_PROVIDER, backendPreference);
                log.info("Set timestamps to {} according to storage backend preference",
                    LoggerUtil.sanitizeAndLaunder(globalWrite.get(TIMESTAMP_PROVIDER)));
            } else {
                globalWrite.set(TIMESTAMP_PROVIDER, TIMESTAMP_PROVIDER.getDefaultValue());
                log.info("Set default timestamp provider {}",
                    LoggerUtil.sanitizeAndLaunder(globalWrite.get(TIMESTAMP_PROVIDER)));
            }
        } else {
            log.info("Using configured timestamp provider {}", localBasicConfiguration.get(TIMESTAMP_PROVIDER));
        }
    }

    private Map<ConfigElement.PathIdentifier, Object> getGlobalSubset(Map<ConfigElement.PathIdentifier, Object> m) {
        return Maps.filterEntries(m, entry -> {
            assert entry != null;
            assert entry.getKey().element.isOption();
            return ((ConfigOption)entry.getKey().element).isGlobal();
        });
    }

    private Map<ConfigElement.PathIdentifier, Object> getManagedSubset(Map<ConfigElement.PathIdentifier, Object> m) {
        return Maps.filterEntries(m, entry -> {
            assert entry != null;
            assert entry.getKey().element.isOption();
            return ((ConfigOption)entry.getKey().element).isManaged();
        });
    }

    private void checkJanusGraphStorageVersionEquality(ModifiableConfiguration globalWrite, String graphName){
        if (!Objects.equals(globalWrite.get(INITIAL_STORAGE_VERSION), JanusGraphConstants.STORAGE_VERSION)) {
            String storageVersion = (globalWrite.has(INITIAL_STORAGE_VERSION)) ? globalWrite.get(INITIAL_STORAGE_VERSION) : "1";
            throw new JanusGraphException(String.format(INCOMPATIBLE_STORAGE_VERSION_EXCEPTION, storageVersion, JanusGraphConstants.STORAGE_VERSION, graphName));
        }
    }

    private void checkJanusGraphVersion(ModifiableConfiguration globalWrite, BasicConfiguration localBasicConfiguration,
                                        KCVSConfiguration keyColumnValueStoreConfiguration, ModifiableConfiguration overwrite){
        if(globalWrite.get(INITIAL_JANUSGRAPH_VERSION) == null){

            log.info("JanusGraph version has not been initialized");

            CompatibilityValidator.validateBackwardCompatibilityWithTitan(
                globalWrite.get(TITAN_COMPATIBLE_VERSIONS), localBasicConfiguration.get(IDS_STORE_NAME));

            setTitanIDStoreNameIfKeystoreNotExists(keyColumnValueStoreConfiguration, overwrite);
        }
    }

    private void setTitanIDStoreNameIfKeystoreNotExists(KCVSConfiguration keyColumnValueStoreConfiguration, ModifiableConfiguration overwrite){
        boolean keyStoreExists = keyColumnValueStoreConfiguration.get(IDS_STORE_NAME.getName(), IDS_STORE_NAME.getDatatype()) != null;
        if (!keyStoreExists) {
            log.info("Setting {} to {} for Titan compatibility", IDS_STORE_NAME.getName(), JanusGraphConstants.TITAN_ID_STORE_NAME);
            overwrite.set(IDS_STORE_NAME, JanusGraphConstants.TITAN_ID_STORE_NAME);
        }
    }

    private boolean isUpgradeAllowed(ModifiableConfiguration globalWrite, BasicConfiguration localBasicConfiguration){
        if (localBasicConfiguration.has(ALLOW_UPGRADE)) {
            return localBasicConfiguration.get(ALLOW_UPGRADE);
        } else if (globalWrite.has(ALLOW_UPGRADE)) {
            return globalWrite.get(ALLOW_UPGRADE);
        }
        return ALLOW_UPGRADE.getDefaultValue();
    }

    private void checkOptionsWithDiscrepancies(ModifiableConfiguration globalWrite, BasicConfiguration localBasicConfiguration,
                                               ModifiableConfiguration overwrite){
        final boolean managedOverridesAllowed = isManagedOverwritesAllowed(globalWrite, localBasicConfiguration);
        Set<String> optionsWithDiscrepancies = getOptionsWithDiscrepancies(globalWrite, localBasicConfiguration, overwrite, managedOverridesAllowed);

        if (optionsWithDiscrepancies.size() > 0 && !managedOverridesAllowed) {
            final String template = "Local settings present for one or more globally managed options: [%s].  These options are controlled through the %s interface; local settings have no effect.";
            throw new JanusGraphConfigurationException(String.format(template, String.join(", ", optionsWithDiscrepancies), ManagementSystem.class.getSimpleName()));
        }
    }

    private boolean isManagedOverwritesAllowed(ModifiableConfiguration globalWrite, BasicConfiguration localBasicConfiguration){
        if (localBasicConfiguration.has(ALLOW_STALE_CONFIG)) {
            return localBasicConfiguration.get(ALLOW_STALE_CONFIG);
        } else if (globalWrite.has(ALLOW_STALE_CONFIG)) {
            return globalWrite.get(ALLOW_STALE_CONFIG);
        }
        return ALLOW_STALE_CONFIG.getDefaultValue();
    }

    /**
     * Check for disagreement between local and backend values for GLOBAL(_OFFLINE) and FIXED options
     * The point of this check is to find edits to the local config which have no effect (and therefore likely indicate misconfiguration)
     *
     * @return Options with discrepancies
     */
    private Set<String> getOptionsWithDiscrepancies(ModifiableConfiguration globalWrite, BasicConfiguration localBasicConfiguration,
                                                    ModifiableConfiguration overwrite, boolean managedOverridesAllowed){
        Set<String> optionsWithDiscrepancies = new HashSet<>();

        for (Map.Entry<ConfigElement.PathIdentifier, Object> entry : getManagedSubset(localBasicConfiguration.getAll()).entrySet()) {
            ConfigElement.PathIdentifier pathId = entry.getKey();
            assert pathId.element.isOption();
            ConfigOption<?> configOption = (ConfigOption<?>)pathId.element;
            Object localValue = entry.getValue();

            // Get the storage backend's setting and compare with localValue
            Object storeValue;
            if (globalWrite.has(configOption, pathId.umbrellaElements)) {
                storeValue = globalWrite.get(configOption, pathId.umbrellaElements);
            } else {
                storeValue = configOption.getDefaultValue();
            }

            // Check if the value is to be overwritten
            if (overwrite.has(configOption, pathId.umbrellaElements))
            {
                storeValue = overwrite.get(configOption, pathId.umbrellaElements);
            }

            // Most validation predicate implementations disallow null, but we can't assume that here
            final boolean match = Objects.equals(localValue , storeValue);

            // Log each option with value disagreement between local and backend configs
            if (!match) {
                final String fullOptionName = ConfigElement.getPath(pathId.element, pathId.umbrellaElements);
                final String template = "Local setting {}={} (Type: {}) is overridden by globally managed value ({}).  Use the {} interface instead of the local configuration to control this setting.";
                Object[] replacements = new Object[]{fullOptionName, localValue, configOption.getType(), storeValue, ManagementSystem.class.getSimpleName()};
                if (managedOverridesAllowed) { // Lower log severity when this is enabled
                    log.warn(template, replacements);
                } else {
                    log.error(template, replacements);
                }
                optionsWithDiscrepancies.add(fullOptionName);
            }
        }
        return optionsWithDiscrepancies;
    }

}
