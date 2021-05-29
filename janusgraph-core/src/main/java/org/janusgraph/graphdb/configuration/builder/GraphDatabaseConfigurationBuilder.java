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

package org.janusgraph.graphdb.configuration.builder;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.MergedConfiguration;
import org.janusgraph.diskstorage.configuration.MixedConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.configuration.backend.builder.KCVSConfigurationBuilder;
import org.janusgraph.diskstorage.configuration.builder.ModifiableConfigurationBuilder;
import org.janusgraph.diskstorage.configuration.builder.ReadConfigurationBuilder;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.ttl.TTLKCVSManager;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.diskstorage.log.kcvs.KCVSLogManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.idmanagement.UniqueInstanceIdRetriever;

import java.time.Duration;
import java.util.Map;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_SEND_DELAY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_STORE_TTL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.MANAGEMENT_LOG;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TRANSACTION_LOG;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TRANSACTION_LOG_DEFAULT_TTL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID;

/**
 * Builder for {@link GraphDatabaseConfiguration}
 */
public class GraphDatabaseConfigurationBuilder {

    public GraphDatabaseConfiguration build(ReadConfiguration localConfig){

        Preconditions.checkNotNull(localConfig);

        BasicConfiguration localBasicConfiguration = new BasicConfiguration(ROOT_NS,localConfig, BasicConfiguration.Restriction.NONE);
        ModifiableConfiguration overwrite = new ModifiableConfiguration(ROOT_NS,new CommonsConfiguration(), BasicConfiguration.Restriction.NONE);

        final KeyColumnValueStoreManager storeManager = Backend.getStorageManager(localBasicConfiguration);
        final StoreFeatures storeFeatures = storeManager.getFeatures();

        final ReadConfiguration globalConfig = new ReadConfigurationBuilder().buildGlobalConfiguration(
            localConfig, localBasicConfiguration, overwrite, storeManager,
            new ModifiableConfigurationBuilder(), new KCVSConfigurationBuilder());

        //Copy over local config options
        ModifiableConfiguration localConfiguration = new ModifiableConfiguration(ROOT_NS, new CommonsConfiguration(), BasicConfiguration.Restriction.LOCAL);
        localConfiguration.setAll(getLocalSubset(localBasicConfiguration.getAll()));

        Configuration combinedConfig = new MixedConfiguration(ROOT_NS,globalConfig,localConfig);

        //Compute unique instance id
        String uniqueGraphId = UniqueInstanceIdRetriever.getInstance().getOrGenerateUniqueInstanceId(combinedConfig);
        overwrite.set(UNIQUE_INSTANCE_ID, uniqueGraphId);

        checkAndOverwriteTransactionLogConfiguration(combinedConfig, overwrite, storeFeatures);
        checkAndOverwriteSystemManagementLogConfiguration(combinedConfig, overwrite);

        MergedConfiguration configuration = new MergedConfiguration(overwrite,combinedConfig);

        return new GraphDatabaseConfiguration(localConfig, localConfiguration, uniqueGraphId, configuration);
    }

    private Map<ConfigElement.PathIdentifier, Object> getLocalSubset(Map<ConfigElement.PathIdentifier, Object> m) {
        return Maps.filterEntries(m, entry -> {
            assert entry.getKey().element.isOption();
            return ((ConfigOption)entry.getKey().element).isLocal();
        });
    }

    private void checkAndOverwriteTransactionLogConfiguration(Configuration combinedConfig, ModifiableConfiguration overwrite, StoreFeatures storeFeatures){

        //Default log configuration for system and tx log
        //TRANSACTION LOG: send_delay=0, ttl=2days and backend=default
        Preconditions.checkArgument(combinedConfig.get(LOG_BACKEND,TRANSACTION_LOG).equals(LOG_BACKEND.getDefaultValue()),
            "Must use default log backend for transaction log");
        Preconditions.checkArgument(!combinedConfig.has(LOG_SEND_DELAY,TRANSACTION_LOG) ||
            combinedConfig.get(LOG_SEND_DELAY, TRANSACTION_LOG).isZero(),"Send delay must be 0 for transaction log.");
        overwrite.set(LOG_SEND_DELAY, Duration.ZERO,TRANSACTION_LOG);
        if (!combinedConfig.has(LOG_STORE_TTL,TRANSACTION_LOG) && TTLKCVSManager.supportsAnyTTL(storeFeatures)) {
            overwrite.set(LOG_STORE_TTL,TRANSACTION_LOG_DEFAULT_TTL,TRANSACTION_LOG);
        }
    }

    private void checkAndOverwriteSystemManagementLogConfiguration(Configuration combinedConfig, ModifiableConfiguration overwrite){

        //SYSTEM MANAGEMENT LOG: backend=default and send_delay=0 and key_consistent=true and fixed-partitions=true
        Preconditions.checkArgument(combinedConfig.get(LOG_BACKEND,MANAGEMENT_LOG).equals(LOG_BACKEND.getDefaultValue()),
            "Must use default log backend for system log");
        Preconditions.checkArgument(!combinedConfig.has(LOG_SEND_DELAY,MANAGEMENT_LOG) ||
            combinedConfig.get(LOG_SEND_DELAY,MANAGEMENT_LOG).isZero(),"Send delay must be 0 for system log.");
        overwrite.set(LOG_SEND_DELAY, Duration.ZERO, MANAGEMENT_LOG);
        Preconditions.checkArgument(!combinedConfig.has(KCVSLog.LOG_KEY_CONSISTENT, MANAGEMENT_LOG) ||
            combinedConfig.get(KCVSLog.LOG_KEY_CONSISTENT, MANAGEMENT_LOG), "Management log must be configured to be key-consistent");
        overwrite.set(KCVSLog.LOG_KEY_CONSISTENT,true,MANAGEMENT_LOG);
        Preconditions.checkArgument(!combinedConfig.has(KCVSLogManager.LOG_FIXED_PARTITION,MANAGEMENT_LOG)
            || combinedConfig.get(KCVSLogManager.LOG_FIXED_PARTITION,MANAGEMENT_LOG),"Fixed partitions must be enabled for management log");
        overwrite.set(KCVSLogManager.LOG_FIXED_PARTITION,true,MANAGEMENT_LOG);
    }

}
