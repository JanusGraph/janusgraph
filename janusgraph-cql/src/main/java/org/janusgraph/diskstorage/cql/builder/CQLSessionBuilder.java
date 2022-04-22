// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.diskstorage.cql.builder;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.cql.CQLConfigOptions;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class CQLSessionBuilder {

    /**
     * @param configuration configuration to use
     * @param baseHostnames will be used only if base configuration is enabled
     * @param baseDefaultPort will be used only if base configuration is enabled
     * @param baseConfigurationLoaderBuilder will be used only if base configuration is enabled
     * @return Returns constructed CqlSession
     */
    public CqlSession build(Configuration configuration, String[] baseHostnames, int baseDefaultPort, Duration baseConnectionTimeoutMS, CQLProgrammaticConfigurationLoaderBuilder baseConfigurationLoaderBuilder) throws PermanentBackendException {

        final List<String> contactPoints = new ArrayList<>(baseHostnames.length);

        for(String contactPoint : baseHostnames){
            if(!contactPoint.contains(":")){
                contactPoint += ":"+baseDefaultPort;
            }
            contactPoints.add(contactPoint);
        }

        DriverConfigLoader driverConfigLoader;
        if(configuration.get(CQLConfigOptions.BASE_PROGRAMMATIC_CONFIGURATION_ENABLED)){
            driverConfigLoader = baseConfigurationLoaderBuilder.build(configuration, contactPoints, baseConnectionTimeoutMS);
        } else {
            driverConfigLoader = null;
        }

        Optional<Supplier<Config>> internalConfigurationSupplier = getInternalConfigSupplier(configuration, driverConfigLoader == null);
        final CqlSessionBuilder builder = CqlSession.builder();

        if(internalConfigurationSupplier.isPresent()){
            DriverConfigLoader internalDriverConfigLoader = new DefaultDriverConfigLoader(
                internalConfigurationSupplier.get(), false);
            if(driverConfigLoader == null){
                driverConfigLoader = internalDriverConfigLoader;
            } else {
                driverConfigLoader = DriverConfigLoader.compose(internalDriverConfigLoader, driverConfigLoader);
            }
        }

        if(driverConfigLoader != null){
            builder.withConfigLoader(driverConfigLoader);
        }

        return builder.build();
    }

    private Optional<Supplier<Config>> getInternalConfigSupplier(Configuration configuration, boolean withFallbackDefaultConfiguration) throws PermanentBackendException {
        boolean hasFileConfiguration = configuration.has(CQLConfigOptions.FILE_CONFIGURATION);
        boolean hasResourceConfiguration = configuration.has(CQLConfigOptions.RESOURCE_CONFIGURATION);
        boolean hasStringConfiguration = configuration.has(CQLConfigOptions.STRING_CONFIGURATION);
        boolean hasUrlConfiguration = configuration.has(CQLConfigOptions.URL_CONFIGURATION);

        boolean hasAnyInternalConfiguration = hasFileConfiguration
            || hasResourceConfiguration
            || hasStringConfiguration
            || hasUrlConfiguration;

        if(!hasAnyInternalConfiguration){
            return Optional.empty();
        }

        final URL url;
        if(hasUrlConfiguration){
            String stringUrlRepresentation = configuration.get(CQLConfigOptions.URL_CONFIGURATION);
            try {
                url = new URL(stringUrlRepresentation);
            } catch (MalformedURLException e) {
                throw new PermanentBackendException("Malformed URL: "+stringUrlRepresentation, e);
            }
        } else {
            url = null;
        }

        return Optional.of(() -> {
            ConfigFactory.invalidateCaches();
            Config config = ConfigFactory.defaultOverrides();
            if(hasFileConfiguration){
                String fileConfigurationPath = configuration.get(CQLConfigOptions.FILE_CONFIGURATION);
                config = config.withFallback(ConfigFactory.parseFileAnySyntax(new File(fileConfigurationPath)));
            }

            if(hasResourceConfiguration){
                String resourceConfigurationPath = configuration.get(CQLConfigOptions.RESOURCE_CONFIGURATION);
                config = config.withFallback(ConfigFactory.parseResourcesAnySyntax(resourceConfigurationPath,
                    ConfigParseOptions.defaults().setClassLoader(Thread.currentThread().getContextClassLoader())));
            }

            if(hasStringConfiguration){
                String stringConfiguration = configuration.get(CQLConfigOptions.STRING_CONFIGURATION);
                config = config.withFallback(ConfigFactory.parseString(stringConfiguration));
            }

            if(hasUrlConfiguration){
                config = config.withFallback(ConfigFactory.parseURL(url));
            }

            if(withFallbackDefaultConfiguration){
                config = config.withFallback(ConfigFactory.defaultReference(CqlSession.class.getClassLoader()));
            }

            config = config.resolve();

            return config.getConfig(DefaultDriverConfigLoader.DEFAULT_ROOT_PATH);
        });
    }

}
