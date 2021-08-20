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
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.cql.CQLConfigOptions;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

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


        final CqlSessionBuilder builder = CqlSession.builder();

        Stack<DriverConfigLoader> driverConfigLoadersToUse = new Stack<>();

        if(configuration.get(CQLConfigOptions.BASE_PROGRAMMATIC_CONFIGURATION_ENABLED)){
            driverConfigLoadersToUse.push(baseConfigurationLoaderBuilder.build(configuration, contactPoints, baseConnectionTimeoutMS));
        }

        if(configuration.has(CQLConfigOptions.URL_CONFIGURATION)){
            String stringUrlRepresentation = configuration.get(CQLConfigOptions.URL_CONFIGURATION);
            URL url;
            try {
                url = new URL(stringUrlRepresentation);
            } catch (MalformedURLException e) {
                throw new PermanentBackendException("Malformed URL: "+stringUrlRepresentation, e);
            }
            driverConfigLoadersToUse.push(DriverConfigLoader.fromUrl(url));
        }

        if(configuration.has(CQLConfigOptions.STRING_CONFIGURATION)){
            String stringConfiguration = configuration.get(CQLConfigOptions.STRING_CONFIGURATION);
            driverConfigLoadersToUse.push(DriverConfigLoader.fromString(stringConfiguration));
        }

        if(configuration.has(CQLConfigOptions.RESOURCE_CONFIGURATION)){
            String resourceConfigurationPath = configuration.get(CQLConfigOptions.RESOURCE_CONFIGURATION);
            driverConfigLoadersToUse.push(DriverConfigLoader.fromClasspath(resourceConfigurationPath));
        }

        if(configuration.has(CQLConfigOptions.FILE_CONFIGURATION)){
            String fileConfigurationPath = configuration.get(CQLConfigOptions.FILE_CONFIGURATION);
            driverConfigLoadersToUse.push(DriverConfigLoader.fromFile(new File(fileConfigurationPath)));
        }

        if(!driverConfigLoadersToUse.empty()){
            DriverConfigLoader composedDriverConfigLoader = driverConfigLoadersToUse.pop();
            while (!driverConfigLoadersToUse.empty()){
                composedDriverConfigLoader = DriverConfigLoader.compose(composedDriverConfigLoader, driverConfigLoadersToUse.pop());
            }
            builder.withConfigLoader(composedDriverConfigLoader);
        }

        return builder.build();
    }

}
