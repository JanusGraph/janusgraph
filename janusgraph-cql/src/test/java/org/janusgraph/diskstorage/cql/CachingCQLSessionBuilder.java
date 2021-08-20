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

package org.janusgraph.diskstorage.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.cql.builder.CQLProgrammaticConfigurationLoaderBuilder;
import org.janusgraph.diskstorage.cql.builder.CQLSessionBuilder;

import java.time.Duration;
import java.util.Map;

public class CachingCQLSessionBuilder extends CQLSessionBuilder {

    private final Map<String,CqlSession> sessions;

    private final String keyspaceName;

    public CachingCQLSessionBuilder(Map<String, CqlSession> sessions, String keyspaceName){
        this.sessions = sessions;
        this.keyspaceName = keyspaceName;
    }

    @Override
    public CqlSession build(Configuration configuration, String[] baseHostnames, int baseDefaultPort, Duration baseConnectionTimeoutMS, CQLProgrammaticConfigurationLoaderBuilder baseConfigurationLoaderBuilder) throws PermanentBackendException{

        if (!sessions.containsKey(keyspaceName)) {
            sessions.put(keyspaceName, super.build(configuration, baseHostnames, baseDefaultPort, baseConnectionTimeoutMS, baseConfigurationLoaderBuilder));
        }
        return sessions.get(keyspaceName);
    }

}
