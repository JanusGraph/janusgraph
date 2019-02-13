// Copyright 2019 JanusGraph Authors
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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;

public class CachingCQLStoreManager extends CQLStoreManager {

    private static Cluster cluster;

    private static final Map<String,Session> sessions = new HashMap<>();

    public CachingCQLStoreManager(final Configuration configuration) throws BackendException {
        super(configuration);
    }

    @Override
    Cluster initializeCluster() throws PermanentBackendException {
        if (cluster == null || cluster.isClosed()) {
            cluster = super.initializeCluster();
        }
        return cluster;
    }

    @Override
    Session initializeSession(final String keyspaceName) {
        if (!sessions.containsKey(keyspaceName)) {
            sessions.put(keyspaceName, super.initializeSession(keyspaceName));
        }
        return sessions.get(keyspaceName);
    }

    @Override
    public void close() {
        if (this.storageConfig.get(DROP_ON_CLEAR)) {
            sessions.values().forEach(Session::close);
            sessions.clear();
        }
        this.executorService.shutdownNow();
    }

}
