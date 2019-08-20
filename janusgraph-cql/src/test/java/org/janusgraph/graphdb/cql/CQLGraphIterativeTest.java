// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.graphdb.cql;

import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.cql.CachingCQLStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.graphdb.JanusGraphIterativeBenchmark;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class CQLGraphIterativeTest extends JanusGraphIterativeBenchmark {
    @Container
    public static final JanusGraphCassandraContainer cqlContainer = new JanusGraphCassandraContainer();

    @Override
    public WriteConfiguration getConfiguration() {
        return cqlContainer.getConfiguration(getClass().getSimpleName()).getConfiguration();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        return new CachingCQLStoreManager(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,getConfiguration(), BasicConfiguration.Restriction.NONE));
    }
}
