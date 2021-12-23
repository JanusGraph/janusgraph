// Copyright 2022 JanusGraph Authors
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

package org.janusgraph.diskstorage.es;

import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.JanusGraphCustomIdIndexTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class CQLElasticsearchCustomIdTest extends JanusGraphCustomIdIndexTest {

    @Container
    protected static JanusGraphElasticsearchContainer esr = new JanusGraphElasticsearchContainer();

    @Container
    private static JanusGraphCassandraContainer cql = new JanusGraphCassandraContainer();

    @Override
    protected ModifiableConfiguration getModifiableConfiguration() {
        String[] indexBackends = getIndexBackends();
        ModifiableConfiguration config =  esr.setConfiguration(
            cql.getConfiguration(CQLElasticsearchCustomIdTest.class.getName()), indexBackends);
        for (String indexBackend : indexBackends) {
            config.set(GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE, 3, indexBackend);
        }
        return config;
    }
}
