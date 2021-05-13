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

package org.janusgraph.core.cql;

import com.datastax.driver.core.Session;
import org.apache.commons.configuration2.MapConfiguration;
import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.core.AbstractConfiguredGraphFactoryTest;
import org.janusgraph.core.ConfiguredGraphFactory;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashMap;
import java.util.Map;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_PORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@Testcontainers
public class CQLConfiguredGraphFactoryTest extends AbstractConfiguredGraphFactoryTest {

    @Container
    public static final JanusGraphCassandraContainer cqlContainer = new JanusGraphCassandraContainer();

    protected MapConfiguration getManagementConfig() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "cql");
        map.put(STORAGE_HOSTS.toStringWithoutRoot(), cqlContainer.getContainerIpAddress());
        map.put(STORAGE_PORT.toStringWithoutRoot(), cqlContainer.getMappedCQLPort());
        return ConfigurationUtil.loadMapConfiguration(map);
    }

    protected MapConfiguration getTemplateConfig() {
        return getManagementConfig();
    }

    protected MapConfiguration getGraphConfig() {
        final Map<String, Object> map = getTemplateConfig().getMap();
        map.put(GRAPH_NAME.toStringWithoutRoot(), "cql_test_graph_name");
        return ConfigurationUtil.loadMapConfiguration(map);
    }

    protected MapConfiguration getTemplateConfigWithMultiHosts() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "cql");
        final String host = cqlContainer.getContainerIpAddress();
        // Add multiple hosts delimited by comma. ConfiguredGraphFactory should handle this properly and parse it into
        // a list of hosts when establishing CQL connections
        map.put(STORAGE_HOSTS.toStringWithoutRoot(), host + "," + host);
        map.put(STORAGE_PORT.toStringWithoutRoot(), cqlContainer.getMappedCQLPort());
        // we should let comma delimited values remain as they are when generating and storing the configuration in the graph.
        // thus, we shall not set list delimiter here.
        return new MapConfiguration(map);
    }

    protected MapConfiguration getGraphConfigWithMultiHosts() {
        final Map<String, Object> map = getTemplateConfigWithMultiHosts().getMap();
        map.put(GRAPH_NAME.toStringWithoutRoot(), "cql_test_graph_name");
        return new MapConfiguration(map);
    }

    @Test
    public void templateConfigurationShouldSupportMultiHosts() throws Exception {
        try {
            ConfiguredGraphFactory.createTemplateConfiguration(getTemplateConfigWithMultiHosts());
            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.create("graph1");
            final StandardJanusGraph graph1 = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");
            assertNotNull(graph);
            assertEquals(graph, graph1);
        } finally {
            ConfiguredGraphFactory.removeConfiguration("graph1");
            ConfiguredGraphFactory.close("graph1");
        }
    }

    @Test
    public void createConfigurationShouldSupportMultiHosts() throws Exception {
        final MapConfiguration graphConfig = getGraphConfigWithMultiHosts();
        final String graphName = graphConfig.getString(GRAPH_NAME.toStringWithoutRoot());
        try {
            ConfiguredGraphFactory.createConfiguration(graphConfig);
            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.open(graphName);
            assertNotNull(graph);
        } finally {
            ConfiguredGraphFactory.removeConfiguration(graphName);
            ConfiguredGraphFactory.close(graphName);
        }
    }

    @Test
    public void dropGraphShouldRemoveGraphKeyspace() throws Exception {
        final MapConfiguration graphConfig = getGraphConfig();
        final String graphName = graphConfig.getString(GRAPH_NAME.toStringWithoutRoot());

        try {
            ConfiguredGraphFactory.createConfiguration(graphConfig);

            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.open(graphName);
            assertNotNull(graph);

            ConfiguredGraphFactory.drop(graphName);
            Session cql = cqlContainer.getCluster().connect();
            Object graph_keyspace = cql.execute("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = ?", graphName).one();
            cql.close();

            assertNull(graph_keyspace);
        } finally {
            ConfiguredGraphFactory.removeConfiguration(graphName);
            ConfiguredGraphFactory.close(graphName);
        }
    }
}

