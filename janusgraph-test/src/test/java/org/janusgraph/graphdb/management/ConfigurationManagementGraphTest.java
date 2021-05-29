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

package org.janusgraph.graphdb.management;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.management.utils.ConfigurationManagementGraphNotEnabledException;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.janusgraph.core.schema.SchemaStatus.ENABLED;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.management.ConfigurationManagementGraph.PROPERTY_GRAPH_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConfigurationManagementGraphTest {

    @AfterEach
    public void removeStaticSingletonAfterTest() {
        JanusGraphManager.shutdownJanusGraphManager();
        ConfigurationManagementGraph.shutdownConfigurationManagementGraph();
    }

    @Test
    public void shouldReindexIfPropertyKeyExists() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        final MapConfiguration config = ConfigurationUtil.loadMapConfiguration(map);
        final StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(new CommonsConfiguration(config)));

        final String propertyKeyName = "Created_Using_Template";
        final Class dataType = Boolean.class;
        JanusGraphManagement management = graph.openManagement();
        management.makePropertyKey(propertyKeyName).dataType(dataType).make();
        management.commit();

        // Instantiate the ConfigurationManagementGraph Singleton
        // This is purposefully done after a property key is created to ensure that a REDINDEX is initiated
        new ConfigurationManagementGraph(graph);

        management = graph.openManagement();
        final JanusGraphIndex index = management.getGraphIndex("Created_Using_Template_Index");
        final PropertyKey propertyKey = management.getPropertyKey("Created_Using_Template");
        assertNotNull(index);
        assertNotNull(propertyKey);
        assertEquals(ENABLED, index.getIndexStatus(propertyKey));
        management.commit();
    }

    @Test
    public void shouldCloseAllTxsIfIndexExists() {
        final StandardJanusGraph graph = (StandardJanusGraph) JanusGraphFactory.open("inmemory");

        // Emulate ConfigurationManagementGraph indices already exists
        JanusGraphManagement management = graph.openManagement();
        PropertyKey key = management.makePropertyKey("some_property").dataType(String.class).make();
        management.buildIndex("Created_Using_Template_Index", Vertex.class).addKey(key).buildCompositeIndex();
        management.buildIndex("Template_Index", Vertex.class).addKey(key).buildCompositeIndex();
        management.buildIndex("Graph_Name_Index", Vertex.class).addKey(key).buildCompositeIndex();
        management.commit();

        new ConfigurationManagementGraph(graph);

        assertEquals(0, graph.getOpenTransactions().size());
    }

    @Test
    public void shouldAlwaysUseACleanTx() throws ConfigurationManagementGraphNotEnabledException {
        final StandardJanusGraph graph = (StandardJanusGraph) JanusGraphFactory.open("inmemory");
        new ConfigurationManagementGraph(graph);
        final ConfigurationManagementGraph configurationManagementGraph = ConfigurationManagementGraph.getInstance();

        // Create a configuration
        final Map<String, Object> map = new HashMap<>();
        map.put(PROPERTY_GRAPH_NAME, "tx_test_graph");
        configurationManagementGraph.createConfiguration(ConfigurationUtil.loadMapConfiguration(map));

        // Get the vertex id from the new configuration
        long vertexId = (Long) graph.traversal().V().limit(1).next().id();

        // These will use a traversal to read the configurations creating a new transaction automatically
        assertEquals(1, configurationManagementGraph.getConfigurations().size());

        // Remove the configuration from outside the ConfigurationManagementGraph, this
        // could happen from another node in a cluster.
        JanusGraphTransaction tx = graph.newTransaction();
        tx.getVertex(vertexId).remove();
        tx.commit();

        // If the same transaction is being reused the traversal will be resolved without hitting
        // the store returning the wrong value.
        assertEquals(0, configurationManagementGraph.getConfigurations().size());
    }
}
