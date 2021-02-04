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

package org.janusgraph.core;

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineManager;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.management.ConfigurationManagementGraph;
import org.janusgraph.graphdb.management.utils.ConfigurationManagementGraphNotEnabledException;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;

import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.commons.configuration.MapConfiguration;

import java.util.Map;
import java.util.HashMap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.mockito.Mockito;

import javax.script.Bindings;
import javax.script.SimpleBindings;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;

public class ConfiguredGraphFactoryTest {
    private static final JanusGraphManager gm;
    static {
        gm = new JanusGraphManager(new Settings());
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        final MapConfiguration config = new MapConfiguration(map);
        final StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(new CommonsConfiguration(config)));
        // Instantiate the ConfigurationManagementGraph Singleton
        new ConfigurationManagementGraph(graph);
    }

    @AfterEach
    public void cleanUp() {
        ConfiguredGraphFactory.removeTemplateConfiguration();
    }

    @Test
    public void shouldGetConfigurationManagementGraphInstance() throws ConfigurationManagementGraphNotEnabledException {
        final ConfigurationManagementGraph thisInstance = ConfigurationManagementGraph.getInstance();
        assertNotNull(thisInstance);
    }

    @Test
    public void shouldOpenGraphUsingConfiguration() throws Exception {
        try {
            final Map<String, Object> map = new HashMap<>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
            map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
            ConfiguredGraphFactory.createConfiguration(new MapConfiguration(map));
            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");
            assertNotNull(graph);
        } finally {
            ConfiguredGraphFactory.removeConfiguration("graph1");
            ConfiguredGraphFactory.close("graph1");
        }
    }

    @Test
    public void graphConfigurationShouldBeWhatWeExpect() throws Exception {
        try {
            final Map<String, Object> map = new HashMap<>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
            map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
            ConfiguredGraphFactory.createConfiguration(new MapConfiguration(map));
            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");
            assertNotNull(graph);
            assertEquals("graph1", graph.getConfiguration().getConfiguration().get(GRAPH_NAME));
            assertEquals("inmemory", graph.getConfiguration().getConfiguration().get(STORAGE_BACKEND));
        } finally {
            ConfiguredGraphFactory.removeConfiguration("graph1");
            ConfiguredGraphFactory.close("graph1");
        }
    }

    @Test
    public void shouldCreateAndGetGraphUsingTemplateConfiguration() throws Exception {
        try {
            final Map<String, Object> map = new HashMap<>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
            ConfiguredGraphFactory.createTemplateConfiguration(new MapConfiguration(map));
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
    public void graphConfigurationShouldBeWhatWeExpectWhenUsingTemplateConfiguration()
        throws Exception {

        try {
            final Map<String, Object> map = new HashMap<>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
            ConfiguredGraphFactory.createTemplateConfiguration(new MapConfiguration(map));
            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.create("graph1");
            final StandardJanusGraph graph1 = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");

            assertNotNull(graph);
            assertEquals(graph, graph1);
            assertEquals("graph1", graph.getConfiguration().getConfiguration().get(GRAPH_NAME));
            assertEquals("inmemory", graph.getConfiguration().getConfiguration().get(STORAGE_BACKEND));

        } finally {
            ConfiguredGraphFactory.removeConfiguration("graph1");
            ConfiguredGraphFactory.close("graph1");
        }
    }

    @Test
    public void shouldThrowConfigurationDoesNotExistError() {
        RuntimeException graph1 = assertThrows(RuntimeException.class, () -> ConfiguredGraphFactory.open("graph1"));
        assertEquals("Please create configuration for this graph using the " +
            "ConfigurationManagementGraph#createConfiguration API.", graph1.getMessage());
    }

    @Test
    public void shouldThrowTemplateConfigurationDoesNotExistError() {
        RuntimeException graph1 = assertThrows(RuntimeException.class, () -> ConfiguredGraphFactory.create("graph1"));
        assertEquals("Please create a template Configuration using the " +
            "ConfigurationManagementGraph#createTemplateConfiguration API.", graph1.getMessage());
    }

    @Test
    public void shouldFailToOpenNewGraphAfterRemoveConfiguration() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
        ConfiguredGraphFactory.createConfiguration(new MapConfiguration(map));
        ConfiguredGraphFactory.removeConfiguration("graph1");

        RuntimeException graph1 = assertThrows(RuntimeException.class, () -> ConfiguredGraphFactory.open("graph1"));
        assertEquals("Please create configuration for this graph using the " +
            "ConfigurationManagementGraph#createConfiguration API.", graph1.getMessage());
    }

    @Test
    public void shouldFailToCreateGraphAfterRemoveTemplateConfiguration() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        ConfiguredGraphFactory.createTemplateConfiguration(new MapConfiguration(map));
        ConfiguredGraphFactory.removeTemplateConfiguration();

        RuntimeException graph1 = assertThrows(RuntimeException.class, () -> ConfiguredGraphFactory.create("graph1"));
        assertEquals("Please create a template Configuration using the " +
            "ConfigurationManagementGraph#createTemplateConfiguration API.", graph1.getMessage());
    }

    @Test
    public void shouldFailToOpenGraphAfterRemoveConfiguration() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
        ConfiguredGraphFactory.createConfiguration(new MapConfiguration(map));
        ConfiguredGraphFactory.removeConfiguration("graph1");

        RuntimeException graph1 = assertThrows(RuntimeException.class, () -> ConfiguredGraphFactory.create("graph1"));
        assertEquals("Please create a template Configuration using the " +
            "ConfigurationManagementGraph#createTemplateConfiguration API.", graph1.getMessage());
    }

    @Test
    public void updateConfigurationShouldRemoveGraphFromCache() throws Exception {
        try {
            final Map<String, Object> map = new HashMap<>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
            map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
            ConfiguredGraphFactory.createConfiguration(new MapConfiguration(map));
            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");
            assertNotNull(graph);

            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "bogusBackend");
            ConfiguredGraphFactory.updateConfiguration("graph1", new MapConfiguration(map));
            assertNull(gm.getGraph("graph1"));
            // we should throw an error since the config has been updated and we are attempting
            // to open a bogus backend
            IllegalArgumentException graph1 = assertThrows(IllegalArgumentException.class, () -> {
                final StandardJanusGraph graph2 = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");
            });
            assertEquals("Could not find implementation class: bogusBackend", graph1.getMessage());
        } finally {
            ConfiguredGraphFactory.removeConfiguration("graph1");
            ConfiguredGraphFactory.close("graph1");
        }
    }

    @Test
    public void removeConfigurationShouldRemoveGraphFromCache() throws Exception {
        try {
            final Map<String, Object> map = new HashMap<>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
            map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
            ConfiguredGraphFactory.createConfiguration(new MapConfiguration(map));
            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");
            assertNotNull(graph);

            ConfiguredGraphFactory.removeConfiguration("graph1");
            assertNull(gm.getGraph("graph1"));
        } finally {
            ConfiguredGraphFactory.removeConfiguration("graph1");
            ConfiguredGraphFactory.close("graph1");
        }
    }

    @Test
    public void shouldBeAbleToRemoveBogusConfiguration() throws Exception {
        try {
            final Map<String, Object> map = new HashMap<>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "bogusBackend");
            map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
            ConfiguredGraphFactory.createConfiguration(new MapConfiguration(map));
            ConfiguredGraphFactory.removeConfiguration("graph1");
        } finally {
            ConfiguredGraphFactory.removeConfiguration("graph1");
            ConfiguredGraphFactory.close("graph1");
        }
    }

    @Test
    public void shouldCreateTwoGraphsUsingSameTemplateConfiguration() throws Exception {
        try {
            final Map<String, Object> map = new HashMap<>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
            ConfiguredGraphFactory.createTemplateConfiguration(new MapConfiguration(map));
            final StandardJanusGraph graph1 = (StandardJanusGraph) ConfiguredGraphFactory.create("graph1");
            final StandardJanusGraph graph2 = (StandardJanusGraph) ConfiguredGraphFactory.create("graph2");

            assertNotNull(graph1);
            assertNotNull(graph2);

            assertEquals("graph1", graph1.getConfiguration().getConfiguration().get(GRAPH_NAME));
            assertEquals("graph2", graph2.getConfiguration().getConfiguration().get(GRAPH_NAME));
        } finally {
            ConfiguredGraphFactory.removeConfiguration("graph1");
            ConfiguredGraphFactory.removeConfiguration("graph2");
            ConfiguredGraphFactory.close("graph1");
            ConfiguredGraphFactory.close("graph2");
        }
    }

    @Test
    public void ensureCallingGraphCloseResultsInNewGraphReferenceOnNextCallToOpen() throws Exception {
        try {
            final Map<String, Object> map = new HashMap<>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
            ConfiguredGraphFactory.createTemplateConfiguration(new MapConfiguration(map));
            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.create("graph1");
            assertNotNull(graph);
            assertEquals("graph1", graph.getConfiguration().getConfiguration().get(GRAPH_NAME));
            graph.close();
            assertTrue(graph.isClosed());

            final StandardJanusGraph newGraph = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");

            assertFalse(newGraph.isClosed());
        } finally {
            ConfiguredGraphFactory.removeConfiguration("graph1");
            ConfiguredGraphFactory.close("graph1");
        }
    }

    @Test
    public void dropShouldCleanUpTraversalSourceAndBindings() throws Exception {
        try {
            final String graphName = "graph1";
            final Map<String, Object> map = new HashMap<>();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
            map.put(GRAPH_NAME.toStringWithoutRoot(), graphName);
            ConfiguredGraphFactory.createConfiguration(new MapConfiguration(map));
            final JanusGraphManager jgm = JanusGraphManager.getInstance();
            final Bindings bindings = new SimpleBindings();
            jgm.configureGremlinExecutor(mockGremlinExecutor(bindings));
            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.open(graphName);
            jgm.putTraversalSource(ConfiguredGraphFactory.toTraversalSourceName(graphName), graph.traversal());
            assertNotNull(jgm.getGraph(graphName));
            assertEquals(ConfiguredGraphFactory.toTraversalSourceName(graphName),
                jgm.getTraversalSourceNames().iterator().next());
            // Confirm the graph and traversal source were added to the Gremlin Script Engine bindings
            assertTrue(bindings.containsKey("graph1"));
            assertTrue(bindings.containsKey("graph1_traversal"));
            // Drop the graph and confirm that the graph and traversal source
            ConfiguredGraphFactory.drop(graphName);
            assertNull(jgm.getGraph(graphName));
            assertTrue(jgm.getTraversalSourceNames().isEmpty());
            // Confirm the graph and traversal source were removed from the Gremlin Script Engine bindings
            assertFalse(bindings.containsKey("graph1"));
            assertFalse(bindings.containsKey("graph1_traversal"));
        } finally {
            ConfiguredGraphFactory.removeConfiguration("graph1");
        }
    }

    /**
     * Build a mock Gremlin Executor that can be used to confirm binding management happens correct on
     * ConfiguredGraphFactory create and drops.
     */
    private static GremlinExecutor mockGremlinExecutor(final Bindings bindings) {
        final GremlinExecutor gremlinExecutor = Mockito.mock(GremlinExecutor.class);
        final GremlinScriptEngineManager scriptEngineManager = Mockito.mock(GremlinScriptEngineManager.class);
        Mockito.when(gremlinExecutor.getScriptEngineManager()).thenReturn(scriptEngineManager);
        Mockito.when(gremlinExecutor.getScriptEngineManager().getBindings()).thenReturn(bindings);
        Mockito.doAnswer(invocation -> {
            bindings.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(scriptEngineManager).put(any(String.class), any(Object.class));
        Mockito.doAnswer(invocation -> bindings.get(invocation.getArgument(0)))
            .when(scriptEngineManager).get(any(String.class));
        return gremlinExecutor;
    }
}

