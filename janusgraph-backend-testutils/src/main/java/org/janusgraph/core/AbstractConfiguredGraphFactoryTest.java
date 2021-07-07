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

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.management.ConfigurationManagementGraph;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.management.utils.ConfigurationManagementGraphNotEnabledException;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Set;
import javax.script.Bindings;
import javax.script.SimpleBindings;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

public abstract class AbstractConfiguredGraphFactoryTest {

    private static JanusGraphManager gm;

    /**
     * Getter for the settings to use to instantiate the graph supporting the
     * ConfigurationManagementGraph instance.
     * @return a MapConfiguration instance
     */
    protected abstract MapConfiguration getManagementConfig();

    /**
     * Getter for the settings to use to create the template configurations
     * (createTemplateConfiguration)
     * @return a MapConfiguration instance
     */
    protected abstract MapConfiguration getTemplateConfig();

    /**
     * Getter for the settings to use to create the graph configurations (create)
     * @return a MapConfiguration instance
     */
    protected abstract MapConfiguration getGraphConfig();

    @BeforeEach
    public void setup() {
        if (gm != null)
            return;
        gm = new JanusGraphManager(new Settings());
        final MapConfiguration config = getManagementConfig();
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
        final MapConfiguration graphConfig = getGraphConfig();
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
    public void graphConfigurationShouldBeWhatWeExpect() throws Exception {
        final MapConfiguration graphConfig = getGraphConfig();
        final String graphName = graphConfig.getString(GRAPH_NAME.toStringWithoutRoot());

        try {
            ConfiguredGraphFactory.createConfiguration(getGraphConfig());

            final String backend = graphConfig.getString(STORAGE_BACKEND.toStringWithoutRoot());
            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.open(graphName);

            assertNotNull(graph);
            assertEquals(graphName, graph.getConfiguration().getConfiguration().get(GRAPH_NAME));
            assertEquals(backend, graph.getConfiguration().getConfiguration().get(STORAGE_BACKEND));
        } finally {
            ConfiguredGraphFactory.removeConfiguration(graphName);
            ConfiguredGraphFactory.close(graphName);
        }
    }

    @Test
    public void shouldCreateAndGetGraphUsingTemplateConfiguration() throws Exception {
        try {
            ConfiguredGraphFactory.createTemplateConfiguration(getTemplateConfig());
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
            final MapConfiguration templateConfig = getTemplateConfig();
            ConfiguredGraphFactory.createTemplateConfiguration(templateConfig);

            final String backend = templateConfig.getString(STORAGE_BACKEND.toStringWithoutRoot());
            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.create("graph1");
            final StandardJanusGraph graph1 = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");

            assertNotNull(graph);
            assertEquals(graph, graph1);
            assertEquals("graph1", graph.getConfiguration().getConfiguration().get(GRAPH_NAME));
            assertEquals(backend, graph.getConfiguration().getConfiguration().get(STORAGE_BACKEND));

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
        final MapConfiguration graphConfig = getGraphConfig();
        final String graphName = graphConfig.getString(GRAPH_NAME.toStringWithoutRoot());

        ConfiguredGraphFactory.createConfiguration(graphConfig);
        ConfiguredGraphFactory.removeConfiguration(graphName);

        RuntimeException graph1 = assertThrows(RuntimeException.class, () -> ConfiguredGraphFactory.open(graphName));
        assertEquals("Please create configuration for this graph using the " +
            "ConfigurationManagementGraph#createConfiguration API.", graph1.getMessage());
    }

    @Test
    public void shouldFailToCreateGraphAfterRemoveTemplateConfiguration() {
        ConfiguredGraphFactory.createTemplateConfiguration(getTemplateConfig());
        ConfiguredGraphFactory.removeTemplateConfiguration();

        RuntimeException graph1 = assertThrows(RuntimeException.class, () -> ConfiguredGraphFactory.create("graph1"));
        assertEquals("Please create a template Configuration using the " +
            "ConfigurationManagementGraph#createTemplateConfiguration API.", graph1.getMessage());
    }

    @Test
    public void shouldFailToOpenGraphAfterRemoveConfiguration() {
        final MapConfiguration graphConfig = getGraphConfig();
        final String graphName = graphConfig.getString(GRAPH_NAME.toStringWithoutRoot());

        ConfiguredGraphFactory.createConfiguration(graphConfig);
        ConfiguredGraphFactory.removeConfiguration(graphName);

        RuntimeException graph1 = assertThrows(RuntimeException.class, () -> ConfiguredGraphFactory.create(graphName));
        assertEquals("Please create a template Configuration using the " +
            "ConfigurationManagementGraph#createTemplateConfiguration API.", graph1.getMessage());
    }

    @Test
    public void updateConfigurationShouldRemoveGraphFromCache() throws Exception {
        final MapConfiguration graphConfig = getGraphConfig();
        final String graphName = graphConfig.getString(GRAPH_NAME.toStringWithoutRoot());

        try {
            ConfiguredGraphFactory.createConfiguration(graphConfig);
            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.open(graphName);
            assertNotNull(graph);

            final Map<String, Object> map = graphConfig.getMap();
            map.put(STORAGE_BACKEND.toStringWithoutRoot(), "bogusBackend");
            ConfiguredGraphFactory.updateConfiguration(graphName, ConfigurationUtil.loadMapConfiguration(map));
            assertNull(gm.getGraph(graphName));
            // we should throw an error since the config has been updated and we are attempting
            // to open a bogus backend
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                ConfiguredGraphFactory.open(graphName));
            assertEquals("Could not find implementation class: bogusBackend", exception.getMessage());
        } finally {
            ConfiguredGraphFactory.removeConfiguration(graphName);
            ConfiguredGraphFactory.close(graphName);
        }
    }

    @Test
    public void removeConfigurationShouldRemoveGraphFromCache() throws Exception {
        final MapConfiguration graphConfig = getGraphConfig();
        final String graphName = graphConfig.getString(GRAPH_NAME.toStringWithoutRoot());

        try {
            ConfiguredGraphFactory.createConfiguration(graphConfig);
            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.open(graphName);
            assertNotNull(graph);

            ConfiguredGraphFactory.removeConfiguration(graphName);
            assertNull(gm.getGraph(graphName));
        } finally {
            ConfiguredGraphFactory.removeConfiguration(graphName);
            ConfiguredGraphFactory.close(graphName);
        }
    }

    @Test
    public void dropGraphShouldRemoveGraphFromCache() throws Exception {
        final MapConfiguration graphConfig = getGraphConfig();
        final String graphName = graphConfig.getString(GRAPH_NAME.toStringWithoutRoot());

        try {
            ConfiguredGraphFactory.createConfiguration(graphConfig);

            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.open(graphName);
            assertNotNull(graph);

            ConfiguredGraphFactory.drop(graphName);
            assertNull(gm.getGraph(graphName));
            assertTrue(graph.isClosed());
        } finally {
            ConfiguredGraphFactory.removeConfiguration(graphName);
            ConfiguredGraphFactory.close(graphName);
        }
    }

    @Test
    public void shouldBeAbleToRemoveBogusConfiguration() throws Exception {
        final MapConfiguration graphConfig = getGraphConfig();
        final String graphName = graphConfig.getString(GRAPH_NAME.toStringWithoutRoot());

        try {
            ConfiguredGraphFactory.createConfiguration(graphConfig);
            ConfiguredGraphFactory.removeConfiguration(graphName);
        } finally {
            ConfiguredGraphFactory.removeConfiguration(graphName);
            ConfiguredGraphFactory.close(graphName);
        }
    }

    @Test
    public void shouldCreateTwoGraphsUsingSameTemplateConfiguration() throws Exception {
        try {
            ConfiguredGraphFactory.createTemplateConfiguration(getTemplateConfig());
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
            ConfiguredGraphFactory.createTemplateConfiguration(getTemplateConfig());
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
        final MapConfiguration graphConfig = getGraphConfig();
        final String graphName = graphConfig.getString(GRAPH_NAME.toStringWithoutRoot());
        final String graphNameTraversal = graphName + "_traversal";

        try {
            ConfiguredGraphFactory.createConfiguration(graphConfig);
            final JanusGraphManager jgm = JanusGraphManager.getInstance();
            final Bindings bindings = new SimpleBindings();
            jgm.configureGremlinExecutor(mockGremlinExecutor(bindings));
            final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.open(graphName);
            jgm.putTraversalSource(ConfiguredGraphFactory.toTraversalSourceName(graphName), graph.traversal());
            assertNotNull(jgm.getGraph(graphName));
            assertEquals(ConfiguredGraphFactory.toTraversalSourceName(graphName),
                jgm.getTraversalSourceNames().iterator().next());
            // Confirm the graph and traversal source were added to the Gremlin Script Engine bindings
            assertTrue(bindings.containsKey(graphName));
            assertTrue(bindings.containsKey(graphNameTraversal));
            // Drop the graph and confirm that the graph and traversal source
            ConfiguredGraphFactory.drop(graphName);
            assertNull(jgm.getGraph(graphName));
            assertTrue(jgm.getTraversalSourceNames().isEmpty());
            // Confirm the graph and traversal source were removed from the Gremlin Script Engine bindings
            assertFalse(bindings.containsKey(graphName));
            assertFalse(bindings.containsKey(graphNameTraversal));
        } finally {
            ConfiguredGraphFactory.removeConfiguration(graphName);
            ConfiguredGraphFactory.close(graphName);
        }
    }

    @Test
    public void shouldGetGraphNames() throws Exception {
        try {
            ConfiguredGraphFactory.createTemplateConfiguration(getTemplateConfig());
            final StandardJanusGraph graph1 = (StandardJanusGraph) ConfiguredGraphFactory.create("graph1");
            final StandardJanusGraph graph2 = (StandardJanusGraph) ConfiguredGraphFactory.create("graph2");

            assertNotNull(graph1);
            assertNotNull(graph2);

            Set<String> graphNames = ConfiguredGraphFactory.getGraphNames();

            assertEquals(2, graphNames.size());
            assertTrue(graphNames.contains("graph1"));
            assertTrue(graphNames.contains("graph2"));

            assertEquals("graph1", graph1.getConfiguration().getConfiguration().get(GRAPH_NAME));
            assertEquals("graph2", graph2.getConfiguration().getConfiguration().get(GRAPH_NAME));
        } finally {
            ConfiguredGraphFactory.removeConfiguration("graph1");
            ConfiguredGraphFactory.removeConfiguration("graph2");
            ConfiguredGraphFactory.close("graph1");
            ConfiguredGraphFactory.close("graph2");
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

