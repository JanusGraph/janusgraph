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

import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.management.ConfigurationManagementGraph;
import org.janusgraph.graphdb.management.utils.ConfigurationManagementGraphNotEnabledException;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;

import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.commons.configuration.MapConfiguration;

import java.util.Map;
import java.util.HashMap;

import org.junit.Test;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.equalTo;

public class ConfiguredGraphFactoryTest {
    private static final JanusGraphManager gm;
    static {
        gm = new JanusGraphManager(new Settings());
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        final MapConfiguration config = new MapConfiguration(map);
        final StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfiguration(new CommonsConfiguration(config)));
        // Instantiate the ConfigurationManagementGraph Singleton
        new ConfigurationManagementGraph(graph);
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @After
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
        thrown.expect(RuntimeException.class);
        thrown.expectMessage(equalTo("Please create configuration for this graph using the " +
                                     "ConfigurationManagementGraph#createConfiguration API."));
        ConfiguredGraphFactory.open("graph1");
    }

    @Test
    public void shouldThrowTemplateConfigurationDoesNotExistError() {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage(equalTo("Please create a template Configuration using the " +
                                     "ConfigurationManagementGraph#createTemplateConfiguration API."));
        ConfiguredGraphFactory.create("graph1");
    }

    @Test
    public void shouldFailToOpenNewGraphAfterRemoveConfiguration() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
        ConfiguredGraphFactory.createConfiguration(new MapConfiguration(map));
        ConfiguredGraphFactory.removeConfiguration("graph1");
        thrown.expect(RuntimeException.class);
        thrown.expectMessage(equalTo("Please create configuration for this graph using the " +
                                     "ConfigurationManagementGraph#createConfiguration API."));
        ConfiguredGraphFactory.open("graph1");
    }

    @Test
    public void shouldFailToCreateGraphAfterRemoveTemplateConfiguration() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        ConfiguredGraphFactory.createTemplateConfiguration(new MapConfiguration(map));
        ConfiguredGraphFactory.removeTemplateConfiguration();

        thrown.expect(RuntimeException.class);
        thrown.expectMessage(equalTo("Please create a template Configuration using the " +
                                     "ConfigurationManagementGraph#createTemplateConfiguration API."));
        ConfiguredGraphFactory.create("graph1");
    }

    @Test
    public void shouldFailToOpenGraphAfterRemoveConfiguration() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        map.put(GRAPH_NAME.toStringWithoutRoot(), "graph1");
        ConfiguredGraphFactory.createConfiguration(new MapConfiguration(map));
        ConfiguredGraphFactory.removeConfiguration("graph1");
        thrown.expect(RuntimeException.class);
        thrown.expectMessage(equalTo("Please create a template Configuration using the " +
                                     "ConfigurationManagementGraph#createTemplateConfiguration API."));
        ConfiguredGraphFactory.create("graph1");
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
            thrown.expect(IllegalArgumentException.class);
            thrown.expectMessage(equalTo("Could not find implementation class: bogusBackend"));
            final StandardJanusGraph graph2 = (StandardJanusGraph) ConfiguredGraphFactory.open("graph1");
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
}

