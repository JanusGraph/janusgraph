package org.janusgraph.graphdb.tinkerpop.gremlin.server.util;

import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.jupiter.api.Test;

import javax.script.Bindings;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class DefaultJanusGraphManagerTest {
    @Test
    public void shouldReturnGraphs() throws Exception {
        final Settings settings = Settings.read("src/test/resources/default-janus-graph-manager.yaml");
        final GraphManager graphManager = new DefaultJanusGraphManager(settings);
        final Set<String> graphNames = graphManager.getGraphNames();

        assertNotNull(graphNames);
        assertEquals(6, graphNames.size());

        assertTrue(graphNames.contains("graph"));
        assertTrue(graphNames.contains("classic"));
        assertTrue(graphNames.contains("modern"));
        assertTrue(graphNames.contains("crew"));
        assertTrue(graphNames.contains("sink"));
        assertTrue(graphNames.contains("grateful"));
        assertTrue(graphManager.getGraph("graph") instanceof StandardJanusGraph);
    }

    @Test
    public void shouldGetAsBindings() throws Exception {
        final Settings settings = Settings.read("src/test/resources/default-janus-graph-manager.yaml");
        final GraphManager graphManager = new DefaultJanusGraphManager(settings);
        final Bindings bindings = graphManager.getAsBindings();

        assertNotNull(bindings);
        assertEquals(6, bindings.size());
        assertTrue(bindings.containsKey("graph"));
        assertTrue(bindings.containsKey("classic"));
        assertTrue(bindings.containsKey("modern"));
        assertTrue(bindings.containsKey("crew"));
        assertTrue(bindings.containsKey("sink"));
        assertTrue(bindings.containsKey("grateful"));
        assertTrue(bindings.get("graph") instanceof StandardJanusGraph);;
    }

    @Test
    public void shouldGetGraph() throws Exception {
        final Settings settings = Settings.read("src/test/resources/default-janus-graph-manager.yaml");
        final GraphManager graphManager = new DefaultJanusGraphManager(settings);
        final Graph graph = graphManager.getGraph("graph");

        assertNotNull(graph);
        assertTrue(graph instanceof StandardJanusGraph);
    }

    @Test
    public void shouldGetDynamicallyAddedGraph() throws Exception {
        final Settings settings = Settings.read("src/test/resources/default-janus-graph-manager.yaml");
        final GraphManager graphManager = new DefaultJanusGraphManager(settings);
        final Graph graph = graphManager.getGraph("graph"); //fake out a graph instance
        graphManager.putGraph("newGraph", graph);

        final Set<String> graphNames = graphManager.getGraphNames();
        assertNotNull(graphNames);
        assertEquals(7, graphNames.size());
        assertTrue(graphNames.contains("newGraph"));
        assertTrue(graphNames.contains("graph"));
        assertTrue(graphNames.contains("classic"));
        assertTrue(graphNames.contains("modern"));
        assertTrue(graphNames.contains("crew"));
        assertTrue(graphNames.contains("sink"));
        assertTrue(graphNames.contains("grateful"));
        assertTrue(graphManager.getGraph("newGraph") instanceof StandardJanusGraph);
    }

    @Test
    public void shouldNotGetRemovedGraph() throws Exception {
        final Settings settings = Settings.read("src/test/resources/default-janus-graph-manager.yaml");
        final GraphManager graphManager = new DefaultJanusGraphManager(settings);
        final Graph graph = graphManager.getGraph("graph"); //fake out a graph instance
        graphManager.putGraph("newGraph", graph);
        final Set<String> graphNames = graphManager.getGraphNames();
        assertNotNull(graphNames);
        assertEquals(7, graphNames.size());
        assertTrue(graphNames.contains("newGraph"));
        assertTrue(graphManager.getGraph("newGraph") instanceof StandardJanusGraph);

        graphManager.removeGraph("newGraph");

        final Set<String> graphNames2 = graphManager.getGraphNames();
        assertEquals(6, graphNames2.size());
        assertFalse(graphNames2.contains("newGraph"));
    }

    @Test
    public void openGraphShouldReturnExistingGraph() throws Exception {
        final Settings settings = Settings.read("src/test/resources/default-janus-graph-manager.yaml");
        final GraphManager graphManager = new DefaultJanusGraphManager(settings);

        final Graph graph = graphManager.openGraph("graph", null);
        assertNotNull(graph);
        assertTrue(graph instanceof StandardJanusGraph);
    }

    @Test
    public void openGraphShouldReturnNewGraphUsingThunk() throws Exception {
        final Settings settings = Settings.read("src/test/resources/default-janus-graph-manager.yaml");
        final GraphManager graphManager = new DefaultJanusGraphManager(settings);

        final Graph graph = graphManager.getGraph("graph"); //fake out graph instance

        final Graph newGraph = graphManager.openGraph("newGraph", (String gName) -> graph);

        assertNotNull(graph);
        assertTrue(graph instanceof StandardJanusGraph);
        assertSame(graph, newGraph);
    }
}
