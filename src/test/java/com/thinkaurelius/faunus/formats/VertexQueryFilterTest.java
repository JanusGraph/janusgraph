package com.thinkaurelius.faunus.formats;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexQueryFilterTest extends BaseTest {

    public void testDegenerateVertexQuery() {
        Configuration config = new Configuration();
        VertexQueryFilter query = VertexQueryFilter.create(config);
        assertFalse(query.doesFilter());
        assertEquals(query.limit, Long.MAX_VALUE);
        assertEquals(query.hasContainers.size(), 0);
        assertEquals(query.direction, Direction.BOTH);
        assertEquals(query.labels.length, 0);
    }

    public void testVertexQueryConstruction() {
        Configuration config = new Configuration();
        config.set(VertexQueryFilter.FAUNUS_GRAPH_INPUT_VERTEX_QUERY_FILTER, "v.query().limit(0).direction(IN).labels('knows')");
        VertexQueryFilter query = VertexQueryFilter.create(config);
        assertTrue(query.doesFilter());
        assertEquals(query.limit, 0);
        assertEquals(query.hasContainers.size(), 0);
        assertEquals(query.direction, Direction.IN);
        assertEquals(query.labels.length, 1);
        assertEquals(query.labels[0], "knows");
    }

    public void testDefaultFilterLimitZeroTinkerGraph() throws Exception {
        Configuration config = new Configuration();
        config.set(VertexQueryFilter.FAUNUS_GRAPH_INPUT_VERTEX_QUERY_FILTER, "v.query().limit(0)");
        VertexQueryFilter query = VertexQueryFilter.create(config);
        Map<Long, FaunusVertex> graph = generateGraph(ExampleGraph.TINKERGRAPH);
        for (FaunusVertex vertex : graph.values()) {
            query.defaultFilter(vertex);
            assertEquals(((List) vertex.getEdges(Direction.IN)).size(), 0);
            assertEquals(((List) vertex.getEdges(Direction.OUT)).size(), 0);
            assertEquals(((List) vertex.getEdges(Direction.BOTH)).size(), 0);
        }
    }

    public void testDefaultFilterLimitOneTinkerGraph() throws Exception {
        Configuration config = new Configuration();
        config.set(VertexQueryFilter.FAUNUS_GRAPH_INPUT_VERTEX_QUERY_FILTER, "v.query().limit(1)");
        VertexQueryFilter query = VertexQueryFilter.create(config);
        Map<Long, FaunusVertex> graph = generateGraph(ExampleGraph.TINKERGRAPH);
        for (FaunusVertex vertex : graph.values()) {
            query.defaultFilter(vertex);
            assertEquals(((List) vertex.getEdges(Direction.BOTH)).size(), 1);
        }
    }

    public void testDefaultFilterHasTinkerGraph() throws Exception {
        Configuration config = new Configuration();
        config.set(VertexQueryFilter.FAUNUS_GRAPH_INPUT_VERTEX_QUERY_FILTER, "v.query().has('weight',0.5d,Query.Compare.LESS_THAN).limit(5)");
        VertexQueryFilter query = VertexQueryFilter.create(config);
        Map<Long, FaunusVertex> graph = generateGraph(ExampleGraph.TINKERGRAPH);

        int counter = 0;
        for (FaunusVertex vertex : graph.values()) {
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                edge.setProperty("weight", ((Number) edge.getProperty("weight")).doubleValue());
            }
            query.defaultFilter(vertex);
            assertTrue(((List) vertex.getEdges(Direction.BOTH)).size() <= 5);

            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                counter++;
                assertTrue(((Number) edge.getProperty("weight")).doubleValue() < 0.5d);
            }
        }
        assertEquals(counter, 6);
    }

}
