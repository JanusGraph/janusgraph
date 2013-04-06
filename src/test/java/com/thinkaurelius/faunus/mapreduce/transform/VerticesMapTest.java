package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VerticesMapTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new VerticesMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testVerticesWithNoPaths() throws Exception {
        Configuration config = new Configuration();
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), mapReduceDriver);

        assertEquals(graph.size(), 6);
        for (FaunusVertex vertex : graph.values()) {
            assertEquals(vertex.pathCount(), 1);
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                assertEquals(((FaunusEdge) edge).pathCount(), 0);
            }

            try {
                vertex.getPaths();
                assertTrue(false);
            } catch (IllegalStateException e) {
                assertTrue(true);
            }
        }

        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesMap.Counters.EDGES_PROCESSED).getValue(), 12);
        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesMap.Counters.VERTICES_PROCESSED).getValue(), 6);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testVerticesWithPaths() throws Exception {
        Configuration config = new Configuration();
        config.setBoolean(FaunusCompiler.PATH_ENABLED, true);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), mapReduceDriver);

        assertEquals(graph.size(), 6);
        for (FaunusVertex vertex : graph.values()) {
            assertEquals(vertex.pathCount(), 1);
            assertEquals(vertex.getPaths().get(0).size(), 1);
            assertEquals(vertex.getPaths().get(0).get(0).getId(), vertex.getId());
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                assertEquals(((FaunusEdge) edge).pathCount(), 0);
            }
        }

        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesMap.Counters.EDGES_PROCESSED).getValue(), 12);
        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesMap.Counters.VERTICES_PROCESSED).getValue(), 6);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }
}
