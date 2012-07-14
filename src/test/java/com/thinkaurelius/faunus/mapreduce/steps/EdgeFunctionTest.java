package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Map;

import static com.tinkerpop.blueprints.Direction.BOTH;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeFunctionTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new EdgeFunction.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testMap1() throws IOException {

        Configuration config = new Configuration();
        config.setStrings(EdgeFunction.FUNCTION, MakeOne.class.getName());
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.TINKERGRAPH, mapReduceDriver);
        assertEquals(results.size(), 6);
        long edges = 0;
        for (FaunusVertex vertex : results.values()) {
            for (Edge edge : vertex.getEdges(BOTH)) {
                edges++;
                assertEquals(edge.getProperty("weight"), 1.0);
            }
        }
        assertEquals(edges, 12);
    }

    public void testMap2() throws IOException {

        Configuration config = new Configuration();
        config.setStrings(EdgeFunction.FUNCTION, Relabel.class.getName());
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.TINKERGRAPH, mapReduceDriver);
        assertEquals(results.size(), 6);
        long edges = 0;
        for (FaunusVertex vertex : results.values()) {
            for (Edge edge : vertex.getEdges(BOTH)) {
                edges++;
                assertTrue(edge.getLabel().equals("knows") || edge.getLabel().equals("made"));
            }
        }
        assertEquals(edges, 12);
    }


    public static class MakeOne implements Function<FaunusEdge, FaunusEdge> {
        public FaunusEdge compute(final FaunusEdge edge) {
            edge.setProperty("weight", 1.0);
            return edge;
        }
    }

    public static class Relabel implements Function<FaunusEdge, FaunusEdge> {
        public FaunusEdge compute(final FaunusEdge edge) {
            if (edge.getLabel().equals("created"))
                edge.setLabel("made");
            return edge;
        }
    }
}
