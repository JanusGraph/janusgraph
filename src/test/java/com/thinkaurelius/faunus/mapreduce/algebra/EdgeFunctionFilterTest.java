package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.algebra.util.Counters;
import com.thinkaurelius.faunus.mapreduce.algebra.util.Function;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeFunctionFilterTest extends TestCase {

    MapDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapDriver;

    public void setUp() throws Exception {
        mapDriver = new MapDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapDriver.setMapper(new EdgeFunctionFilter.Map());
    }

    public void testMap1() throws IOException {
        mapDriver.resetOutput();

        Configuration config = new Configuration();
        config.setStrings(EdgeFunctionFilter.FUNCTION, "com.thinkaurelius.faunus.mapreduce.algebra.EdgeFunctionFilterTest$BigWeightFunction");

        mapDriver.withConfiguration(config);

        FaunusVertex vertex1 = new FaunusVertex(1);
        vertex1.setProperty("name", "marko");

        FaunusVertex vertex2 = new FaunusVertex(2);
        vertex2.setProperty("name", "faunus");

        FaunusEdge edge1 = new FaunusEdge(vertex1, vertex2, "created");
        edge1.setProperty("weight", 0.6d);
        FaunusEdge edge2 = new FaunusEdge(vertex1, vertex2, "knows");
        edge2.setProperty("weight", 0.4d);
        vertex1.addOutEdge(edge1);
        vertex1.addOutEdge(edge2);
        assertEquals(((List) vertex1.getOutEdges()).size(), 2);

        mapDriver.withInput(NullWritable.get(), vertex1);

        List<Pair<NullWritable, FaunusVertex>> list = mapDriver.run();
        assertEquals(list.size(), 1);
        assertEquals(((List) list.get(0).getSecond().getOutEdges()).size(), 1);
        assertEquals(list.get(0).getSecond().getOutEdges().iterator().next().getLabel(), "created");
        assertEquals(list.get(0).getSecond().getOutEdges().iterator().next().getProperty("weight"), 0.6d);

        assertEquals(mapDriver.getCounters().findCounter(Counters.EDGES_ALLOWED_BY_FUNCTION).getValue(), 1);
        assertEquals(mapDriver.getCounters().findCounter(Counters.EDGES_FILTERED_BY_FUNCTION).getValue(), 1);

    }

    public static class BigWeightFunction implements Function<FaunusEdge, Boolean> {

        public Boolean compute(final FaunusEdge edge) {
            return ((Double) edge.getProperty("weight")) > 0.5;
        }
    }
}
