package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.List;

import static com.tinkerpop.blueprints.Direction.OUT;

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
        config.setStrings(EdgeFunctionFilter.FUNCTION, "com.thinkaurelius.faunus.mapreduce.steps.EdgeFunctionFilterTest$BigWeightFunction");

        mapDriver.withConfiguration(config);

        FaunusVertex vertex1 = new FaunusVertex(1);
        vertex1.setProperty("name", "marko");

        FaunusVertex vertex2 = new FaunusVertex(2);
        vertex2.setProperty("name", "faunus");

        FaunusEdge edge1 = new FaunusEdge(vertex1, vertex2, "created");
        edge1.setProperty("weight", 0.6d);
        FaunusEdge edge2 = new FaunusEdge(vertex1, vertex2, "knows");
        edge2.setProperty("weight", 0.4d);
        vertex1.addEdge(OUT, edge1);
        vertex1.addEdge(OUT, edge2);
        assertEquals(((List) vertex1.getEdges(Direction.OUT)).size(), 2);

        mapDriver.withInput(NullWritable.get(), vertex1);

        List<Pair<NullWritable, FaunusVertex>> list = mapDriver.run();
        assertEquals(list.size(), 1);
        assertEquals(((List) list.get(0).getSecond().getEdges(Direction.OUT)).size(), 1);
        assertEquals(list.get(0).getSecond().getEdges(Direction.OUT).iterator().next().getLabel(), "created");
        assertEquals(list.get(0).getSecond().getEdges(Direction.OUT).iterator().next().getProperty("weight"), 0.6d);

        assertEquals(mapDriver.getCounters().findCounter(EdgeFunctionFilter.Counters.EDGES_ALLOWED).getValue(), 1);
        assertEquals(mapDriver.getCounters().findCounter(EdgeFunctionFilter.Counters.EDGES_FILTERED).getValue(), 1);

    }

    public static class BigWeightFunction implements Function<FaunusEdge, Boolean> {

        public Boolean compute(final FaunusEdge edge) {
            return ((Double) edge.getProperty("weight")) > 0.5;
        }
    }
}
