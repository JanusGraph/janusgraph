package com.thinkaurelius.faunus.mapreduce.algebra;

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
public class VertexFunctionFilterTest extends TestCase {

    MapDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapDriver;

    public void setUp() throws Exception {
        mapDriver = new MapDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapDriver.setMapper(new VertexFunctionFilter.Map());
    }

    public void testMap1() throws IOException {
        mapDriver.resetOutput();

        Configuration config = new Configuration();
        config.setStrings(VertexFunctionFilter.FUNCTION, "com.thinkaurelius.faunus.mapreduce.algebra.VertexFunctionFilterTest$NoMarkoFunction");

        mapDriver.withConfiguration(config);

        FaunusVertex vertex1 = new FaunusVertex(1);
        vertex1.setProperty("name", "marko");
        vertex1.setProperty("age", 32);

        FaunusVertex vertex2 = new FaunusVertex(2);
        vertex2.setProperty("name", "bobby");

        mapDriver.withInput(NullWritable.get(), vertex1);
        assertEquals(mapDriver.run().size(), 0);
        assertEquals(mapDriver.getCounters().findCounter(Counters.VERTICES_ALLOWED_BY_FUNCTION).getValue(), 0);
        assertEquals(mapDriver.getCounters().findCounter(Counters.VERTICES_FILTERED_BY_FUNCTION).getValue(), 1);
        
        mapDriver.withInput(NullWritable.get(), vertex2);

        List<Pair<NullWritable, FaunusVertex>> list = mapDriver.run();
        assertEquals(list.size(), 1);
        assertEquals(list.get(0).getSecond(), vertex2);

        assertEquals(mapDriver.getCounters().findCounter(Counters.VERTICES_ALLOWED_BY_FUNCTION).getValue(), 1);
        assertEquals(mapDriver.getCounters().findCounter(Counters.VERTICES_FILTERED_BY_FUNCTION).getValue(), 1);

    }

    public static class NoMarkoFunction implements Function<FaunusVertex, Boolean> {

        public Boolean compute(final FaunusVertex vertex) {
            return !vertex.getProperty("name").equals("marko");
        }
    }
}
