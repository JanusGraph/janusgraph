package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.algebra.util.Counters;
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
public class RemovePropertiesTest extends TestCase {

    MapDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapDriver;

    public void setUp() throws Exception {
        mapDriver = new MapDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapDriver.setMapper(new RemoveProperties.Map());
    }

    public void testMap1() throws IOException {
        mapDriver.resetOutput();

        FaunusVertex vertex1 = new FaunusVertex(1);
        vertex1.setProperty("name", "marko");

        mapDriver.withInput(NullWritable.get(), vertex1);
        List<Pair<NullWritable, FaunusVertex>> list = mapDriver.run();
        assertEquals(list.size(), 1);
        FaunusVertex vertex2 = list.get(0).getSecond();
        assertEquals(vertex2.getPropertyKeys().size(), 0);
        assertEquals(mapDriver.getCounters().findCounter(Counters.PROPERTIES_REMOVED).getValue(), 1);
    }

    public void testMap2() throws IOException {
        mapDriver.resetOutput();

        Configuration config = new Configuration();
        config.setStrings(RemoveProperties.KEYS, "name");
        mapDriver.withConfiguration(config);

        FaunusVertex vertex1 = new FaunusVertex(1);
        vertex1.setProperty("name", "marko");
        vertex1.setProperty("age", 32);

        mapDriver.withInput(NullWritable.get(), vertex1);

        List<Pair<NullWritable, FaunusVertex>> list = mapDriver.run();
        assertEquals(list.size(), 1);
        FaunusVertex vertex2 = list.get(0).getSecond();
        assertEquals(vertex2.getPropertyKeys().size(), 1);
        assertEquals(vertex2.getProperty("age"), 32);
        assertEquals(mapDriver.getCounters().findCounter(Counters.PROPERTIES_REMOVED).getValue(), 1);

    }
}
