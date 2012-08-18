package com.thinkaurelius.faunus.mapreduce.statistics;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AdjacentPropertiesTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>, Text, Text> mapReduceDriver;
    MapReduceDriver<Text, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver2;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>, Text, Text>();
        mapReduceDriver.setMapper(new AdjacentProperties.Map());
        mapReduceDriver.setReducer(new AdjacentProperties.Reduce());

        mapReduceDriver2 = new MapReduceDriver<Text, Text, Text, LongWritable, Text, LongWritable>();
        mapReduceDriver2.setMapper(new AdjacentProperties.Map2());
        mapReduceDriver2.setCombiner(new AdjacentProperties.Reduce2());
        mapReduceDriver2.setReducer(new AdjacentProperties.Reduce2());


    }

    public void testTypeProperty() throws IOException {
        Configuration config = new Configuration();
        config.set(AdjacentProperties.PROPERTY, "type");
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, Text>> results = runWithToyGraphNoFormatting(BaseTest.ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        // System.out.println(results);
        assertEquals(results.size(), 17);
        for (Pair<Text, Text> result : results) {
            if (result.getFirst().toString().equals("god"))
                assertTrue(result.getSecond().toString().equals("god") || result.getSecond().toString().equals("titan") || result.getSecond().toString().equals("location") || result.getSecond().toString().equals("monster"));
            else if (result.getFirst().toString().equals("demigod"))
                assertTrue(result.getSecond().toString().equals("god") || result.getSecond().toString().equals("human") || result.getSecond().toString().equals("monster"));
            else if (result.getFirst().toString().equals("monster"))
                assertTrue(result.getSecond().toString().equals("location"));
            else
                assertTrue(false);

        }
        assertEquals(17, this.mapReduceDriver.getCounters().findCounter(AdjacentProperties.Counters.EDGES_COUNTED).getValue());
        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(AdjacentProperties.Counters.VERTICES_COUNTED).getValue());
    }

    public void testTypeProperty2() throws IOException {
        Configuration config = new Configuration();
        config.set(AdjacentProperties.PROPERTY, "type");
        this.mapReduceDriver.withConfiguration(config);
        for (Pair<Text, Text> result : (List<Pair>) runWithToyGraphNoFormatting(BaseTest.ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver)) {
            this.mapReduceDriver2.withInput(result);
        }
        List<Pair<Text, LongWritable>> results = this.mapReduceDriver2.run();
        // System.out.println(results);
        for (Pair<Text, LongWritable> result : results) {
            if (result.getFirst().toString().equals("demigod\tgod"))
                assertEquals(result.getSecond().get(), 1);
            else if (result.getFirst().toString().equals("demigod\thuman"))
                assertEquals(result.getSecond().get(), 1);
            else if (result.getFirst().toString().equals("demigod\tmonster"))
                assertEquals(result.getSecond().get(), 3);
            else if (result.getFirst().toString().equals("god\tgod"))
                assertEquals(result.getSecond().get(), 6);
            else if (result.getFirst().toString().equals("god\tlocation"))
                assertEquals(result.getSecond().get(), 3);
            else if (result.getFirst().toString().equals("god\tmonster"))
                assertEquals(result.getSecond().get(), 1);
            else if (result.getFirst().toString().equals("god\ttitan"))
                assertEquals(result.getSecond().get(), 1);
            else if (result.getFirst().toString().equals("monster\tlocation"))
                assertEquals(result.getSecond().get(), 1);
            else
                assertTrue(false);

        }

        assertEquals(17, this.mapReduceDriver.getCounters().findCounter(AdjacentProperties.Counters.EDGES_COUNTED).getValue());
        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(AdjacentProperties.Counters.VERTICES_COUNTED).getValue());
    }

    public void testTypeProperty3BattledEdge() throws IOException {
        Configuration config = new Configuration();
        config.set(AdjacentProperties.PROPERTY, "type");
        config.setStrings(AdjacentProperties.LABELS, "battled");
        this.mapReduceDriver.withConfiguration(config);
        for (Pair<Text, Text> result : (List<Pair>) runWithToyGraphNoFormatting(BaseTest.ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver)) {
            this.mapReduceDriver2.withInput(result);
        }
        List<Pair<Text, LongWritable>> results = this.mapReduceDriver2.run();
        // System.out.println(results);
        for (Pair<Text, LongWritable> result : results) {
            if (result.getFirst().toString().equals("demigod\tmonster"))
                assertEquals(result.getSecond().get(), 3);
            else
                assertTrue(false);

        }

        assertEquals(3, this.mapReduceDriver.getCounters().findCounter(AdjacentProperties.Counters.EDGES_COUNTED).getValue());
        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(AdjacentProperties.Counters.VERTICES_COUNTED).getValue());
    }
}
