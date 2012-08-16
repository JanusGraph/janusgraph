package com.thinkaurelius.faunus.mapreduce.statistics;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.statistics.Degree;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DegreeTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, Text, IntWritable, Text, IntWritable>();
        mapReduceDriver.setMapper(new Degree.Map());
        mapReduceDriver.setReducer(new Reducer<Text, IntWritable, Text, IntWritable>());
    }

    public void testOutDegree() throws IOException {
        Configuration config = new Configuration();
        config.set(Degree.DIRECTION, "OUT");
        config.set(Degree.PROPERTY, "name");
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, IntWritable>> results = runWithToyGraphNoFormatting(BaseTest.ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 12);
        for (Pair<Text, IntWritable> result : results) {
            String name = result.getFirst().toString();
            int degree = result.getSecond().get();
            if (name.equals("hercules")) {
                assertEquals(degree, 5);
            } else if (name.equals("jupiter")) {
                assertEquals(degree, 4);
            } else if (name.equals("pluto")) {
                assertEquals(degree, 4);
            } else if (name.equals("neptune")) {
                assertEquals(degree, 3);
            } else if (name.equals("cerberus")) {
                assertEquals(degree, 1);
            } else if (name.equals("saturn")) {
                assertEquals(degree, 0);
            } else if (name.equals("sky")) {
                assertEquals(degree, 0);
            } else if (name.equals("sea")) {
                assertEquals(degree, 0);
            } else if (name.equals("tartarus")) {
                assertEquals(degree, 0);
            } else if (name.equals("alcmene")) {
                assertEquals(degree, 0);
            } else if (name.equals("nemean")) {
                assertEquals(degree, 0);
            } else if (name.equals("hydra")) {
                assertEquals(degree, 0);
            } else {
                assertFalse(true);
            }
        }

        assertEquals(17, this.mapReduceDriver.getCounters().findCounter(Degree.Counters.EDGES_COUNTED).getValue());
        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(Degree.Counters.VERTICES_COUNTED).getValue());
    }

    public void testInDegree() throws IOException {
        Configuration config = new Configuration();
        config.set(Degree.DIRECTION, "IN");
        config.set(Degree.PROPERTY, "name");
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, IntWritable>> results = runWithToyGraphNoFormatting(BaseTest.ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 12);
        for (Pair<Text, IntWritable> result : results) {
            String name = result.getFirst().toString();
            int degree = result.getSecond().get();
            if (name.equals("hercules")) {
                assertEquals(degree, 0);
            } else if (name.equals("jupiter")) {
                assertEquals(degree, 3);
            } else if (name.equals("pluto")) {
                assertEquals(degree, 2);
            } else if (name.equals("neptune")) {
                assertEquals(degree, 2);
            } else if (name.equals("cerberus")) {
                assertEquals(degree, 2);
            } else if (name.equals("saturn")) {
                assertEquals(degree, 1);
            } else if (name.equals("sky")) {
                assertEquals(degree, 1);
            } else if (name.equals("sea")) {
                assertEquals(degree, 1);
            } else if (name.equals("tartarus")) {
                assertEquals(degree, 2);
            } else if (name.equals("alcmene")) {
                assertEquals(degree, 1);
            } else if (name.equals("nemean")) {
                assertEquals(degree, 1);
            } else if (name.equals("hydra")) {
                assertEquals(degree, 1);
            } else {
                assertFalse(true);
            }
        }

        assertEquals(17, this.mapReduceDriver.getCounters().findCounter(Degree.Counters.EDGES_COUNTED).getValue());
        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(Degree.Counters.VERTICES_COUNTED).getValue());
    }

    public void testBothDegree() throws IOException {
        Configuration config = new Configuration();
        config.set(Degree.DIRECTION, "BOTH");
        config.set(Degree.PROPERTY, "name");
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, IntWritable>> results = runWithToyGraphNoFormatting(BaseTest.ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 12);
        for (Pair<Text, IntWritable> result : results) {
            String name = result.getFirst().toString();
            int degree = result.getSecond().get();
            if (name.equals("hercules")) {
                assertEquals(degree, 5);
            } else if (name.equals("jupiter")) {
                assertEquals(degree, 7);
            } else if (name.equals("pluto")) {
                assertEquals(degree, 6);
            } else if (name.equals("neptune")) {
                assertEquals(degree, 5);
            } else if (name.equals("cerberus")) {
                assertEquals(degree, 3);
            } else if (name.equals("saturn")) {
                assertEquals(degree, 1);
            } else if (name.equals("sky")) {
                assertEquals(degree, 1);
            } else if (name.equals("sea")) {
                assertEquals(degree, 1);
            } else if (name.equals("tartarus")) {
                assertEquals(degree, 2);
            } else if (name.equals("alcmene")) {
                assertEquals(degree, 1);
            } else if (name.equals("nemean")) {
                assertEquals(degree, 1);
            } else if (name.equals("hydra")) {
                assertEquals(degree, 1);
            } else {
                assertFalse(true);
            }
        }

        assertEquals(34, this.mapReduceDriver.getCounters().findCounter(Degree.Counters.EDGES_COUNTED).getValue());
        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(Degree.Counters.VERTICES_COUNTED).getValue());
    }
}
