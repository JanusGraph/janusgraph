package com.thinkaurelius.faunus.mapreduce.operators;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
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
public class EdgeLabelDistributionTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, Text, LongWritable, Text, LongWritable> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, Text, LongWritable, Text, LongWritable>();
        mapReduceDriver.setMapper(new EdgeLabelDistribution.Map());
        mapReduceDriver.setCombiner(new EdgeLabelDistribution.Reduce());
        mapReduceDriver.setReducer(new EdgeLabelDistribution.Reduce());
    }

    public void testMapReduce1() throws IOException {
        Configuration config = new Configuration();
        config.setStrings(EdgeLabelDistribution.DIRECTION, "OUT");
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, LongWritable>> results = runWithToyGraphNoFormatting(ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        assertEquals(results.size(), 6);
        for (final Pair<Text, LongWritable> result : results) {
            if (result.getFirst().toString().equals("lives")) {
                assertEquals(result.getSecond().get(), 4l);
            } else if (result.getFirst().toString().equals("battled")) {
                assertEquals(result.getSecond().get(), 3l);
            } else if (result.getFirst().toString().equals("brother")) {
                assertEquals(result.getSecond().get(), 6l);
            } else if (result.getFirst().toString().equals("pet")) {
                assertEquals(result.getSecond().get(), 1l);
            } else if (result.getFirst().toString().equals("mother")) {
                assertEquals(result.getSecond().get(), 1l);
            } else if (result.getFirst().toString().equals("father")) {
                assertEquals(result.getSecond().get(), 2l);
            } else {
                assertTrue(false);
            }

            assertEquals(17, this.mapReduceDriver.getCounters().findCounter(EdgeLabelDistribution.Counters.EDGES_COUNTED).getValue());
            assertEquals(12, this.mapReduceDriver.getCounters().findCounter(EdgeLabelDistribution.Counters.VERTICES_COUNTED).getValue());
        }


    }
}
