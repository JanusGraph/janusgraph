package com.thinkaurelius.faunus.mapreduce.statistics;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.statistics.DegreeDistribution;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DegreeDistributionTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, IntWritable, LongWritable, IntWritable, LongWritable> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, IntWritable, LongWritable, IntWritable, LongWritable>();
        mapReduceDriver.setMapper(new DegreeDistribution.Map());
        mapReduceDriver.setCombiner(new DegreeDistribution.Reduce());
        mapReduceDriver.setReducer(new DegreeDistribution.Reduce());
    }

    public void testOutDegree() throws IOException {
        Configuration config = new Configuration();
        config.setStrings(DegreeDistribution.DIRECTION, "OUT");
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<IntWritable, LongWritable>> results = runWithToyGraphNoFormatting(ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 5);
        for (final Pair<IntWritable, LongWritable> result : results) {
            if (result.getFirst().get() == 0) {
                assertEquals(result.getSecond().get(), 7);
            } else if (result.getFirst().get() == 1) {
                assertEquals(result.getSecond().get(), 1);
            } else if (result.getFirst().get() == 3) {
                assertEquals(result.getSecond().get(), 1);
            } else if (result.getFirst().get() == 4) {
                assertEquals(result.getSecond().get(), 2);
            } else if (result.getFirst().get() == 5) {
                assertEquals(result.getSecond().get(), 1);
            } else {
                assertTrue(false);
            }
        }


        assertEquals(17, this.mapReduceDriver.getCounters().findCounter(DegreeDistribution.Counters.EDGES_COUNTED).getValue());
        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(DegreeDistribution.Counters.VERTICES_COUNTED).getValue());
    }

    public void testInDegree() throws IOException {
        Configuration config = new Configuration();
        config.setStrings(DegreeDistribution.DIRECTION, "IN");
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<IntWritable, LongWritable>> results = runWithToyGraphNoFormatting(ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 4);
        for (final Pair<IntWritable, LongWritable> result : results) {
            if (result.getFirst().get() == 0) {
                assertEquals(result.getSecond().get(), 1);
            } else if (result.getFirst().get() == 1) {
                assertEquals(result.getSecond().get(), 6);
            } else if (result.getFirst().get() == 2) {
                assertEquals(result.getSecond().get(), 4);
            } else if (result.getFirst().get() == 3) {
                assertEquals(result.getSecond().get(), 1);
            } else {
                assertTrue(false);
            }
        }

        assertEquals(17, this.mapReduceDriver.getCounters().findCounter(DegreeDistribution.Counters.EDGES_COUNTED).getValue());
        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(DegreeDistribution.Counters.VERTICES_COUNTED).getValue());
    }

    public void testBothDegree() throws IOException {
        Configuration config = new Configuration();
        config.setStrings(DegreeDistribution.DIRECTION, "BOTH");
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<IntWritable, IntWritable>> results = runWithToyGraphNoFormatting(ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        //System.out.println(results);

        assertEquals(34, this.mapReduceDriver.getCounters().findCounter(DegreeDistribution.Counters.EDGES_COUNTED).getValue());
        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(DegreeDistribution.Counters.VERTICES_COUNTED).getValue());

    }
}
