package com.thinkaurelius.faunus.mapreduce.statistics;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
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
public class DistributionTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, Text, LongWritable, Text, LongWritable> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, Text, LongWritable, Text, LongWritable>();
        mapReduceDriver.setMapper(new Distribution.Map());
        mapReduceDriver.setCombiner(new Distribution.Reduce());
        mapReduceDriver.setReducer(new Distribution.Reduce());
    }

    public void testOutDegreeDistribution() throws IOException {
        Configuration config = new Configuration();
        config.set(Distribution.CLASS, Vertex.class.getName());
        config.set(Distribution.FUNCTION, "{ it -> [it.outE.count(), 1] }");
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, LongWritable>> results = runWithToyGraphNoFormatting(ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 5);
        for (final Pair<Text, LongWritable> result : results) {
            if (result.getFirst().toString().equals("0")) {
                assertEquals(result.getSecond().get(), 7);
            } else if (result.getFirst().toString().equals("1")) {
                assertEquals(result.getSecond().get(), 1);
            } else if (result.getFirst().toString().equals("3")) {
                assertEquals(result.getSecond().get(), 1);
            } else if (result.getFirst().toString().equals("4")) {
                assertEquals(result.getSecond().get(), 2);
            } else if (result.getFirst().toString().equals("5")) {
                assertEquals(result.getSecond().get(), 1);
            } else {
                assertTrue(false);
            }
        }


        assertEquals(0, this.mapReduceDriver.getCounters().findCounter(Distribution.Counters.EDGES_PROCESSED).getValue());
        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(Distribution.Counters.VERTICES_PROCESSED).getValue());
    }

    public void testEdgePropertySizeDistribution() throws IOException {
        Configuration config = new Configuration();
        config.set(Distribution.CLASS, Edge.class.getName());
        config.set(Distribution.FUNCTION, "{ it -> [it.map.next().size(), 1] }");
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, LongWritable>> results = runWithToyGraphNoFormatting(ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 2);
        for (final Pair<Text, LongWritable> result : results) {
            if (result.getFirst().toString().equals("0")) {
                assertEquals(result.getSecond().get(), 14);
            } else if (result.getFirst().toString().equals("1")) {
                assertEquals(result.getSecond().get(), 3);
            } else {
                assertTrue(false);
            }
        }


        assertEquals(17, this.mapReduceDriver.getCounters().findCounter(Distribution.Counters.EDGES_PROCESSED).getValue());
        assertEquals(0, this.mapReduceDriver.getCounters().findCounter(Distribution.Counters.VERTICES_PROCESSED).getValue());
    }


}
