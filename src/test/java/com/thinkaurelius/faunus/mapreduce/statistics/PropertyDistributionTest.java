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
public class PropertyDistributionTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, Text, LongWritable, Text, LongWritable> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, Text, LongWritable, Text, LongWritable>();
        mapReduceDriver.setMapper(new KeyDistribution.Map());
        mapReduceDriver.setCombiner(new KeyDistribution.Reduce());
        mapReduceDriver.setReducer(new KeyDistribution.Reduce());
    }

    public void testVertexPropertyDistribution() throws IOException {
        Configuration config = new Configuration();
        config.set(KeyDistribution.CLASS, Vertex.class.getName());
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, LongWritable>> results = runWithToyGraphNoFormatting(ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 2);
        for (final Pair<Text, LongWritable> result : results) {
            if (result.getFirst().toString().equals("type")) {
                assertEquals(result.getSecond().get(), 12l);
            } else if (result.getFirst().toString().equals("name")) {
                assertEquals(result.getSecond().get(), 12l);
            } else {
                assertTrue(false);
            }
        }

        assertEquals(24, this.mapReduceDriver.getCounters().findCounter(KeyDistribution.Counters.PROPERTIES_COUNTED).getValue());
    }

    public void testEdgePropertyDistribution() throws IOException {
        Configuration config = new Configuration();
        config.set(KeyDistribution.CLASS, Edge.class.getName());
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, LongWritable>> results = runWithToyGraphNoFormatting(ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 1);
        for (final Pair<Text, LongWritable> result : results) {
            if (result.getFirst().toString().equals("time")) {
                assertEquals(result.getSecond().get(), 3l);
            } else {
                assertTrue(false);
            }
        }

        assertEquals(3, this.mapReduceDriver.getCounters().findCounter(KeyDistribution.Counters.PROPERTIES_COUNTED).getValue());
    }
}
