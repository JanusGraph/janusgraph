package com.thinkaurelius.faunus.mapreduce.statistics;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.statistics.PropertyValueDistribution;
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
public class PropertyValueDistributionTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, Text, LongWritable, Text, LongWritable> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, Text, LongWritable, Text, LongWritable>();
        mapReduceDriver.setMapper(new PropertyValueDistribution.Map());
        mapReduceDriver.setCombiner(new PropertyValueDistribution.Reduce());
        mapReduceDriver.setReducer(new PropertyValueDistribution.Reduce());
    }

    public void testVertexTypeProperty() throws IOException {
        Configuration config = new Configuration();
        config.set(PropertyValueDistribution.CLASS, Vertex.class.getName());
        config.set(PropertyValueDistribution.PROPERTY, "type");
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, LongWritable>> results = runWithToyGraphNoFormatting(ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 6);
        for (final Pair<Text, LongWritable> result : results) {
            if (result.getFirst().toString().equals("demigod")) {
                assertEquals(result.getSecond().get(), 1l);
            } else if (result.getFirst().toString().equals("god")) {
                assertEquals(result.getSecond().get(), 3l);
            } else if (result.getFirst().toString().equals("human")) {
                assertEquals(result.getSecond().get(), 1l);
            } else if (result.getFirst().toString().equals("location")) {
                assertEquals(result.getSecond().get(), 3l);
            } else if (result.getFirst().toString().equals("monster")) {
                assertEquals(result.getSecond().get(), 3l);
            } else if (result.getFirst().toString().equals("titan")) {
                assertEquals(result.getSecond().get(), 1l);
            } else {
                assertTrue(false);
            }
        }

        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(PropertyValueDistribution.Counters.PROPERTIES_COUNTED).getValue());
    }

    public void testVertexNoProperty() throws IOException {
        Configuration config = new Configuration();
        config.set(PropertyValueDistribution.CLASS, Vertex.class.getName());
        config.set(PropertyValueDistribution.PROPERTY, "nothing property");
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, LongWritable>> results = runWithToyGraphNoFormatting(ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 1);
        for (final Pair<Text, LongWritable> result : results) {
            if (result.getFirst().toString().equals("null")) {
                assertEquals(result.getSecond().get(), 12l);
            } else {
                assertTrue(false);
            }
        }

        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(PropertyValueDistribution.Counters.PROPERTIES_COUNTED).getValue());
    }

    public void testEdgeTimeProperty() throws IOException {
        Configuration config = new Configuration();
        config.set(PropertyValueDistribution.CLASS, Edge.class.getName());
        config.set(PropertyValueDistribution.PROPERTY, "time");
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, LongWritable>> results = runWithToyGraphNoFormatting(ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 4);
        for (final Pair<Text, LongWritable> result : results) {
            if (result.getFirst().toString().equals("1")) {
                assertEquals(result.getSecond().get(), 1l);
            } else if (result.getFirst().toString().equals("2")) {
                assertEquals(result.getSecond().get(), 1l);
            } else if (result.getFirst().toString().equals("12")) {
                assertEquals(result.getSecond().get(), 1l);
            } else if (result.getFirst().toString().equals("null")) {
                assertEquals(result.getSecond().get(), 14l);
            } else {
                assertTrue(false);
            }
        }

        assertEquals(17, this.mapReduceDriver.getCounters().findCounter(PropertyValueDistribution.Counters.PROPERTIES_COUNTED).getValue());
    }

}
