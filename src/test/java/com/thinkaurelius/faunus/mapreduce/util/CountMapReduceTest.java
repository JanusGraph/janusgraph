package com.thinkaurelius.faunus.mapreduce.util;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
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
public class CountMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, IntWritable, LongWritable, NullWritable, Text> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, IntWritable, LongWritable, NullWritable, Text>();
        mapReduceDriver.setMapper(new CountMapReduce.Map());
        mapReduceDriver.setReducer(new CountMapReduce.Reduce());
    }

    public void testVertexCount() throws IOException {
        Configuration config = new Configuration();
        config.setClass(CountMapReduce.CLASS, Vertex.class, Element.class);
        mapReduceDriver.withConfiguration(config);

        final List<Pair<NullWritable, Text>> results = runWithGraphNoIndex(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), this.mapReduceDriver);
        assertEquals(results.size(), 1);
        for (final Pair<NullWritable, Text> result : results) {
            assertEquals(result.getSecond().toString(), "6");
        }
    }

    public void testEdgeCount() throws IOException {
        Configuration config = new Configuration();
        config.setClass(CountMapReduce.CLASS, Edge.class, Element.class);
        mapReduceDriver.withConfiguration(config);

        final List<Pair<NullWritable, Text>> results = runWithGraphNoIndex(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Edge.class), this.mapReduceDriver);
        assertEquals(results.size(), 1);
        for (final Pair<NullWritable, Text> result : results) {
            assertEquals(result.getSecond().toString(), "6");
        }
    }
}
