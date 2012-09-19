package com.thinkaurelius.faunus.mapreduce.util;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.util.MicroVertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CountMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, LongWritable, NullWritable, LongWritable> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, LongWritable, NullWritable, LongWritable>();
        mapReduceDriver.setMapper(new CountMapReduce.Map());
        mapReduceDriver.setCombiner(new CountMapReduce.Combiner());
        mapReduceDriver.setReducer(new CountMapReduce.Reduce());
    }

    public void testVertexCount() throws IOException {
        Configuration config = new Configuration();
        config.setClass(CountMapReduce.CLASS, Vertex.class, Element.class);
        mapReduceDriver.withConfiguration(config);

        final List<Pair<NullWritable, LongWritable>> results = runWithGraphNoIndex(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), this.mapReduceDriver);
        assertEquals(results.size(), 1);
        for (final Pair<NullWritable, LongWritable> result : results) {
            assertEquals(result.getSecond().get(), 6);
        }
    }

    public void testEdgeCount() throws IOException {
        Configuration config = new Configuration();
        config.setClass(CountMapReduce.CLASS, Edge.class, Element.class);
        mapReduceDriver.withConfiguration(config);

        final List<Pair<NullWritable, LongWritable>> results = runWithGraphNoIndex(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Edge.class), this.mapReduceDriver);
        assertEquals(results.size(), 1);
        for (final Pair<NullWritable, LongWritable> result : results) {
            assertEquals(result.getSecond().get(), 6);
        }
    }

    public void testMultiVertexCount() throws IOException {
        Configuration config = new Configuration();
        config.setClass(CountMapReduce.CLASS, Vertex.class, Element.class);
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).incrPath(10), 10);
        assertEquals(graph.get(2l).incrPath(5), 5);
        assertEquals(graph.get(3l).incrPath(1), 1);
        assertEquals(graph.get(4l).incrPath(7), 7);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);
                            
        List<Pair<NullWritable,LongWritable>> results = runWithGraphNoIndex(graph, mapReduceDriver);
        assertEquals(results.size(), 1);
        assertEquals(results.get(0).getSecond().get(), 23l);

        
    }
}
