package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyMapTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, WritableComparable, WritableComparable, WritableComparable, WritableComparable> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, WritableComparable, WritableComparable, WritableComparable, WritableComparable>();
        mapReduceDriver.setMapper(new PropertyMap.Map());
        mapReduceDriver.setReducer(new Reducer<WritableComparable, WritableComparable, WritableComparable, WritableComparable>());
    }

    public void testVertexPropertiesName() throws IOException {
        Configuration config = new Configuration();
        config.set(PropertyMap.CLASS, Vertex.class.getName());
        config.set(PropertyMap.KEY, "name");
        config.setClass(PropertyMap.TYPE, Text.class, WritableComparable.class);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Vertex.class, 1, 1, 2, 3, 4);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 2);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 1);
        assertEquals(graph.get(4l).pathCount(), 1);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        final List<Pair<LongWritable, Text>> results = runWithGraphNoIndex(graph, mapReduceDriver);
        assertEquals(results.size(), 5);
        assertEquals(results.get(0).getFirst().get(), 1l);
        assertEquals(results.get(0).getSecond().toString(), "marko");

        assertEquals(results.get(1).getFirst().get(), 1l);
        assertEquals(results.get(1).getSecond().toString(), "marko");

        assertEquals(results.get(2).getFirst().get(), 2l);
        assertEquals(results.get(2).getSecond().toString(), "vadas");

        assertEquals(results.get(3).getFirst().get(), 3l);
        assertEquals(results.get(3).getSecond().toString(), "lop");

        assertEquals(results.get(4).getFirst().get(), 4l);
        assertEquals(results.get(4).getSecond().toString(), "josh");


        assertEquals(mapReduceDriver.getCounters().findCounter(PropertyMap.Counters.VERTICES_PROCESSED).getValue(), 4);
        assertEquals(mapReduceDriver.getCounters().findCounter(PropertyMap.Counters.OUT_EDGES_PROCESSED).getValue(), 0);

        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
    }

    public void testVertexPropertiesAge() throws IOException {
        Configuration config = new Configuration();
        config.set(PropertyMap.CLASS, Vertex.class.getName());
        config.set(PropertyMap.KEY, "age");
        config.setClass(PropertyMap.TYPE, IntWritable.class, WritableComparable.class);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Vertex.class, 1, 1, 2, 3, 4);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 2);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 1);
        assertEquals(graph.get(4l).pathCount(), 1);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        final List<Pair<LongWritable, IntWritable>> results = runWithGraphNoIndex(graph, mapReduceDriver);
        assertEquals(results.size(), 5);
        assertEquals(results.get(0).getFirst().get(), 1l);
        assertEquals(results.get(0).getSecond().get(), 29);

        assertEquals(results.get(1).getFirst().get(), 1l);
        assertEquals(results.get(1).getSecond().get(), 29);

        assertEquals(results.get(2).getFirst().get(), 2l);
        assertEquals(results.get(2).getSecond().get(), 27);

        assertEquals(results.get(3).getFirst().get(), 3l);
        assertEquals(results.get(3).getSecond().get(), Integer.MIN_VALUE);

        assertEquals(results.get(4).getFirst().get(), 4l);
        assertEquals(results.get(4).getSecond().get(), 32);


        assertEquals(mapReduceDriver.getCounters().findCounter(PropertyMap.Counters.VERTICES_PROCESSED).getValue(), 4);
        assertEquals(mapReduceDriver.getCounters().findCounter(PropertyMap.Counters.OUT_EDGES_PROCESSED).getValue(), 0);

        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
    }
}
