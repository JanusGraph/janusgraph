package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ValueGroupCountMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, WritableComparable, LongWritable, WritableComparable, LongWritable> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, WritableComparable, LongWritable, WritableComparable, LongWritable>();
        mapReduceDriver.setMapper(new ValueGroupCountMapReduce.Map());
        mapReduceDriver.setCombiner(new ValueGroupCountMapReduce.Combiner());
        mapReduceDriver.setReducer(new ValueGroupCountMapReduce.Reduce());
    }

    public void testTrue() {
    }

    public void testVertexTypeProperty() throws Exception {
        Configuration config = ValueGroupCountMapReduce.createConfiguration(Vertex.class, "type", Text.class);
        this.mapReduceDriver.withConfiguration(config);

        final List<Pair<Text, LongWritable>> results = runWithGraphNoIndex(startPath(generateGraph(ExampleGraph.GRAPH_OF_THE_GODS, config), Vertex.class), this.mapReduceDriver);
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

        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(ValueGroupCountMapReduce.Counters.PROPERTIES_COUNTED).getValue());
    }

    public void testVertexNoProperty() throws Exception {
        Configuration config = ValueGroupCountMapReduce.createConfiguration(Vertex.class, "nothing property", Text.class);

        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<WritableComparable, LongWritable>> results = runWithGraphNoIndex(startPath(generateGraph(ExampleGraph.GRAPH_OF_THE_GODS, config), Vertex.class), this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 1);
        for (final Pair<WritableComparable, LongWritable> result : results) {
            if (result.getFirst().toString().equals("null")) {
                assertEquals(result.getSecond().get(), 12l);
            } else {
                assertTrue(false);
            }
        }

        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(ValueGroupCountMapReduce.Counters.PROPERTIES_COUNTED).getValue());
    }

    public void testEdgeTimeProperty() throws Exception {
        Configuration config = ValueGroupCountMapReduce.createConfiguration(Edge.class, "time", Text.class);
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<WritableComparable, LongWritable>> results = runWithGraphNoIndex(startPath(generateGraph(ExampleGraph.GRAPH_OF_THE_GODS, config), Edge.class), this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 4);
        for (final Pair<WritableComparable, LongWritable> result : results) {
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

        assertEquals(17, this.mapReduceDriver.getCounters().findCounter(ValueGroupCountMapReduce.Counters.PROPERTIES_COUNTED).getValue());
    }

    public void testEdgeLabelDistribution1() throws Exception {
        Configuration config = ValueGroupCountMapReduce.createConfiguration(Edge.class, "label", Text.class);
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, LongWritable>> results = runWithGraphNoIndex(startPath(generateGraph(ExampleGraph.GRAPH_OF_THE_GODS, config), Edge.class), this.mapReduceDriver);
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
        }

        assertEquals(17, this.mapReduceDriver.getCounters().findCounter(ValueGroupCountMapReduce.Counters.PROPERTIES_COUNTED).getValue());
    }

    public void testEdgeLabelDistribution2() throws Exception {
        Configuration config = ValueGroupCountMapReduce.createConfiguration(Edge.class, "label", Text.class);
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, LongWritable>> results = runWithGraphNoIndex(startPath(generateGraph(ExampleGraph.GRAPH_OF_THE_GODS, config), Edge.class), this.mapReduceDriver);
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
        }

        assertEquals(17, this.mapReduceDriver.getCounters().findCounter(ValueGroupCountMapReduce.Counters.PROPERTIES_COUNTED).getValue());
    }

    public void testPropertySortingOnInteger() throws Exception {
        Configuration config = ValueGroupCountMapReduce.createConfiguration(Vertex.class, "age", IntWritable.class);
        this.mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> vertices = new HashMap<Long, FaunusVertex>();
        for (long i = 0; i < 15; i++) {
            FaunusVertex v = new FaunusVertex(i);
            v.setProperty("age", i);
            vertices.put(i, v);
            v.startPath();
        }
        final List<Pair<IntWritable, LongWritable>> results = runWithGraphNoIndex(vertices, mapReduceDriver);
        for (int i = 0; i < results.size(); i++) {
            assertEquals(results.get(i).getSecond().get(), 1l);
            assertEquals(results.get(i).getFirst().get(), i);
        }
    }

    public void testPropertySortingOnText() throws Exception {
        Configuration config = ValueGroupCountMapReduce.createConfiguration(Vertex.class, "age", Text.class);
        this.mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> vertices = new HashMap<Long, FaunusVertex>();
        for (long i = 0; i < 15; i++) {
            FaunusVertex v = new FaunusVertex(i);
            v.setProperty("age", i);
            vertices.put(i, v);
            v.startPath();
        }
        final List<Pair<Text, LongWritable>> results = runWithGraphNoIndex(vertices, mapReduceDriver);
        final List<String> sortedText = new ArrayList<String>();
        sortedText.addAll(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14"));
        Collections.sort(sortedText);
        //System.out.print(sortedText);
        for (int i = 0; i < results.size(); i++) {
            assertEquals(results.get(i).getSecond().get(), 1l);
            assertEquals(results.get(i).getFirst().toString(), sortedText.get(i));
        }
    }


}
