package com.thinkaurelius.faunus.mapreduce.sideeffect;

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

import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, Text, LongWritable, Text, LongWritable> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, Text, LongWritable, Text, LongWritable>();
        mapReduceDriver.setMapper(new GroupCountMapReduce.Map());
        mapReduceDriver.setCombiner(new GroupCountMapReduce.Combiner());
        mapReduceDriver.setReducer(new GroupCountMapReduce.Reduce());
    }

    public void testOutDegreeDistribution() throws Exception {
        Configuration config = GroupCountMapReduce.createConfiguration(Vertex.class, "{ it -> it.outE.count() }", null);
        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> graph = generateGraph(ExampleGraph.GRAPH_OF_THE_GODS, config);
        final List<Pair<Text, LongWritable>> results = runWithGraphNoIndex(startPath(graph, Vertex.class), this.mapReduceDriver);
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

        identicalStructure(graph, ExampleGraph.GRAPH_OF_THE_GODS);
        assertEquals(0, this.mapReduceDriver.getCounters().findCounter(GroupCountMapReduce.Counters.OUT_EDGES_PROCESSED).getValue());
        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(GroupCountMapReduce.Counters.VERTICES_PROCESSED).getValue());
    }

    public void testEdgePropertySizeDistribution() throws Exception {
        Configuration config = GroupCountMapReduce.createConfiguration(Edge.class, "{ it -> it.map.next().size() }", "{ it -> 2}");
        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> graph = generateGraph(ExampleGraph.GRAPH_OF_THE_GODS, config);
        final List<Pair<Text, LongWritable>> results = runWithGraphNoIndex(startPath(graph, Edge.class), this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 2);
        for (final Pair<Text, LongWritable> result : results) {
            if (result.getFirst().toString().equals("0")) {
                assertEquals(result.getSecond().get(), 28);
            } else if (result.getFirst().toString().equals("1")) {
                assertEquals(result.getSecond().get(), 6);
            } else {
                assertTrue(false);
            }
        }

        identicalStructure(graph, ExampleGraph.GRAPH_OF_THE_GODS);
        assertEquals(17, this.mapReduceDriver.getCounters().findCounter(GroupCountMapReduce.Counters.OUT_EDGES_PROCESSED).getValue());
        assertEquals(0, this.mapReduceDriver.getCounters().findCounter(GroupCountMapReduce.Counters.VERTICES_PROCESSED).getValue());
    }

    public void testVertexDistribution() throws Exception {
        Configuration config = GroupCountMapReduce.createConfiguration(Vertex.class, null, "{ it -> 3.2}");
        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> graph = generateGraph(ExampleGraph.GRAPH_OF_THE_GODS, config);
        final List<Pair<Text, LongWritable>> results = runWithGraphNoIndex(startPath(graph, Vertex.class), this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 12);
        for (final Pair<Text, LongWritable> result : results) {
            assertTrue(result.getFirst().toString().startsWith("v["));
            assertEquals(result.getSecond().get(), 3);
        }

        identicalStructure(graph, ExampleGraph.GRAPH_OF_THE_GODS);
        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(GroupCountMapReduce.Counters.VERTICES_PROCESSED).getValue());
        assertEquals(0, this.mapReduceDriver.getCounters().findCounter(GroupCountMapReduce.Counters.OUT_EDGES_PROCESSED).getValue());

    }

    public void testEdgeDistribution() throws Exception {
        Configuration config = GroupCountMapReduce.createConfiguration(Edge.class, null, null);
        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> graph = generateGraph(ExampleGraph.GRAPH_OF_THE_GODS, config);
        final List<Pair<Text, LongWritable>> results = runWithGraphNoIndex(startPath(graph, Edge.class), this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 17);
        for (final Pair<Text, LongWritable> result : results) {
            assertTrue(result.getFirst().toString().startsWith("e["));
            assertEquals(result.getSecond().get(), 1);
        }

        identicalStructure(graph, ExampleGraph.GRAPH_OF_THE_GODS);
        assertEquals(0, this.mapReduceDriver.getCounters().findCounter(GroupCountMapReduce.Counters.VERTICES_PROCESSED).getValue());
        assertEquals(17, this.mapReduceDriver.getCounters().findCounter(GroupCountMapReduce.Counters.OUT_EDGES_PROCESSED).getValue());

    }
}
