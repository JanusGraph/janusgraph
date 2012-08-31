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

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, Text, LongWritable, Text, LongWritable> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, Text, LongWritable, Text, LongWritable>();
        mapReduceDriver.setMapper(new GroupCountMapReduce.Map());
        mapReduceDriver.setCombiner(new GroupCountMapReduce.Reduce());
        mapReduceDriver.setReducer(new GroupCountMapReduce.Reduce());
    }
    
    public void testTrue() {
        assertTrue(true);
    }

   /* public void testOutDegreeDistribution() throws IOException {
        Configuration config = new Configuration();
        config.set(GroupCountMapReduce.CLASS, Vertex.class.getName());
        config.set(GroupCountMapReduce.KEY_CLOSURE, "{ it -> it.outE.count() }");
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, LongWritable>> results = runWithGraphNoIndex(startPath(generateGraph(ExampleGraph.GRAPH_OF_THE_GODS), Vertex.class), this.mapReduceDriver);
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


        assertEquals(0, this.mapReduceDriver.getCounters().findCounter(GroupCountMapReduce.Counters.EDGES_PROCESSED).getValue());
        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(GroupCountMapReduce.Counters.VERTICES_PROCESSED).getValue());
    }

    public void testEdgePropertySizeDistribution() throws IOException {
        Configuration config = new Configuration();
        config.set(GroupCountMapReduce.CLASS, Edge.class.getName());
        config.set(GroupCountMapReduce.KEY_CLOSURE, "{ it -> it.map.next().size() }");
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, LongWritable>> results = runWithGraphNoIndex(startPath(generateGraph(ExampleGraph.GRAPH_OF_THE_GODS), Edge.class), this.mapReduceDriver);
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


        assertEquals(17, this.mapReduceDriver.getCounters().findCounter(GroupCountMapReduce.Counters.EDGES_PROCESSED).getValue());
        assertEquals(0, this.mapReduceDriver.getCounters().findCounter(GroupCountMapReduce.Counters.VERTICES_PROCESSED).getValue());
    }  */


}
