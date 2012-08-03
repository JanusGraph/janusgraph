package com.thinkaurelius.faunus.mapreduce.operators;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SortedVertexDegreeTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, IntWritable, FaunusVertex, Text, IntWritable> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, IntWritable, FaunusVertex, Text, IntWritable>();
        mapReduceDriver.setMapper(new SortedVertexDegree.Map());
        mapReduceDriver.setReducer(new SortedVertexDegree.Reduce());
    }

    public void testOutDegreeReverseSort() throws IOException {
        Configuration config = new Configuration();
        config.setStrings(SortedVertexDegree.DIRECTION, "OUT");
        config.setStrings(SortedVertexDegree.PROPERTY, "name");
        config.setStrings(SortedVertexDegree.ORDER, Tokens.Order.REVERSE.name());
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, IntWritable>> results = runWithToyGraphNoFormatting(BaseTest.ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        System.out.println(results);
        assertEquals(results.size(), 12);

        assertEquals(results.get(0).getFirst().toString(), "hercules");
        assertEquals(results.get(0).getSecond().get(), 5);

        assertEquals(results.get(1).getFirst().toString(), "jupiter");
        assertEquals(results.get(1).getSecond().get(), 4);

        assertEquals(results.get(2).getFirst().toString(), "pluto");
        assertEquals(results.get(2).getSecond().get(), 4);

        assertEquals(results.get(3).getFirst().toString(), "neptune");
        assertEquals(results.get(3).getSecond().get(), 3);

        assertEquals(results.get(4).getFirst().toString(), "cerberus");
        assertEquals(results.get(4).getSecond().get(), 1);

        assertEquals(results.get(5).getFirst().toString(), "saturn");
        assertEquals(results.get(5).getSecond().get(), 0);

        assertEquals(results.get(6).getFirst().toString(), "sky");
        assertEquals(results.get(6).getSecond().get(), 0);

        assertEquals(results.get(7).getFirst().toString(), "sea");
        assertEquals(results.get(7).getSecond().get(), 0);

        assertEquals(results.get(8).getFirst().toString(), "tartarus");
        assertEquals(results.get(8).getSecond().get(), 0);

        assertEquals(results.get(9).getFirst().toString(), "alcmene");
        assertEquals(results.get(9).getSecond().get(), 0);

        assertEquals(results.get(10).getFirst().toString(), "nemean");
        assertEquals(results.get(10).getSecond().get(), 0);

        assertEquals(results.get(11).getFirst().toString(), "hydra");
        assertEquals(results.get(11).getSecond().get(), 0);


        assertEquals(17, this.mapReduceDriver.getCounters().findCounter(SortedVertexDegree.Counters.EDGES_COUNTED).getValue());
        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(SortedVertexDegree.Counters.VERTICES_COUNTED).getValue());
    }

    public void testOutDegreeStandardSort() throws IOException {
        Configuration config = new Configuration();
        config.setStrings(SortedVertexDegree.DIRECTION, "OUT");
        config.setStrings(SortedVertexDegree.PROPERTY, "name");
        config.setStrings(SortedVertexDegree.ORDER, Tokens.Order.STANDARD.name());
        this.mapReduceDriver.withConfiguration(config);
        final List<Pair<Text, IntWritable>> results = runWithToyGraphNoFormatting(BaseTest.ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        //System.out.println(results);
        assertEquals(results.size(), 12);

        assertEquals(results.get(11).getFirst().toString(), "hercules");
        assertEquals(results.get(11).getSecond().get(), 5);

        // assertEquals(results.get(10).getFirst().toString(), "jupiter");
        assertEquals(results.get(10).getSecond().get(), 4);

        // assertEquals(results.get(9).getFirst().toString(), "pluto");
        assertEquals(results.get(9).getSecond().get(), 4);

        assertEquals(results.get(8).getFirst().toString(), "neptune");
        assertEquals(results.get(8).getSecond().get(), 3);

        assertEquals(results.get(7).getFirst().toString(), "cerberus");
        assertEquals(results.get(7).getSecond().get(), 1);

        // assertEquals(results.get(6).getFirst().toString(), "saturn");
        assertEquals(results.get(6).getSecond().get(), 0);

        // assertEquals(results.get(5).getFirst().toString(), "sky");
        assertEquals(results.get(5).getSecond().get(), 0);

        // assertEquals(results.get(4).getFirst().toString(), "sea");
        assertEquals(results.get(4).getSecond().get(), 0);

        // assertEquals(results.get(3).getFirst().toString(), "tartarus");
        assertEquals(results.get(3).getSecond().get(), 0);

        // assertEquals(results.get(2).getFirst().toString(), "alcmene");
        assertEquals(results.get(2).getSecond().get(), 0);

        // assertEquals(results.get(1).getFirst().toString(), "nemean");
        assertEquals(results.get(1).getSecond().get(), 0);

        // assertEquals(results.get(0).getFirst().toString(), "hydra");
        assertEquals(results.get(0).getSecond().get(), 0);


        assertEquals(17, this.mapReduceDriver.getCounters().findCounter(SortedVertexDegree.Counters.EDGES_COUNTED).getValue());
        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(SortedVertexDegree.Counters.VERTICES_COUNTED).getValue());


    }
}
