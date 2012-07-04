package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.thinkaurelius.faunus.io.graph.util.TaggedHolder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Map;

import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SimpleTraverseTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, TaggedHolder, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, TaggedHolder, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new SimpleTraverse.Map());
        mapReduceDriver.setReducer(new SimpleTraverse.Reduce());
    }

    public void testMapReduce1() throws IOException {
        Configuration config = new Configuration();
        config.setStrings(SimpleTraverse.LABELS, "knows", "created");
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithToyGraph(mapReduceDriver);
        assertEquals(results.size(), 6);

        assertEquals(asList(results.get(1l).getEdges(OUT)).size(), 5);


        /*assertEquals(list.size(), 2);
       for (Pair<NullWritable, FaunusVertex> pair : list) {
           assertEquals(pair.getFirst(), NullWritable.get());
           FaunusVertex temp = pair.getSecond();
           if (temp.getId().equals(1l)) {
               assertFalse(temp.getEdges(Direction.OUT).iterator().hasNext());
               assertEquals(temp.getPropertyKeys().size(), 1);
               assertEquals(temp.getProperty("name"), "marko");
           } else {
               Edge edge = temp.getEdges(Direction.OUT).iterator().next();
               assertEquals(edge.getLabel(), "created_inv");
               assertEquals(edge.getVertex(Direction.IN), vertex1);
               assertEquals(edge.getVertex(Direction.OUT), vertex2);
               assertEquals(temp.getPropertyKeys().size(), 1);
               assertEquals(temp.getProperty("name"), "gremlin");
           }
       }

       assertEquals(mapReduceDriver.getCounters().findCounter(Counters.EDGES_TRANSPOSED).getValue(), 1);*/
    }
}
