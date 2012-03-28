package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.thinkaurelius.faunus.io.graph.util.Holder;
import com.tinkerpop.blueprints.pgm.Edge;
import junit.framework.TestCase;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TransposeTest extends TestCase {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new Transpose.Map());
        mapReduceDriver.setReducer(new Transpose.Reduce());
    }

    public void testMapReduce1() throws IOException {
        FaunusVertex vertex1 = new FaunusVertex(1);
        vertex1.setProperty("name", "marko");
        FaunusVertex vertex2 = new FaunusVertex(2);
        vertex2.setProperty("name", "gremlin");
        vertex1.addOutEdge(new FaunusEdge(vertex1, vertex2, "created"));

        mapReduceDriver.withInput(NullWritable.get(), vertex1).withInput(NullWritable.get(), vertex2);
        List<Pair<NullWritable, FaunusVertex>> list = mapReduceDriver.run();

        for (Pair<NullWritable, FaunusVertex> pair : list) {
            assertEquals(pair.getFirst(), NullWritable.get());
            FaunusVertex temp = pair.getSecond();
            if (temp.getId().equals(1l)) {
                assertFalse(temp.getOutEdges().iterator().hasNext());
                assertEquals(temp.getPropertyKeys().size(), 1);
                assertEquals(temp.getProperty("name"), "marko");
            } else {
                Edge edge = temp.getOutEdges().iterator().next();
                assertEquals(edge.getLabel(), "created_inv");
                assertEquals(edge.getInVertex(), vertex1);
                assertEquals(edge.getOutVertex(), vertex2);
                assertEquals(temp.getPropertyKeys().size(), 1);
                assertEquals(temp.getProperty("name"), "gremlin");
            }
        }
    }

}
