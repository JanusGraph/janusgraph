package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.util.TaggedHolder;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.List;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TransposeTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, TaggedHolder, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, TaggedHolder, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new Transpose.Map());
        mapReduceDriver.setReducer(new Transpose.Reduce());
    }

    public void testMapReduce1() throws IOException {
        FaunusVertex vertex1 = new FaunusVertex(1);
        vertex1.setProperty("name", "marko");
        FaunusVertex vertex2 = new FaunusVertex(2);
        vertex2.setProperty("name", "gremlin");
        vertex1.addEdge(OUT, new FaunusEdge(vertex1, vertex2, "created"));
        vertex2.addEdge(IN, new FaunusEdge(vertex1, vertex2, "created"));

        Configuration config = new Configuration();
        config.set(Transpose.LABEL, "created");
        config.set(Transpose.NEW_LABEL, "createdBy");
        mapReduceDriver.withConfiguration(config);
        mapReduceDriver.withInput(NullWritable.get(), vertex1).withInput(NullWritable.get(), vertex2);
        List<Pair<NullWritable, FaunusVertex>> list = mapReduceDriver.run();
        assertEquals(list.size(), 2);
        for (Pair<NullWritable, FaunusVertex> pair : list) {
            assertEquals(pair.getFirst(), NullWritable.get());
            FaunusVertex temp = pair.getSecond();
            if (temp.getId().equals(1l)) {
                List<Edge> edges = asList(temp.getEdges(Direction.OUT));
                assertEquals(edges.size(), 1);
                assertEquals(edges.get(0).getLabel(), "created");

                edges = asList(temp.getEdges(Direction.IN));
                assertEquals(edges.size(), 1);
                assertEquals(edges.get(0).getLabel(), "createdBy");

                assertEquals(temp.getPropertyKeys().size(), 1);
                assertEquals(temp.getProperty("name"), "marko");
            } else {
                List<Edge> edges = asList(temp.getEdges(Direction.OUT));
                assertEquals(edges.size(), 1);
                assertEquals(edges.get(0).getLabel(), "createdBy");
                assertEquals(edges.get(0).getVertex(Direction.IN), vertex1);
                assertEquals(edges.get(0).getVertex(Direction.OUT), vertex2);

                edges = asList(temp.getEdges(Direction.IN));
                assertEquals(edges.size(), 1);
                assertEquals(edges.get(0).getLabel(), "created");
                assertEquals(edges.get(0).getVertex(Direction.IN), vertex2);
                assertEquals(edges.get(0).getVertex(Direction.OUT), vertex1);

                assertEquals(temp.getPropertyKeys().size(), 1);
                assertEquals(temp.getProperty("name"), "gremlin");
            }
        }

        //assertEquals(mapReduceDriver.getCounters().findCounter(Counters.EDGES_TRANSPOSED).getValue(), 1);
    }

}
