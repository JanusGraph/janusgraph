package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.derivations.Identity;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Map;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new Identity.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testIdentity() throws IOException {
        Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 6);

        // vertex 1
        FaunusVertex vertex = results.get(1l);
        assertEquals(vertex.getProperty("name"), "marko");
        assertEquals(vertex.getProperty("age"), 29);
        assertEquals(vertex.getPropertyKeys().size(), 2);
        assertEquals(asList(vertex.getEdges(OUT)).size(), 3);
        assertEquals(asList(vertex.getEdges(OUT, "knows")).size(), 2);
        assertEquals(asList(vertex.getEdges(OUT, "created")).size(), 1);

        assertEquals(asList(vertex.getEdges(OUT, "created")).get(0).getId(), 9l);
        assertEquals(asList(vertex.getEdges(OUT, "created")).get(0).getLabel(), "created");
        assertEquals(asList(vertex.getEdges(OUT, "created")).get(0).getVertex(IN).getId(), 3l);

        //vertex 2
        vertex = results.get(2l);
        assertEquals(vertex.getProperty("name"), "vadas");
        assertEquals(vertex.getProperty("age"), 27);
        assertEquals(vertex.getPropertyKeys().size(), 2);
        assertFalse(vertex.getEdges(OUT).iterator().hasNext());
        assertEquals(asList(vertex.getEdges(IN)).size(), 1);
        assertEquals(asList(vertex.getEdges(IN)).iterator().next().getLabel(), "knows");
        assertEquals(asList(vertex.getEdges(IN)).iterator().next().getVertex(OUT).getId(), 1l);

        // vertex 6
        vertex = results.get(6l);
        assertEquals(vertex.getProperty("name"), "peter");
        assertEquals(vertex.getProperty("age"), 35);
        assertEquals(vertex.getPropertyKeys().size(), 2);
        assertFalse(vertex.getEdges(IN).iterator().hasNext());
        assertEquals(asList(vertex.getEdges(OUT)).size(), 1);
        assertEquals(asList(vertex.getEdges(OUT)).iterator().next().getLabel(), "created");
        assertEquals(asList(vertex.getEdges(OUT)).iterator().next().getVertex(IN).getId(), 3l);

    }
}
