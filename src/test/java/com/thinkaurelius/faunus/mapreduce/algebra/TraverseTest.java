package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.thinkaurelius.faunus.io.graph.util.TaggedHolder;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Map;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraverseTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, TaggedHolder, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, TaggedHolder, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new Traverse.Map());
        mapReduceDriver.setReducer(new Traverse.Reduce());
    }

    public void testMapReduce1() throws IOException {
        Configuration config = new Configuration();
        config.set(Traverse.FIRST_LABEL, "knows");
        config.set(Traverse.SECOND_LABEL, "created");
        config.set(Traverse.NEW_LABEL, "knowsCreated");
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.TINKERGRAPH, mapReduceDriver);
        assertEquals(results.size(), 6);
        int knowsEdges = 0;
        int createdEdges = 0;
        int knowsCreatedEdges = 0;
        int inEdges = 0;
        int outEdges = 0;

        for (final FaunusVertex vertex : results.values()) {
            for (Edge edge : vertex.getEdges(OUT)) {
                outEdges++;
                if (edge.getLabel().equals("knows"))
                    knowsEdges++;
                else if (edge.getLabel().equals("created"))
                    createdEdges++;
                else if (edge.getLabel().equals("knowsCreated"))
                    knowsCreatedEdges++;
            }

            for (Edge edge : vertex.getEdges(IN)) {
                inEdges++;
                if (edge.getLabel().equals("knows"))
                    knowsEdges++;
                else if (edge.getLabel().equals("created"))
                    createdEdges++;
                else if (edge.getLabel().equals("knowsCreated"))
                    knowsCreatedEdges++;
            }
        }

        assertEquals(outEdges, inEdges);
        assertEquals(knowsEdges % 2, 0);
        assertEquals(createdEdges % 2, 0);
        assertEquals(knowsCreatedEdges % 2, 0);

        assertEquals(knowsEdges, 4);
        assertEquals(createdEdges, 8);
        assertEquals(knowsCreatedEdges, 4);

        assertEquals(mapReduceDriver.getCounters().findCounter(Traverse.Counters.EDGES_CREATED).getValue(), knowsCreatedEdges);

        // vertex 1
        assertEquals(asList(results.get(1l).getEdges(IN)).size(), 0);
        assertEquals(asList(results.get(1l).getEdges(OUT)).size(), 5);
        assertEquals(asList(results.get(1l).getEdges(OUT, "knows")).size(), 2);
        assertEquals(asList(results.get(1l).getEdges(OUT, "created")).size(), 1);
        assertEquals(asList(results.get(1l).getEdges(OUT, "knowsCreated")).size(), 2);
        for (Edge edge : results.get(1l).getEdges(OUT, "knowsCreated")) {
            assertEquals(edge.getVertex(OUT).getId(), 1l);
            assertTrue(edge.getVertex(IN).getId().equals(3l) || edge.getVertex(IN).getId().equals(5l));
        }
        assertEquals(results.get(1l).getProperty("name"), "marko");
        assertEquals(results.get(1l).getProperty("age"), 29);
        assertEquals(results.get(1l).getPropertyKeys().size(), 2);

        // vertex 3
        assertEquals(asList(results.get(5l).getEdges(OUT)).size(), 0);
        assertEquals(asList(results.get(5l).getEdges(IN)).size(), 2);
        assertEquals(asList(results.get(5l).getEdges(IN, "created")).size(), 1);
        assertEquals(asList(results.get(5l).getEdges(IN, "knowsCreated")).size(), 1);
        assertEquals(asList(results.get(5l).getEdges(IN, "knowsCreated")).get(0).getVertex(OUT).getId(), 1l);
        assertEquals(asList(results.get(5l).getEdges(IN, "knowsCreated")).get(0).getVertex(IN).getId(), 5l);

        assertEquals(results.get(5l).getProperty("name"), "ripple");
        assertEquals(results.get(5l).getProperty("lang"), "java");
        assertEquals(results.get(5l).getPropertyKeys().size(), 2);

    }
}
