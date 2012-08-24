package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Map;

import static com.tinkerpop.blueprints.Direction.BOTH;
import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CloseTriangleTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new CloseTriangle.Map());
        mapReduceDriver.setReducer(new CloseTriangle.Reduce());
    }

    public void testKnowsCreatedTraversal() throws IOException {
        Configuration config = new Configuration();
        config.setStrings(CloseTriangle.FIRST_LABELS, "knows");
        config.setStrings(CloseTriangle.SECOND_LABELS, "created");
        config.set(CloseTriangle.NEW_LABEL, "knowsCreated");
        config.set(CloseTriangle.FIRST_DIRECTION, OUT.toString());
        config.set(CloseTriangle.SECOND_DIRECTION, OUT.toString());
        config.set(CloseTriangle.FIRST_ACTION, Tokens.Action.KEEP.name());
        config.set(CloseTriangle.SECOND_ACTION, Tokens.Action.KEEP.name());
        config.set(CloseTriangle.NEW_DIRECTION, Direction.OUT.name());

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

        assertEquals(mapReduceDriver.getCounters().findCounter(CloseTriangle.Counters.EDGES_CREATED).getValue(), knowsCreatedEdges);

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

    public void testCreatedCreatedCollaborator() throws IOException {
        Configuration config = new Configuration();
        config.setStrings(CloseTriangle.FIRST_LABELS, "created");
        config.setStrings(CloseTriangle.SECOND_LABELS, "created");
        config.set(CloseTriangle.NEW_LABEL, "collaborator");
        config.set(CloseTriangle.FIRST_DIRECTION, OUT.toString());
        config.set(CloseTriangle.SECOND_DIRECTION, IN.toString());
        config.set(CloseTriangle.FIRST_ACTION, Tokens.Action.KEEP.toString());
        config.set(CloseTriangle.SECOND_ACTION, Tokens.Action.KEEP.toString());
        config.set(CloseTriangle.NEW_DIRECTION, Direction.OUT.name());

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.TINKERGRAPH, mapReduceDriver);
        assertEquals(results.size(), 6);
        int knowsEdges = 0;
        int createdEdges = 0;
        int collaboratorEdges = 0;
        int inEdges = 0;
        int outEdges = 0;

        /*for (FaunusVertex vertex : results.values()) {
            System.out.println(vertex + " " + vertex.getEdges(OUT));
        }*/

        for (final FaunusVertex vertex : results.values()) {
            for (Edge edge : vertex.getEdges(OUT)) {
                outEdges++;
                if (edge.getLabel().equals("knows"))
                    knowsEdges++;
                else if (edge.getLabel().equals("created"))
                    createdEdges++;
                else if (edge.getLabel().equals("collaborator"))
                    collaboratorEdges++;
            }

            for (Edge edge : vertex.getEdges(IN)) {
                inEdges++;
                if (edge.getLabel().equals("knows"))
                    knowsEdges++;
                else if (edge.getLabel().equals("created"))
                    createdEdges++;
                else if (edge.getLabel().equals("collaborator"))
                    collaboratorEdges++;
            }
        }

        assertEquals(outEdges, inEdges);
        assertEquals(knowsEdges % 2, 0);
        assertEquals(createdEdges % 2, 0);
        assertEquals(collaboratorEdges % 2, 0);

        assertEquals(knowsEdges, 4);
        assertEquals(createdEdges, 8);
        assertEquals(collaboratorEdges, 20);

        assertEquals(mapReduceDriver.getCounters().findCounter(CloseTriangle.Counters.EDGES_CREATED).getValue(), collaboratorEdges);

        // vertex 1
        assertEquals(asList(results.get(1l).getEdges(IN)).size(), 3);
        assertEquals(asList(results.get(1l).getEdges(OUT)).size(), 6);
        assertEquals(asList(results.get(1l).getEdges(OUT, "knows")).size(), 2);
        assertEquals(asList(results.get(1l).getEdges(OUT, "created")).size(), 1);
        assertEquals(asList(results.get(1l).getEdges(OUT, "collaborator")).size(), 3);
        for (Edge edge : results.get(1l).getEdges(OUT, "collaborator")) {
            assertEquals(edge.getVertex(OUT).getId(), 1l);
            assertTrue(edge.getVertex(IN).getId().equals(1l) || edge.getVertex(IN).getId().equals(4l) || edge.getVertex(IN).getId().equals(6l));
        }
        assertEquals(results.get(1l).getProperty("name"), "marko");
        assertEquals(results.get(1l).getProperty("age"), 29);
        assertEquals(results.get(1l).getPropertyKeys().size(), 2);

    }

    public void testFatherFatherGrandfather() throws IOException {

        Configuration config = new Configuration();
        config.setStrings(CloseTriangle.FIRST_LABELS, "father");
        config.setStrings(CloseTriangle.SECOND_LABELS, "father");
        config.set(CloseTriangle.NEW_LABEL, "grandfather");
        config.set(CloseTriangle.FIRST_DIRECTION, OUT.toString());
        config.set(CloseTriangle.SECOND_DIRECTION, OUT.toString());
        config.set(CloseTriangle.FIRST_ACTION, Tokens.Action.DROP.toString());
        config.set(CloseTriangle.SECOND_ACTION, Tokens.Action.DROP.toString());
        config.set(CloseTriangle.NEW_DIRECTION, Direction.OUT.name());

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.GRAPH_OF_THE_GODS, mapReduceDriver);
        assertEquals(results.size(), 12);

        for (final FaunusVertex vertex : results.values()) {
            assertEquals(asList(vertex.getEdges(BOTH, "father")).size(), 0);
            if (vertex.getProperty("name").equals("hercules")) {
                assertEquals(asList(vertex.getEdges(IN, "grandfather")).size(), 0);
                assertEquals(asList(vertex.getEdges(OUT, "grandfather")).size(), 1);
                assertEquals(vertex.getEdges(OUT, "grandfather").iterator().next().getVertex(IN).getId(), 0l);
            } else if (vertex.getProperty("name").equals("saturn")) {
                assertEquals(asList(vertex.getEdges(IN, "grandfather")).size(), 1);
                assertEquals(asList(vertex.getEdges(OUT, "grandfather")).size(), 0);
                assertEquals(vertex.getEdges(IN, "grandfather").iterator().next().getVertex(OUT).getId(), 7l);
            } else {
                assertEquals(asList(vertex.getEdges(OUT, "grandfather")).size(), 0);
                assertEquals(asList(vertex.getEdges(IN, "grandfather")).size(), 0);
                assertFalse(vertex.getProperty("name").equals("hercules") || vertex.getProperty("name").equals("saturn"));
            }
        }
    }
}
