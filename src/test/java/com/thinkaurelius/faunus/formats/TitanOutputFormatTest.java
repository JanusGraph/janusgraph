package com.thinkaurelius.faunus.formats;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusPipeline;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.util.PipeHelper;
import org.apache.hadoop.conf.Configuration;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanOutputFormatTest extends BaseTest {

    public void testTrue() {
        assertTrue(true);
    }

    public FaunusGraph generateFaunusGraph(final InputStream inputStream) throws Exception {
        Configuration configuration = new Configuration();
        Properties properties = new Properties();
        properties.load(inputStream);
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            configuration.set(entry.getKey().toString(), entry.getValue().toString());
        }
        return new FaunusGraph(configuration);
    }

    public void bulkLoadGraphOfTheGods(final FaunusGraph f) throws Exception {
        new FaunusPipeline(f)._().submit();
    }

    public void testBulkLoading(final TitanGraph g, final FaunusGraph f1) throws Exception {
        bulkLoadGraphOfTheGods(f1);
        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());
        new GremlinPipeline(g).V().sideEffect(new PipeFunction<Vertex, Vertex>() {
            @Override
            public Vertex compute(Vertex vertex) {
                assertEquals(2, vertex.getPropertyKeys().size());
                assertNotNull(vertex.getProperty("name"));
                return vertex;
            }
        }).iterate();
        assertEquals("saturn", new GremlinPipeline(g).V("name", "hercules").out("father").out("father").property("name").next());
        List names = new GremlinPipeline(g).V("name", "hercules").outE("battled").sideEffect(new PipeFunction<Edge, Edge>() {
            @Override
            public Edge compute(Edge edge) {
                assertNotNull(edge.getProperty("time"));
                return edge;
            }
        }).inV().property("name").toList();
        assertTrue(names.contains("nemean"));
        assertTrue(names.contains("hydra"));
        assertTrue(names.contains("cerberus"));
    }

    public void testBulkElementDeletions(final TitanGraph g, final FaunusGraph f1, FaunusGraph f2) throws Exception {
        bulkLoadGraphOfTheGods(f1);
        new FaunusPipeline(f2).V().drop().submit();

        assertEquals(0, new GremlinPipeline(g).V().count());
        assertEquals(0, new GremlinPipeline(g).E().count());

        bulkLoadGraphOfTheGods(f1);
        new FaunusPipeline(f2).E().drop().submit();

        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(0, new GremlinPipeline(g).E().count());
    }

    public void testBulkVertexPropertyDeletions(final TitanGraph g, final FaunusGraph f1, FaunusGraph f2) throws Exception {
        bulkLoadGraphOfTheGods(f1);
        new FaunusPipeline(f2).V().sideEffect("{it.removeProperty('name')}").submit();

        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());

        for (Vertex v : g.getVertices()) {
            assertNull(v.getProperty("name"));
            assertEquals(1, v.getPropertyKeys().size());
        }
        new GremlinPipeline(g).V("name", "hercules").outE("battled").sideEffect(new PipeFunction<Edge, Edge>() {
            @Override
            public Edge compute(Edge edge) {
                assertNotNull(edge.getProperty("time"));
                return edge;
            }
        }).iterate();
    }

    public void testBulkVertexPropertyUpdates(final TitanGraph g, final FaunusGraph f1, FaunusGraph f2) throws Exception {
        bulkLoadGraphOfTheGods(f1);
        new FaunusPipeline(f2).V().sideEffect("{it.name = 'marko' + it.name}").submit();

        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());

        for (Vertex v : g.getVertices()) {
            assertTrue(v.<String>getProperty("name").startsWith("marko"));
            assertEquals(2, v.getPropertyKeys().size());
        }
        new GremlinPipeline(g).V("name", "hercules").outE("battled").sideEffect(new PipeFunction<Edge, Edge>() {
            @Override
            public Edge compute(Edge edge) {
                assertNotNull(edge.getProperty("time"));
                return edge;
            }
        }).iterate();

        new FaunusPipeline(f2).V().drop().submit();
        assertEquals(0, new GremlinPipeline(g).V().count());
        assertEquals(0, new GremlinPipeline(g).E().count());
        bulkLoadGraphOfTheGods(f1);
        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());

        /*int counter = 0;
        TitanVertex v = (TitanVertex) g.getVertices("name", "saturn").iterator().next();
        new FaunusPipeline(f2).V().has("name", "saturn").sideEffect("{it.addProperty('name','chronos')}").submit();
        for (Object property : new GremlinPipeline(v).transform(new PipeFunction<TitanVertex, Iterable<TitanProperty>>() {
            @Override
            public Iterable<TitanProperty> compute(TitanVertex vertex) {
                return vertex.getProperties("name");
            }
        }).toList()) {
            String value = (String) ((TitanProperty) property).getValue();
            assertTrue(value.equals("saturn") || value.equals("chronos"));
            counter++;
        }
        assertEquals(counter, 2);*/


    }

    public void testBulkEdgeDerivations(final TitanGraph g, final FaunusGraph f1, FaunusGraph f2) throws Exception {
        bulkLoadGraphOfTheGods(f1);
        new FaunusPipeline(f2).V().as("x").out("father").out("father").linkIn("grandfather", "x").submit();

        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(18, new GremlinPipeline(g).E().count());

        assertTrue(PipeHelper.areEqual(
                new GremlinPipeline(g).V("name", "hercules").out("father").out("father"),
                new GremlinPipeline(g).V("name", "hercules").out("grandfather")));
    }

    public void testBulkEdgePropertyUpdates(final TitanGraph g, final FaunusGraph f1, FaunusGraph f2) throws Exception {
        bulkLoadGraphOfTheGods(f1);
        new FaunusPipeline(f2).E().has("label", "battled").sideEffect("{it.time = it.time+1}").submit();

        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());

        int counter = 0;
        for (Edge edge : g.getEdges()) {
            if (edge.getLabel().equals("battled")) {
                assertEquals(edge.getPropertyKeys().size(), 1);
                int time = edge.getProperty("time");
                assertTrue(time == 2 || time == 3 || time == 13);
                counter++;
            } else {
                assertEquals(edge.getPropertyKeys().size(), 0);
            }
        }
        assertEquals(counter, 3);
        assertEquals(3, new GremlinPipeline(g).V("name", "hercules").outE("battled").count());
        assertEquals(1, new GremlinPipeline(g).V("name", "hercules").outE("father").count());
        assertEquals(1, new GremlinPipeline(g).V("name", "hercules").outE("mother").count());
        assertEquals(3, new GremlinPipeline(g).V("name", "hercules").out("battled").count());
        assertEquals(1, new GremlinPipeline(g).V("name", "hercules").out("father").count());
        assertEquals(1, new GremlinPipeline(g).V("name", "hercules").out("mother").count());
        assertEquals(5, new GremlinPipeline(g).V("name", "hercules").out().count());
    }
}
