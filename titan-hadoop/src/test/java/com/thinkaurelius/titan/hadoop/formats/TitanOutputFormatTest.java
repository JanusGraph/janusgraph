package com.thinkaurelius.titan.hadoop.formats;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.HadoopGraph;
import com.thinkaurelius.titan.hadoop.HadoopPipeline;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.util.PipeHelper;

import org.apache.commons.configuration.BaseConfiguration;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanOutputFormatTest extends BaseTest {

    public void testTrue() {
        assertTrue(true);
    }

    public void bulkLoadGraphOfTheGods(final HadoopGraph f) throws Exception {
        new HadoopPipeline(f)._().submit();
    }

    public void testBulkLoading(final BaseConfiguration configuration, final HadoopGraph f1) throws Exception {
        bulkLoadGraphOfTheGods(f1);
        TitanGraph g = TitanFactory.open(configuration);
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

    public void testBulkElementDeletions(final BaseConfiguration configuration, final HadoopGraph f1, final HadoopGraph f2) throws Exception {
        bulkLoadGraphOfTheGods(f1);
        TitanGraph g = TitanFactory.open(configuration);
        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());

        new HadoopPipeline(f2).V().drop().submit();
        g = TitanFactory.open(configuration);
        assertEquals(0, new GremlinPipeline(g).V().count());
        assertEquals(0, new GremlinPipeline(g).E().count());

        bulkLoadGraphOfTheGods(f1);
        g = TitanFactory.open(configuration);
        new HadoopPipeline(f2).E().drop().submit();
        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(0, new GremlinPipeline(g).E().count());

        new HadoopPipeline(f2).V().drop().submit();
        g = TitanFactory.open(configuration);
        assertEquals(0, new GremlinPipeline(g).V().count());
        assertEquals(0, new GremlinPipeline(g).E().count());
    }

    public void testFewElementDeletions(final BaseConfiguration configuration, final HadoopGraph f1, final HadoopGraph f2) throws Exception {
        bulkLoadGraphOfTheGods(f1);
        TitanGraph g = TitanFactory.open(configuration);
        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());

        new HadoopPipeline(f2).E().has("label", "battled").drop().submit();
        g = TitanFactory.open(configuration);
        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(14, new GremlinPipeline(g).E().count());
        assertEquals(0, new GremlinPipeline(g).E().has("label", "battled").count());
        assertEquals(1, new GremlinPipeline(g).E().has("label", "mother").count());
        assertEquals(2, new GremlinPipeline(g).E().has("label", "father").count());

        new HadoopPipeline(f2).V().has("name", "hercules").drop().submit();
        g = TitanFactory.open(configuration);
        assertEquals(11, new GremlinPipeline(g).V().count());
        assertEquals(12, new GremlinPipeline(g).E().count());
        assertEquals(0, new GremlinPipeline(g).E().has("label", "battled").count());
        assertEquals(0, new GremlinPipeline(g).E().has("label", "mother").count());
        assertEquals(1, new GremlinPipeline(g).E().has("label", "father").count());
    }

    public void testBulkVertexPropertyDeletions(final BaseConfiguration configuration, final HadoopGraph f1, final HadoopGraph f2) throws Exception {
        bulkLoadGraphOfTheGods(f1);
        new HadoopPipeline(f2).V().sideEffect("{it.removeProperty('name')}").submit();
        TitanGraph g = TitanFactory.open(configuration);

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

    public void testBulkVertexPropertyUpdates(final BaseConfiguration configuration, final HadoopGraph f1, final HadoopGraph f2) throws Exception {
        TitanGraph g = TitanFactory.open(configuration);
        g.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.LIST).make();
        g.commit();

        bulkLoadGraphOfTheGods(f1);
        new HadoopPipeline(f2).V().sideEffect("{it.name = 'marko' + it.name}").submit();
        g = TitanFactory.open(configuration);
        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());

        for (Vertex v : g.getVertices()) {
            assertTrue(v.<List<String>>getProperty("name").get(0).startsWith("marko"));
            assertEquals(v.<List<String>>getProperty("name").size(), 1);
            assertEquals(2, v.getPropertyKeys().size());
        }
        new GremlinPipeline(g).V("name", "hercules").outE("battled").sideEffect(new PipeFunction<Edge, Edge>() {
            @Override
            public Edge compute(Edge edge) {
                assertNotNull(edge.getProperty("time"));
                return edge;
            }
        }).iterate();

        new HadoopPipeline(f2).V().drop().submit();
        g = TitanFactory.open(configuration);
        assertEquals(0, new GremlinPipeline(g).V().count());
        assertEquals(0, new GremlinPipeline(g).E().count());

        bulkLoadGraphOfTheGods(f1);
        g = TitanFactory.open(configuration);
        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());

        int counter = 0;
        g = TitanFactory.open(configuration);
        new HadoopPipeline(f2).V().has("name", "saturn").sideEffect("{it.addProperty('name','chronos')}").submit();
        TitanVertex v = (TitanVertex) g.getVertices("name", "saturn").iterator().next();
        for (Object property : new GremlinPipeline(v).transform(new PipeFunction<TitanVertex, Iterable<TitanProperty>>() {
            @Override
            public Iterable<TitanProperty> compute(TitanVertex vertex) {
                return vertex.getProperties("name");
            }
        }).scatter().toList()) {
            String value = (String) ((TitanProperty) property).getValue();
            assertTrue(value.equals("saturn") || value.equals("chronos"));
            counter++;
        }
        assertEquals(counter, 2);
    }

    public void testBulkEdgeDerivations(final BaseConfiguration configuration, final HadoopGraph f1, final HadoopGraph f2) throws Exception {
        bulkLoadGraphOfTheGods(f1);
        new HadoopPipeline(f2).V().as("x").out("father").out("father").linkIn("grandfather", "x").submit();
        TitanGraph g = TitanFactory.open(configuration);

        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(18, new GremlinPipeline(g).E().count());

        assertTrue(PipeHelper.areEqual(
                new GremlinPipeline(g).V("name", "hercules").out("father").out("father"),
                new GremlinPipeline(g).V("name", "hercules").out("grandfather")));
    }

    public void testBulkEdgePropertyUpdates(final BaseConfiguration configuration, final HadoopGraph f1, final HadoopGraph f2) throws Exception {
        bulkLoadGraphOfTheGods(f1);
        new HadoopPipeline(f2).E().has("label", "battled").sideEffect("{it.time = it.time+1}").submit();
        TitanGraph g = TitanFactory.open(configuration);

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

    public void testUnidirectionEdges(final BaseConfiguration configuration, final HadoopGraph f1, final HadoopGraph f2) throws Exception {
        TitanGraph g = TitanFactory.open(configuration);
        g.makeEdgeLabel("father").unidirected().make();
        g.commit();

        bulkLoadGraphOfTheGods(f1);
        g = TitanFactory.open(configuration);
        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());
        assertEquals(new GremlinPipeline(g).V("name", "hercules").out("father").count(), 1);
        assertEquals(new GremlinPipeline(g).V("name", "jupiter").in("father").count(), 0);
    }

    // TODO: Unidirectional edges test cases
    // TODO: Multi-properties
}
