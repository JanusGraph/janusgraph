package com.thinkaurelius.titan.graphdb;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Titan;
import com.thinkaurelius.titan.core.TitanGraphIndex;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanManagement;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.attribute.Decimal;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class TtlTest extends TitanGraphBaseTest {

    @Test
    public void testPerEdgeTtlTiming() throws Exception {

        Vertex v1 = graph.addVertex(null), v2 = graph.addVertex(null), v3 = graph.addVertex(null);
        int ttl1 = 1;
        int ttl2 = 2;

        Edge e1 = graph.addEdge(null, v1, v2, "likes");
        Edge e2 = graph.addEdge(null, v2, v1, "likes");
        Edge e3 = graph.addEdge(null, v3, v1, "likes");

        e1.setProperty(Titan.TTL, ttl1);
        e2.setProperty(Titan.TTL, ttl2);

        // initial, pre-commit state of the edges.  They are not yet subject to TTL
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v2.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v3.getVertices(Direction.OUT).iterator().hasNext());

        long start = System.currentTimeMillis();
        graph.commit();

        // edges are now subject to TTL, although we must commit() or rollback() to see it
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v2.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v3.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(start + 1100 - System.currentTimeMillis());
        graph.rollback();

        // e1 has dropped out
        assertFalse(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v2.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v3.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(start + 2100 - System.currentTimeMillis());
        graph.rollback();

        // both e1 and e2 have dropped out.  e3 has no TTL, and so remains
        assertFalse(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertFalse(v2.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v3.getVertices(Direction.OUT).iterator().hasNext());
    }

    @Test
    public void testCommitRequiredForTtl() throws Exception {

        Vertex v1 = graph.addVertex(null), v2 = graph.addVertex(null);

        Edge e1 = graph.addEdge(null, v1, v2, "likes");
        e1.setProperty(Titan.TTL, 1);

        // pre-commit state of the edge.  It is not yet subject to TTL
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(1001);

        // the edge should have expired by now, but only if it had been committed
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());

        graph.commit();

        // still here, because we have just committed the edge.  Its countdown starts at the commit
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(1001);

        // the edge has expired in Cassandra, but still appears alive in this transaction
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());

        // syncing with Cassandra, we see that the edge has expired
        graph.rollback();
        assertFalse(v1.getVertices(Direction.OUT).iterator().hasNext());
    }

    @Test
    public void testPerLabelTtl() throws Exception {
        graph.makeLabel("likes").ttl(1).make();
        graph.commit();

        Vertex v1 = graph.addVertex(null), v2 = graph.addVertex(null);

        // no need to explicitly set TTL
        graph.addEdge(null, v1, v2, "likes");
        graph.commit();

        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(1001);
        graph.rollback();

        assertFalse(v1.getVertices(Direction.OUT).iterator().hasNext());
    }

    @Test
    public void testPerEdgeTtlOverridesPerLabelTtl() throws Exception {
        // timeout of 1 second by label
        graph.makeLabel("likes").ttl(1).make();
        graph.commit();

        Vertex v1 = graph.addVertex(null), v2 = graph.addVertex(null);

        // explicit, per-edge TTL of 5s
        Edge e1 = graph.addEdge(null, v1, v2, "likes");
        e1.setProperty(Titan.TTL, 5);

        // implicit, per-label TTL of 1s
        graph.addEdge(null, v2, v1, "likes");

        long start = System.currentTimeMillis();
        graph.commit();

        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v2.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(start + 1001 - System.currentTimeMillis());
        graph.rollback();

        // 1s edge has expired, 5s edge still here
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertFalse(v2.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(start + 5001 - System.currentTimeMillis());
        graph.rollback();

        // both edges expired
        assertFalse(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertFalse(v2.getVertices(Direction.OUT).iterator().hasNext());
    }

    @Test
    public void testKeyindexWithTtl() throws Exception {
        TitanManagement tm = graph.getManagementSystem();
        TitanKey key = tm.makeKey("edge-name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        tm.createInternalIndex("edge-name", Edge.class, true, key);
        tm.commit();

        graph.makeLabel("likes").ttl(1).make();
        graph.commit();

        Vertex v1 = graph.addVertex(null), v2 = graph.addVertex(null);

        Edge e = graph.addEdge(null, v1, v2, "likes");
        e.setProperty("edge-name", "v1-likes-v2");

        graph.commit();

        assertTrue(v1.getEdges(Direction.OUT).iterator().hasNext());
        assertTrue(graph.getEdges("edge-name", "v1-likes-v2").iterator().hasNext());

        Thread.sleep(1001);

        graph.rollback();

        // the edge is gone not only from its previous endpoints, but also from key indices
        assertFalse(v1.getEdges(Direction.OUT).iterator().hasNext());
        assertFalse(graph.getEdges("edge-name", "v1-likes-v2").iterator().hasNext());
    }
}
