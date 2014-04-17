package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.Titan;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class ExternalEmbeddedCassandraTtlTest {

    private TransactionalGraph g;

    @Test
    public void testPerEdgeTtlTiming() throws Exception {

        Vertex v1 = g.addVertex(null), v2 = g.addVertex(null), v3 = g.addVertex(null);
        int ttl1 = 1;
        int ttl2 = 2;

        Edge e1 = g.addEdge(null, v1, v2, "likes");
        Edge e2 = g.addEdge(null, v2, v1, "likes");
        Edge e3 = g.addEdge(null, v3, v1, "likes");

        e1.setProperty(Titan.TTL, ttl1);
        e2.setProperty(Titan.TTL, ttl2);

        // initial, pre-commit state of the edges.  They are not yet subject to TTL
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v2.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v3.getVertices(Direction.OUT).iterator().hasNext());

        long start = System.currentTimeMillis();
        g.commit();

        // edges are now subject to TTL, although we must commit() or rollback() to see it
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v2.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v3.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(start + 1100 - System.currentTimeMillis());
        g.rollback();

        // e1 has dropped out
        assertFalse(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v2.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v3.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(start + 2100 - System.currentTimeMillis());
        g.rollback();

        // both e1 and e2 have dropped out.  e3 has no TTL, and so remains
        assertFalse(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertFalse(v2.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v3.getVertices(Direction.OUT).iterator().hasNext());
    }

    @Test
    public void testCommitRequiredForTtl() throws Exception {

        Vertex v1 = g.addVertex(null), v2 = g.addVertex(null);

        Edge e1 = g.addEdge(null, v1, v2, "likes");
        e1.setProperty(Titan.TTL, 1);

        // pre-commit state of the edge.  It is not yet subject to TTL
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(1001);

        // the edge should have expired by now, but only if it had been committed
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());

        g.commit();

        // still here, because we have just committed the edge.  Its countdown starts at the commit
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(1001);

        // the edge has expired in Cassandra, but still appears alive in this transaction
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());

        // syncing with Cassandra, we see that the edge has expired
        g.rollback();
        assertFalse(v1.getVertices(Direction.OUT).iterator().hasNext());
    }

    @Test
    public void testPerLabelTtl() throws Exception {
        ((TitanGraph) g).makeLabel("likes").ttl(1).make();
        g.commit();

        Vertex v1 = g.addVertex(null), v2 = g.addVertex(null);

        // no need to explicitly set TTL
        g.addEdge(null, v1, v2, "likes");
        g.commit();

        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(1001);
        g.rollback();

        assertFalse(v1.getVertices(Direction.OUT).iterator().hasNext());
    }

    @Test
    public void testPerEdgeTtlOverridesPerLabelTtl() throws Exception {
        // timeout of 1 second by label
        ((TitanGraph) g).makeLabel("likes").ttl(1).make();
        g.commit();

        Vertex v1 = g.addVertex(null), v2 = g.addVertex(null);

        // explicit, per-edge TTL of 3s
        Edge e1 = g.addEdge(null, v1, v2, "likes");
        e1.setProperty(Titan.TTL, 3);

        // implicit, per-label TTL of 1s
        g.addEdge(null, v2, v1, "likes");

        long start = System.currentTimeMillis();
        g.commit();

        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v2.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(start + 1001 - System.currentTimeMillis());
        g.rollback();

        // 1s edge has expired, 3s edge still here
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertFalse(v2.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(start + 3001 - System.currentTimeMillis());
        g.rollback();

        // both edges expired
        assertFalse(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertFalse(v2.getVertices(Direction.OUT).iterator().hasNext());
    }

    @Before
    public void startUp() {
        generateGraph();
    }

    @After
    public void shutDown() throws StorageException {
        cleanUp();
    }

    public Graph generateGraph() {
        g = TitanFactory.open(getGraphConfig());
        return g;
    }

    public void cleanUp() throws StorageException {
        StandardTitanGraph graph = (StandardTitanGraph) generateGraph();
        graph.getConfiguration().getBackend().clearStorage();
        graph.shutdown();
    }

    protected WriteConfiguration getGraphConfig() {
        return CassandraStorageSetup.getEmbeddedGraphConfiguration(getClass().getSimpleName());
    }
}
