package com.thinkaurelius.titan.graphdb

import static org.junit.Assert.*

import org.apache.commons.configuration.Configuration
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.carrotsearch.junitbenchmarks.BenchmarkOptions
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Iterables
import com.thinkaurelius.titan.core.TitanEdge
import com.thinkaurelius.titan.core.TitanGraph
import com.thinkaurelius.titan.core.TitanKey
import com.thinkaurelius.titan.core.TitanVertex
import com.thinkaurelius.titan.diskstorage.StorageException
import com.thinkaurelius.titan.graphdb.FakeVertex
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph
import com.thinkaurelius.titan.testutil.GraphGenerator
import com.thinkaurelius.titan.testutil.JUnitBenchmarkProvider
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.frames.FramedGraph
import com.tinkerpop.gremlin.Tokens.T
import com.tinkerpop.gremlin.groovy.Gremlin


@BenchmarkOptions(warmupRounds=1, benchmarkRounds=1)
abstract class GroovySerialTest {

    @Rule
    public TestRule benchmark = JUnitBenchmarkProvider.get();
    
    static {
        Gremlin.load()
    }
    
    protected final Random random = new Random(7) // Arbitrary seed
    protected final GraphGenerator gen
    protected final TitanGraph graph
    protected final Configuration conf
    
    protected static final int VERTEX_COUNT = 10 * 1000
    protected static final int EDGE_COUNT = VERTEX_COUNT * 5
    private static final int TX_COUNT = 10
    private static final int OPS_PER_TX = 100
    private static final Logger LOG = LoggerFactory.getLogger(GroovySerialTest)

    GroovySerialTest(Configuration conf) throws StorageException {
        this.conf = conf;
    }
    
    @Before
    void open() {
        Preconditions.checkArgument(TX_COUNT * OPS_PER_TX <= VERTEX_COUNT);
        
        if (null == graph) {
            try {
                graph = getGraph();
            } catch (StorageException e) {
                throw new RuntimeException(e);
            }
        }
        if (null == gen)
            gen = getGenerator()
    }
    
    @After
    void rollback() {
        if (null != graph)
            graph.rollback()
    }
    
    void close() {
        if (null != graph)
            graph.shutdown()
    }

    protected abstract StandardTitanGraph getGraph() throws StorageException;
    protected abstract GraphGenerator getGenerator();
    
    /**
     * Retrieve 100 vertices, each by its exact uid. Repeat the process with
     * different uids in 50 transactions. The transactions are read-only and are
     * all rolled back rather than committed.
     * 
     */
    @Test
    void testVertexUidLookup() throws Exception {
        rollbackTx({ txIndex, tx ->
            TitanKey uidKey = tx.getPropertyKey(GraphGenerator.UID_PROP)
            for (int u = 0; u < GroovySerialTest.OPS_PER_TX; u++) {
                Preconditions.checkNotNull(uidKey)
                long uid = txIndex * GroovySerialTest.OPS_PER_TX + u
                TitanVertex v = tx.getVertex(uidKey, uid)
                assertNotNull(v)
                assertEquals(uid, v.getProperty(uidKey))
            }
        })
    }
    
    /**
     * Same as {@link #testVertexUidLookup}, except add or modify a single property
     * on every vertex retrieved and commit the changes in each transaction
     * instead of rolling back.
     * 
     */
    @Test
    void testVertexPropertyModification() {
        commitTx({ txIndex, tx ->
            TitanKey uidKey = tx.getPropertyKey(GraphGenerator.UID_PROP)
            for (int u = 0; u < GroovySerialTest.OPS_PER_TX; u++) {
                Preconditions.checkNotNull(uidKey)
                long uid = txIndex * GroovySerialTest.OPS_PER_TX + u
                TitanVertex v = tx.getVertex(uidKey, uid)
                assertNotNull("Vertex ID #" + uid + " failed", v)
                assertEquals(uid, v.getProperty(uidKey))
                Set<String> props = ImmutableSet.copyOf(v.getPropertyKeys())
                String propKeyToModify = gen.getVertexPropertyName(random.nextInt(gen.getVertexPropKeys()))
                if (props.contains(propKeyToModify) && !propKeyToModify.equals(GraphGenerator.UID_PROP)) {
                    v.removeProperty(propKeyToModify)
                    v.setProperty(propKeyToModify, random.nextInt(gen.getMaxVertexPropVal()))
                }
            }
        })
    }

    /**
     * Retrieve a vertex by randomly chosen uid, then delete its first edge.
     * 
     */
    @Test
    void testEdgeRemoval() {
        int deleted = 0;
        commitTx({ txIndex, tx ->
            TitanKey uidKey = tx.getPropertyKey(GraphGenerator.UID_PROP)
            for (int u = 0; u < GroovySerialTest.OPS_PER_TX; u++) {
                long uid = Math.abs(random.nextLong()) % gen.getMaxUid()
                Preconditions.checkArgument(null != uidKey)
                TitanVertex v = tx.getVertex(uidKey, uid)
                assertNotNull(v)
                Iterable<TitanEdge> edges = v.getEdges()
                assertNotNull(edges)
                TitanEdge e = Iterables.getFirst(edges, null)
                if (null == e) {
                    u--
                    continue
                }
                e.remove()
                deleted++
            }
        })
        assertEquals(TX_COUNT * GroovySerialTest.OPS_PER_TX, deleted);
    }

    /**
     * Retrieve a vertex by randomly chosen uid, then remove the vertex. After
     * removing all vertices, add new vertices with the same uids as those
     * removed (but no incident edges or properties besides uid)
     * 
     */
    @Test
    void testVertexRemoval() {
        Set<Long> visited = new HashSet<Long>(TX_COUNT);
        commitTx({ txIndex, tx ->
            long uid
            Vertex v = null
            while (null == v) {
                uid = Math.abs(random.nextLong()) % gen.getMaxUid();
                TitanKey uidKey = tx.getPropertyKey(GraphGenerator.UID_PROP);
                v = tx.getVertex(uidKey, uid);
            }
            assertNotNull(v)
            tx.removeVertex(v)
            visited.add(uid)
        })
        
        def tx = graph.newTransaction()
        // Insert new vertices with the same uids as removed vertices, but no edges or properties besides uid
        TitanKey uidKey = tx.getPropertyKey(GraphGenerator.UID_PROP)
        for (long uid : visited) {
            TitanVertex v = tx.addVertex()
            v.setProperty(uidKey, uid)
        }
        tx.commit()
    }
    
    /**
     * JUnitBenchmarks appears to include {@code Before} method execution in round-avg times.
     * This method has no body and exists only to measure that overhead.
     */
    @Test
    void testNoop() {
        // Do nothing
    }
    
    /**
     * Query for edges using a vertex-centric index on a known high-out-degree vertex
     * 
     */
    @Test
    void testVertexCentricIndexQuery() {
        Preconditions.checkArgument(1000 <= VERTEX_COUNT)
        
        long uid = gen.getHighDegVertexUid()
        String label = gen.getHighDegEdgeLabel()
        assertNotNull(label)
        String pkey  = gen.getPrimaryKeyForLabel(label)
        assertNotNull(pkey)
        def v = graph.V(GraphGenerator.UID_PROP, uid).next()
        assertNotNull(v)
        
        def c = v.outE(label)
             .has(pkey, T.gte, 25)
             .has(pkey, T.lt,  75)
             .count()
        assertEquals(50, c)
        
        c = v.outE(label)
            .has(pkey, T.gte, 125)
            .has(pkey, T.lt,  225)
            .count()
        assertEquals(100, c)
        
        c = v.outE(label)
             .has(pkey, T.gte, (int)((VERTEX_COUNT / 2) - 250))
             .has(pkey, T.lt,  (int)((VERTEX_COUNT / 2) + 250))
             .count()
        assertEquals(500, c)
             
        c = v.outE(label)
             .has(pkey, T.gt, 0)
             .has(pkey, T.lt, 2)
             .count()
        assertEquals(1, c)
    }
    
    @Test
    void testLabeledEdgeTraversal() {
        long uid = gen.getHighDegVertexUid()
        String label = gen.getHighDegEdgeLabel()
        assertNotNull(label)
        def v = graph.V(GraphGenerator.UID_PROP, uid).next()
        assertNotNull(v)
        
        def c = v.outE(label)[0..99].inV().outE(label).count()
        assertTrue(0 < c)
    }
    
    @Test
    void testEdgeTraversalUsingVertexCentricIndex() {
        long uid = gen.getHighDegVertexUid()
        String label = gen.getHighDegEdgeLabel()
        assertNotNull(label)
        String pkey  = gen.getPrimaryKeyForLabel(label)
        assertNotNull(pkey)
        def v = graph.V(GraphGenerator.UID_PROP, uid).next()
        assertNotNull(v)
        
        def c = v.outE(label)
                 .has(pkey, T.gte, 1).has(pkey, T.lte, 100)
                 .inV()
                 .outE(label)
                 .count()
        assertTrue(0 < c)
    }
    
    /**
     * Same query as in {@link #testEdgePropertyQuery()}, except with limit(1).
     * 
     */
    @Test
    void testLimitedEdgeQuery() {
        rollbackTx({ txIndex, tx ->
            for (int u = 0; u < GroovySerialTest.OPS_PER_TX; u++) {
                int n = Iterables.size(tx.query().limit(1).has(gen.getEdgePropertyName(0), 0).edges());
                assertTrue(0 <= n);
                assertTrue(n <= 1);
            }
        })
    }
    
    /**
     * Retrieve all vertices with an OUT-unique standard-indexed property and limit(1).
     * 
     */
    @Test
    void testLimitedVertexQuery() {
        rollbackTx({ txIndex, tx ->
            for (int u = 0; u < GroovySerialTest.OPS_PER_TX; u++) {
                int n = Iterables.size(tx.query().limit(1).has(gen.getVertexPropertyName(0), 0).vertices());
                assertTrue(0 <= n);
                assertTrue(n <= 1);
            }
        })
    }
    
    /**
     * Retrieve all vertices with uid equal to a randomly chosen value. Note
     * that uid is standard-indexed and BOTH-unique, so this query should return
     * one vertex in practice, but no limit is specified.
     * 
     */
    @Test
    void testVertexPropertyQuery() {
        rollbackTx({ txIndex, tx ->
            for (int u = 0; u < GroovySerialTest.OPS_PER_TX; u++) {
                int n = Iterables.size(tx.query().has(GraphGenerator.UID_PROP, Math.abs(random.nextLong()) % gen.getMaxUid()).vertices());
                assertTrue(1 == n);
            }
        })
    }
    
    /**
     * Retrieve all edges with a single OUT-unique standard-indexed property. No limit.
     * 
     */
    @Test
    void testEdgePropertyQuery() {
        rollbackTx({ txIndex, tx ->
            int n = Iterables.size(tx.query().has(gen.getEdgePropertyName(0), 0).edges())
            assertTrue(0 < n)
        })
    }
    
    /**
     * Retrieve all edges matching on has(...) clause and one hasNot(...)
     * clause, both on OUT-unique standard-indexed properties. No limit.
     * 
     */
    @Test
    void testHasAndHasNotEdgeQuery() {
        rollbackTx({ txIndex, tx ->
            int n = Iterables.size(tx.query().has(gen.getEdgePropertyName(0), 0).hasNot(gen.getEdgePropertyName(1), 0).edges());
            assertTrue(0 < n);
        })
    }
    
    /**
     * Retrieve all vertices matching on has(...) clause and one hasNot(...)
     * clause, both on OUT-unique standard-indexed properties. No limit.
     * 
     */
    @Test
    void testHasAndHasNotVertexQuery() {
        rollbackTx({ txIndex, tx ->
            for (int u = 0; u < GroovySerialTest.OPS_PER_TX; u++) {
                int n = Iterables.size(tx.query().has(gen.getVertexPropertyName(0), 0).hasNot(gen.getVertexPropertyName(1), 0).vertices());
                assertTrue(0 < n);
            }
        })
    }
    
    /**
     * Retrieve vertices by uid, then retrieve their associated properties. All
     * access is done through a FramedGraph interface. This is inspired by part
     * of the ONLAB benchmark, but written separately ("from scratch").
     * 
     */
    @Test
    void testFramedUidAndPropertyLookup() {
        FramedGraph<TitanGraph> fg = new FramedGraph<TitanGraph>(graph);
        int totalNonNullProps = 0;
        for (int t = 0; t < TX_COUNT; t++) {
            for (int u = 0; u < GroovySerialTest.OPS_PER_TX; u++) {
                Long uid = (long)t * GroovySerialTest.OPS_PER_TX + u;
                Iterable<FakeVertex> iter = fg.getVertices(GraphGenerator.UID_PROP, uid, FakeVertex.class);
                boolean visited = false;
                for (FakeVertex fv : iter) {
                    assertTrue(uid == fv.getUid().longValue());
                    // Three property retrievals, as in ONLAB, with some
                    // busywork to attempt to prevent the runtime or compiler
                    // from optimizing this all away
                    int nonNullProps = 0;
                    if (null != fv.getProp0())
                        nonNullProps++;
                    if (null != fv.getProp1())
                        nonNullProps++;
                    if (null != fv.getProp2())
                        nonNullProps++;
                    assertTrue(0 <= nonNullProps);
                    totalNonNullProps += nonNullProps;
                    visited = true;
                }
                assertTrue(visited);
            }
        }
        // The chance of this going to zero during random scale-free graph
        // generation (for a graph of non-trivial size) is insignificant.
        assertTrue(0 < totalNonNullProps);
    }
    
    
    /*
     * Helper methods
     */
    
    void rollbackTx(closure) {
        doTx(closure, { tx -> tx.rollback() })
    }
    
    void commitTx(closure) {
        doTx(closure, { tx -> tx.commit() })
    }

    void doTx(txWork, afterWork) {
        def tx
        for (int t = 0; t < TX_COUNT; t++) {
            tx = graph.newTransaction()
            txWork.call(t, tx)
            afterWork.call(tx)
        }
    }
    
    void noTx(closure) {
        for (int t = 0; t < TX_COUNT; t++) {
            closure.call(t)
            graph.rollback()
        }
    }
    
    protected void initializeGraph(TitanGraph g) throws StorageException {
        LOG.info("Initializing graph...");
        long before = System.currentTimeMillis()
        GraphGenerator generator = getGenerator();
        GraphDatabaseConfiguration graphconfig = new GraphDatabaseConfiguration(conf);
        graphconfig.getBackend().clearStorage();
        generator.generate(g);
        long after = System.currentTimeMillis()
        long duration = after - before
        if (15 * 1000 <= duration) {
            LOG.warn("Initialized graph (" + duration + " ms).")
        } else {
            LOG.info("Initialized graph (" + duration + " ms).")
        }
    }
}