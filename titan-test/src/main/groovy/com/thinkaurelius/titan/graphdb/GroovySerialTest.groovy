package com.thinkaurelius.titan.graphdb

import static org.junit.Assert.*

import org.apache.commons.configuration.Configuration
import org.junit.FixMethodOrder;
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runners.MethodSorters
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.carrotsearch.junitbenchmarks.BenchmarkOptions
import com.google.common.base.Preconditions
import com.google.common.collect.Iterables
import com.thinkaurelius.titan.core.TitanEdge
import com.thinkaurelius.titan.core.TitanGraph
import com.thinkaurelius.titan.core.TitanKey
import com.thinkaurelius.titan.core.TitanVertex
import com.thinkaurelius.titan.graphdb.FakeVertex
import com.thinkaurelius.titan.testutil.GraphGenerator
import com.thinkaurelius.titan.testutil.JUnitBenchmarkProvider
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.frames.FramedGraph
import com.tinkerpop.gremlin.Tokens.T
import com.thinkaurelius.titan.diskstorage.StorageException


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@BenchmarkOptions(warmupRounds=1, benchmarkRounds=1)
abstract class GroovySerialTest extends GroovyTestSupport {
    
    private static final Logger LOG = LoggerFactory.getLogger(GroovySerialTest)

    @Rule public TestRule benchmark = JUnitBenchmarkProvider.get()
    
    GroovySerialTest(Configuration conf) throws StorageException {
        super(conf)
    }

    @Test
    void testVertexUidLookup() throws Exception {
        randomUidTask { tx, vertex ->  assertNotNull(vertex) }
    }
    
    /**
     * Query for edges using a vertex-centric index on a known high-out-degree vertex
     * 
     */
    @Test
    void testVertexCentricIndexQuery() {
        
        Preconditions.checkArgument(1000 <= VERTEX_COUNT)
        
        supernodeTask { v, indexLabel, indexPK ->
            
            def c = v.outE(indexLabel)
                 .has(indexPK, T.gte, 25)
                 .has(indexPK, T.lt,  75)
                 .count()
            assertEquals(50, c)
       
            c = v.outE(indexLabel)
                .has(indexPK, T.gte, 125)
                .has(indexPK, T.lt,  225)
                .count()
            assertEquals(100, c)
            
            c = v.outE(indexLabel)
                 .has(indexPK, T.gte, 500)
                 .has(indexPK, T.lt,  1000)
                 .count()
            assertEquals(500, c)
                 
            c = v.outE(indexLabel)
                 .has(indexPK, T.gt, 0)
                 .has(indexPK, T.lt, 2)
                 .count()
            assertEquals(1, c)
        }
    }
    
    @Test
    void testLabeledEdgeTraversal() {
        supernodeTask { v, indexLabel, indexPK ->
            def c = v.outE(indexLabel)[0..99].inV().outE(indexLabel).count()
            assertTrue(0 < c)
        }
    }
    
    @Test
    void testEdgeTraversalUsingVertexCentricIndex() {
        supernodeTask { v, label, pkey ->
            def c = v.outE(label)
                     .has(pkey, T.gte, 1).has(pkey, T.lte, 100)
                     .inV()
                     .outE(label)
                     .count()
            assertTrue(0 < c)
        }
    }
    
    @Test
    void testLimitedGlobalEdgePropertyQuery() {
        standardIndexEdgeTask { tx, indexedPropName, indexedPropVal ->
            int n = Iterables.size(tx.query().limit(1).has(indexedPropName, indexedPropVal).edges())
            assertTrue(0 <= n)
            assertTrue(n <= 1)
        }
    }
    
    /**
     * Retrieve all vertices with an OUT-unique standard-indexed property and limit(1).
     * 
     */
    @Test
    void testLimitedGlobalVertexPropertyQuery() {
        standardIndexVertexTask { tx, indexedPropName, indexedPropVal ->
            int n = Iterables.size(tx.query().limit(1).has(indexedPropName, indexedPropVal).vertices())
            assertTrue(0 <= n)
            assertTrue(n <= 1)
        }
    }
    
    @Test
    void testGlobalVertexPropertyQuery() {
        standardIndexVertexTask { tx, indexedPropName, indexedPropVal ->
            int n = Iterables.size(tx.query().has(indexedPropName, indexedPropVal).vertices())
            assertTrue(0 < n)
        }
    }
    
    /**
     * Retrieve all edges with a single OUT-unique standard-indexed property. No limit.
     * 
     */
    @Test
    void testGlobalEdgePropertyQuery() {
        standardIndexEdgeTask { tx, indexedPropName, indexedPropVal ->
            int n = Iterables.size(tx.query().has(indexedPropName, indexedPropVal).edges())
            assertTrue(0 < n)
        }
    }
    
//    /**
//     * Retrieve all edges matching on has(...) clause and one hasNot(...)
//     * clause, both on OUT-unique standard-indexed properties. No limit.
//     * 
//     */
//    @Test
//    void testGlobalCompositeEdgeQuery() {
//        rollbackTx({ txIndex, tx ->
//            int n = Iterables.size(tx.query().has(gen.getEdgePropertyName(0), 0).hasNot(gen.getEdgePropertyName(1), 0).edges());
//            assertTrue(0 < n);
//        })
//    }

    
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
        for (int t = 0; t < DEFAULT_TX_COUNT; t++) {
            for (int u = 0; u < 100; u++) {
                Long uid = (long)t * 100 + u;
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
    
    
    /**
     * Retrieve a vertex by randomly chosen uid, then remove the vertex. After
     * removing all vertices, add new vertices with the same uids as those
     * removed (but no incident edges or properties besides uid)
     *
     */
//    @Test
//    void testVertexRemoval() {
//
//        Set<Long> visited = new HashSet<Long>();
//        commitTx({ txIndex, tx ->
//            long uid
//            Vertex v = null
//            while (null == v) {
//                uid = Math.abs(random.nextLong()) % gen.getMaxUid();
//                TitanKey uidKey = tx.getPropertyKey(GraphGenerator.UID_PROP);
//                v = tx.getVertex(uidKey, uid);
//            }
//            assertNotNull(v)
//            tx.removeVertex(v)
//            visited.add(uid)
//        })
//
//        def tx = graph.newTransaction()
//        // Insert new vertices with the same uids as removed vertices, but no edges or properties besides uid
//        TitanKey uidKey = tx.getPropertyKey(GraphGenerator.UID_PROP)
//        for (long uid : visited) {
//            TitanVertex v = tx.addVertex()
//            v.setProperty(uidKey, uid)
//        }
//        tx.commit()
//    }
    
    @Test
    void testZVertexPropertyModification() {
        int propsModified = 0
        int visited = 0
        int n = 314159
        sequentialUidTask { tx, vertex ->
            visited++
            for (p in vertex.getPropertyKeys()) {
                if (p.equals(GraphGenerator.UID_PROP))
                    continue
                int old = vertex.getProperty(p)
                vertex.removeProperty(p)
                vertex.setProperty(p, old * n)
                n *= n
                propsModified++
                break
            }
        }
        assertEquals(DEFAULT_ITERATIONS, visited)
        assertTrue(0 < propsModified)
    }
    
    @Test
    void testZEdgeAddition() {
        int edgesAdded = 0
        int skipped = 0
        long last = -1
        sequentialUidTask { tx, vertex ->
            if (-1 != last && last == vertex.getId()) {
                Vertex target = tx.getVertex(last)
                vertex.addEdge(gen.getEdgeLabelName(0), target)
                edgesAdded++
            } else {
                skipped++
            }
            last = vertex.getId()
        }
        assertEquals(DEFAULT_ITERATIONS, edgesAdded + skipped)
        assertTrue(edgesAdded < skipped)
    }
    
    /**
     * JUnitBenchmarks appears to include {@code Before} method execution in round-avg times.
     * This method has no body and exists only to measure that overhead.
     */
    @Test
    void testNoop() {
        // Do nothing
        LOG.debug("Noop test executed");
    }
}