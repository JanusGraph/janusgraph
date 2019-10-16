// Copyright 2019 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb

import org.janusgraph.TestCategory
import org.janusgraph.diskstorage.configuration.WriteConfiguration
import org.janusgraph.graphdb.util.ElementHelper
import org.junit.jupiter.api.Tag

import static org.junit.Assert.*

import org.junit.FixMethodOrder
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runners.MethodSorters
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.carrotsearch.junitbenchmarks.BenchmarkOptions
import com.google.common.base.Preconditions
import com.google.common.collect.Iterables
import org.janusgraph.core.JanusGraphEdge
import org.janusgraph.core.JanusGraphTransaction
import org.janusgraph.core.JanusGraphVertex
import org.janusgraph.testutil.JUnitBenchmarkProvider
import org.janusgraph.diskstorage.BackendException


/**
 * This class was formerly known as GroovySerialTest.
 * Several issues and commitlogs refer to it that way.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@BenchmarkOptions(warmupRounds = 1, benchmarkRounds = 1)
@Tag(TestCategory.PERFORMANCE_TESTS)
abstract class JanusGraphSpeedTest extends GroovySpeedTestSupport {

    private static final Logger log = LoggerFactory.getLogger(JanusGraphSpeedTest)

    @Rule
    public TestRule benchmark = JUnitBenchmarkProvider.get()

    JanusGraphSpeedTest(WriteConfiguration conf) throws BackendException {
        super(conf)
    }

    /*
     * Summary of graph schema and data
     * 
     * 
     * - 10k vertices
     * - 50k edges
     * - 3 edge labels
     * - 10 out-unique property keys with standard index for edges
     * - 20 out-unique property keys with standard index for vertices
     * - one both-unique, standard-indexed vertex property keycalled "uid"
     * 
     * The edges are all directed.  Each (edge label, edge direction) pair
     * forms a scale-free graph.  There is one exception to this rule: after
     * generating scale free distributions, the generator adds one more
     * vertex with an outgoing edge to every other vertex in the graph.
     * Its permanent ID is returned by schema.getSupernodeUid().  All its
     * outgoing edges have the same label.  The label is returned by
     * schema.getSupernodeOutLabel().
     * 
     * The "uid" property uniquely identifies each vertex globally and
     * exists to allow standard index lookups and iteration.  The minimum
     * value is 0 (the supernode).  The maximum value is
     * schema.getMaxUid() - 1 -- that is, getMaxUid() is exclusive.
     * 
     * The labels are all primary keyed.  Their names are returned by
     * schema.getEdgeLabelName(n), where n is on the interval
     * [0, schema.getEdgeLabels()).  The names of the primary keys are
     * returned by schema.getPrimaryKeyForLabel(String).
     * 
     * Each vertex or edge has property values set on a randomly-sized
     * subset of the keys available for vertices or edges, respectively.
     * 
     * All properties except uid are integer-valued and have values
     * on these intervals:
     * 
     * - vertices: [0, schema.getMaxVertexPropVal())
     * - edges:    [0, schema.getMaxEdgePropVal())
     * 
     * The values were generated with a random distribution.
     * 
     * In each case above, n is a numerical index corresponding to a key.
     * The index n takes these values:
     * 
     * - vertices: [0, schema.getVertexPropKeys())
     * - edges:    [0, schema.getEdgePropKeys())
     * 
     * The same n indexes retrieve the property key names when passed
     * to these methods:
     * 
     * - vertices: schema.getVertexPropertyName(n)
     * - edges:    schema.getEdgePropertyName(n)
     * 
     * The graph was generated and written to a file with
     * c.t.janusgraph.testutil.gen.{Schema, GraphGenerator}.
     */

    @Test
    void testVertexUidLookup() throws Exception {
        sequentialUidTask { tx, vertex -> assertNotNull(vertex) }
    }

    /**
     * Query for edges using a vertex-centric index on a fixed supernode.
     *
     */
    @Test
    void testVertexCentricIndexQuery() {

        final long maxUid = 1000L // exclusive
        final long minUid = 1L    // inclusive

        Preconditions.checkArgument(maxUid - minUid <= VERTEX_COUNT)

        supernodeTask { v, indexLabel, indexPK ->

            def c = v.outE(indexLabel)
                    .has(indexPK, T.gte, 25)
                    .has(indexPK, T.lt, 75)
                    .count()
            assertEquals(50, c)

            c = v.outE(indexLabel)
                    .has(indexPK, T.gte, 125)
                    .has(indexPK, T.lt, 225)
                    .count()
            assertEquals(100, c)

            c = v.outE(indexLabel)
                    .has(indexPK, T.gte, 500)
                    .has(indexPK, T.lt, 1000)
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
        int i = 0
        supernodeTask { v, indexLabel, indexPK ->
            int start = 100 * i++
            int end = start + 99
            def c = v.outE(indexLabel)[start..end].inV().outE(indexLabel).inV().outE(indexLabel).count()
            assertTrue(0 < c)
        }
    }

    @Test
    void testEdgeTraversalUsingVertexCentricIndex() {
        supernodeTask { v, label, pkey ->
            def c = v.outE(label)
                    .has(pkey, T.gte, 0).has(pkey, T.lte, 100)
                    .inV()
                    .outE(label)
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

    @Test
    void testGlobalEdgePropertyQuery() {
        standardIndexEdgeTask { tx, indexedPropName, indexedPropVal ->
            int n = Iterables.size(tx.query().has(indexedPropName, indexedPropVal).edges())
            assertTrue(0 < n)
        }
    }

    @Test
    void testMultiVertexQuery() {
        chunkedSequentialUidTask(50, 50, this.&multiVertexQueryTask)
    }

    @Test
    void testPathologicalMultiVertexQuery() {
        chunkedSequentialUidTask(1, 50, this.&multiVertexQueryTask)
    }

    @Test
    void testSingleVertexQuery() {
        sequentialUidTask(50, this.&singleVertexQueryTask)
    }

    @Test
    void testSingleVertexMultiProperty() {
        sequentialUidTask(50, { tx, v ->
            int c = 0
            for (int i = 0; i < schema.getVertexPropKeys(); i++) {
                if (v.valueOrNull(schema.getVertexPropertyName(i)) != null) {
                    c++
                }
            }
            def k = ElementHelper.getPropertyKeys(v) - 1
            assertTrue(k + "vs" + c, k <= c)
            assertTrue(0 <= c)
        })
    }

    @Test
    void testSingleVertexProperty() {
        sequentialUidTask(50, { tx, v ->
            assertNotNull(v.valueOrNull(Schema.UID_PROP))
        })
    }

    /*
    * I'm prefixing test methods that modify the graph with "testZ". In
    * conjunction with JUnit's @FixMethodOrder annotation, this makes
    * graph-mutating test methods run after the rest of the test methods.
    */

    @Test
    void testZVertexPropertyModification() {
        int propsModified = 0
        int visited = 0
        int n = 314159
        sequentialUidTask { tx, vertex ->
            visited++
            for (p in ElementHelper.getPropertyKeys(v)) {
                if (p.equals(Schema.UID_PROP)) {
                    continue
                }
                int old = vertex.valueOrNull(p)
                vertex.property(p).remove()
                vertex.property(p, old * n)
                n *= n
                propsModified++
                break
            }
        }
        assertTrue(0 < propsModified)
    }

    @Test
    void testZEdgeAddition() {
        int edgesAdded = 0
        int skipped = 0
        long last = -1
        String labelName = schema.getEdgeLabelName(0)
        sequentialUidTask { tx, vertex ->
            if (-1 != last && last != vertex.id()) {
                JanusGraphVertex target = tx.v(last)
                vertex.addEdge(labelName, target)
                edgesAdded++
            } else {
                skipped++
            }
            last = vertex.id()
        }
        assertTrue(0 < edgesAdded + skipped)
        assertTrue(edgesAdded > skipped)
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
//                JanusGraphKey uidKey = tx.getPropertyKey(Schema.UID_PROP);
//                v = tx.getVertex(uidKey, uid);
//            }
//            assertNotNull(v)
//            tx.removeVertex(v)
//            visited.add(uid)
//        })
//
//        def tx = graph.newTransaction()
//        // Insert new vertices with the same uids as removed vertices, but no edges or properties besides uid
//        JanusGraphKey uidKey = tx.getPropertyKey(Schema.UID_PROP)
//        for (long uid : visited) {
//            JanusGraphVertex v = tx.addVertex()
//            v.setProperty(uidKey, uid)
//        }
//        tx.commit()
//    }

    /**
     * JUnitBenchmarks appears to include {@code Before} method execution in round-avg times.
     * This method has no body and exists only to measure that overhead.
     */
    @Test
    static void testNoop() {
        // Do nothing
        log.debug("Noop test executed")
    }

    private void multiVertexQueryTask(JanusGraphTransaction tx, JanusGraphVertex[] vbuf, int vcount) {
        if (vcount != vbuf.length) {
            def newbuf = new JanusGraphVertex[vcount]
            for (int i = 0; i < vcount; i++) {
                newbuf[i] = vbuf[i]
                Preconditions.checkArgument(null != newbuf[i])
            }
            vbuf = newbuf
        }

        // I tried labels(schema.edgeLabelNames), but it causes a
        // Preconditions failure because Query.isQueryNormalForm returns false
        int n = 0
        for (int i = 0; i < schema.edgeLabels; i++) {
            Map<JanusGraphVertex, Iterable<JanusGraphEdge>> m = tx.multiQuery(vbuf).labels(schema.edgeLabelNames[i]).edges()
            for (Iterable<JanusGraphEdge> iter : m.values()) {
                for (JanusGraphEdge e : iter) {
                    n++
                }
            }
        }
        assertTrue(0 < n)
    }

    private void singleVertexQueryTask(JanusGraphTransaction tx, JanusGraphVertex v) {
        int n = 0
        for (int i = 0; i < schema.edgeLabels; i++) {
            for (JanusGraphEdge iter : v.query().labels(schema.edgeLabelNames[i]).edges()) {
                n++
            }
        }
        assertTrue(0 < n)
    }
}
