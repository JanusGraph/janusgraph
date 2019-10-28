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

import org.janusgraph.diskstorage.configuration.WriteConfiguration
import org.janusgraph.graphdb.query.QueryUtil
import org.apache.tinkerpop.gremlin.util.Gremlin
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.google.common.base.Preconditions
import org.janusgraph.core.JanusGraphVertex
import org.janusgraph.core.JanusGraph
import org.janusgraph.graphdb.database.StandardJanusGraph
import org.janusgraph.diskstorage.BackendException

import com.google.common.collect.Iterables

import java.util.zip.GZIPInputStream

abstract class GroovySpeedTestSupport {

    private static final Logger log = LoggerFactory.getLogger(GroovySpeedTestSupport)

    // Graph generation settings
    public static final int VERTEX_COUNT = SpeedTestSchema.VERTEX_COUNT
    public static final int EDGE_COUNT = SpeedTestSchema.EDGE_COUNT

    // Query execution setting defaults
    public static final int DEFAULT_TX_COUNT = 3
    public static final int DEFAULT_VERTICES_PER_TX = 100
    public static final int DEFAULT_ITERATIONS = DEFAULT_TX_COUNT * DEFAULT_VERTICES_PER_TX

    public static final String RELATION_FILE = "../janusgraph-test/data/v10k.graphml.gz"

    // Mutable state

    /*  JUnit constructs a new test class instance before executing each test method. 
     * Ergo, each test method gets its own Random instance. 
     * The seed is arbitrary and carries no special significance,
     * but we keep the see fixed for repeatability.
     */
    protected Random random = new Random(7)
    protected SpeedTestSchema schema
    protected JanusGraph graph
    protected WriteConfiguration conf

    static {
        Gremlin.load()
    }

    GroovySpeedTestSupport(WriteConfiguration conf) throws BackendException {
        this.conf = conf
    }

    @BeforeEach
    void open() {
//        Preconditions.checkArgument(TX_COUNT * DEFAULT_OPS_PER_TX <= VERTEX_COUNT);

        if (null == graph) {
            try {
                graph = getGraph()
            } catch (BackendException e) {
                throw new RuntimeException(e)
            }
        }
        if (null == schema) {
            schema = getSchema()
        }
    }

    @AfterEach
    void rollback() {
        if (null != graph)
            graph.rollback()
    }

    void close() {
        if (null != graph)
            graph.shutdown()
    }

    protected abstract StandardJanusGraph getGraph() throws BackendException

    protected abstract SpeedTestSchema getSchema()

    /*
     * Helper methods
     */

    protected void sequentialUidTask(int verticesPerTx = DEFAULT_VERTICES_PER_TX, closure) {
        chunkedSequentialUidTask(1, verticesPerTx, { tx, vbuf, vloaded ->
            assert 1 == vloaded
            assert 1 == vbuf.length
            def v = vbuf[0]
            closure.call(tx, v)
        })
    }

    protected void chunkedSequentialUidTask(int chunksize = DEFAULT_VERTICES_PER_TX, int verticesPerTx = DEFAULT_VERTICES_PER_TX, closure) {

        /*
         * Need this condition because of how we handle transactions and buffer
         * Vertex objects.  If this divisibility constraint were violated, then
         * we would end up passing Vertex instances from one or more committed
         * transactions as if those instances were not stale.
         */
        Preconditions.checkArgument(0 == verticesPerTx % chunksize)

        long count = DEFAULT_TX_COUNT * verticesPerTx
        long offset = Math.abs(random.nextLong()) % schema.getMaxUid()
        def uids = new SequentialLongIterator(count, schema.getMaxUid(), offset)
        def tx = graph.newTransaction()
        JanusGraphVertex[] vbuf = new JanusGraphVertex[chunksize]
        int vloaded = 0

        while (uids.hasNext()) {
            long u = uids.next()
            JanusGraphVertex v = Iterables.getOnlyElement(QueryUtil.getVertices(tx,Schema.UID_PROP, u))
            assertNotNull(v)
            vbuf[vloaded++] = v
            if (vloaded == chunksize) {
                closure.call(tx, vbuf, vloaded)
                vloaded = 0
                tx.commit()
                tx = graph.newTransaction()
            }
        }

        if (0 < vloaded) {
            closure.call(tx, vbuf, vloaded)
            tx.commit()
        } else {
            tx.rollback()
        }
    }

    protected void supernodeTask(closure) {
        long uid = schema.getSupernodeUid()
        String label = schema.getSupernodeOutLabel()
        assertNotNull(label)
        String pkey = schema.getSortKeyForLabel(label)
        assertNotNull(pkey)

        def tx = graph.newTransaction()
        def v = Iterables.getOnlyElement(QueryUtil.getVertices(tx,Schema.UID_PROP, uid))
//            def v = graph.V(Schema.UID_PROP, uid).next()
        assertNotNull(v)
        closure(v, label, pkey)
        tx.commit()
    }

    protected void standardIndexEdgeTask(closure) {
        final int keyCount = schema.getEdgePropKeys()

        def tx = graph.newTransaction()
        int value = -1
        for (int p = 0; p < schema.getEdgePropKeys(); p++) {
            for (int i = 0; i < 5; i++) {
                if (++value >= schema.getMaxEdgePropVal())
                    value = 0
                closure(tx, schema.getEdgePropertyName(p), value)
            }
        }
        tx.commit()
    }

    protected void standardIndexVertexTask(closure) {
        final int keyCount = schema.getVertexPropKeys()

        def tx = graph.newTransaction()
        int value = -1
        for (int p = 0; p < schema.getVertexPropKeys(); p++) {
            for (int i = 0; i < 5; i++) {
                if (++value >= schema.getMaxVertexPropVal()) {
                    value = 0
                }
                closure(tx, schema.getVertexPropertyName(p), value)
            }

        }
        tx.commit()
    }

    protected void initializeGraph(JanusGraph g) throws BackendException {
        log.info("Initializing graph...")
        long before = System.currentTimeMillis()
        SpeedTestSchema schema = getSchema()

        try {
            InputStream data = new GZIPInputStream(new FileInputStream(RELATION_FILE))
            schema.makeTypes(g)
            GraphMLReader.inputGraph(g, data)
        } catch (IOException e) {
            throw new RuntimeException(e)
        }
        long after = System.currentTimeMillis()
        long duration = after - before
        if (15 * 1000 <= duration) {
            log.warn("Initialized graph (" + duration + " ms).")
        } else {
            log.info("Initialized graph (" + duration + " ms).")
        }
    }
}
