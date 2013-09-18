package com.thinkaurelius.titan.graphdb

import static org.junit.Assert.*

import org.apache.commons.configuration.Configuration
import org.junit.After
import org.junit.Before
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.google.common.base.Preconditions
import com.tinkerpop.blueprints.Vertex
import com.thinkaurelius.titan.core.TitanVertex
import com.thinkaurelius.titan.core.TitanGraph
import com.thinkaurelius.titan.diskstorage.StorageException
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph
import com.thinkaurelius.titan.testutil.GraphGenerator
import com.tinkerpop.gremlin.groovy.Gremlin
import com.thinkaurelius.titan.diskstorage.StorageException

abstract class GroovyTestSupport {
    
    private static final Logger LOG = LoggerFactory.getLogger(GroovyTestSupport)
    
    // Graph generation settings
    public static final int VERTEX_COUNT = 10 * 1000
    public static final int EDGE_COUNT = VERTEX_COUNT * 5
    
    // Query execution setting defaults
    public static final int DEFAULT_TX_COUNT = 3
    public static final int DEFAULT_OPS_PER_TX = 100
    public static final int DEFAULT_ITERATIONS = DEFAULT_TX_COUNT * DEFAULT_OPS_PER_TX
    
    // Mutable state
    protected Random random = new Random(7) // Arbitrary seed
    protected GraphGenerator gen
    protected TitanGraph graph
    protected Configuration conf

    static {
        Gremlin.load()
    }
    
    GroovyTestSupport(Configuration conf) throws StorageException {
        this.conf = conf;
    }    
    
    @Before
    void open() {
//        Preconditions.checkArgument(TX_COUNT * opsPerTx <= VERTEX_COUNT);
        
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

    /*
     * Helper methods
     */
    
    
    
    protected void sequentialUidTask(int txCount = DEFAULT_TX_COUNT, int opsPerTx = DEFAULT_OPS_PER_TX, closure) {
        def uids = new SequentialLongIterator(txCount * opsPerTx)
        multiVertexTask(uids, txCount, opsPerTx, closure)
    }
    
    protected void randomUidTask(int txCount = DEFAULT_TX_COUNT, int opsPerTx = DEFAULT_OPS_PER_TX, closure) {
        def uids = new RandomLongIterator(txCount * opsPerTx, gen.getMaxUid(), random)
        multiVertexTask(uids, txCount, opsPerTx, closure)
    }

    protected void supernodeTask(closure) {
        
        long uid = gen.getHighDegVertexUid()
        String label = gen.getHighDegEdgeLabel()
        assertNotNull(label)
        String pkey  = gen.getPrimaryKeyForLabel(label)
        assertNotNull(pkey)
        def v = graph.V(GraphGenerator.UID_PROP, uid).next()
        assertNotNull(v)
        
        closure(v, label, pkey)
    }
    
    protected void multiVertexTask(LongIterator uids, int txCount = DEFAULT_TX_COUNT, int opsPerTx = DEFAULT_OPS_PER_TX, closure) {
        Preconditions.checkNotNull(uids)
        
        int op = 0
        def tx = graph.newTransaction()
        
        while (uids.hasNext()) {
            long u = uids.next()
            Vertex v = tx.getVertex(GraphGenerator.UID_PROP, u)
            assertNotNull(v)
            closure.call(tx, v)
            if (opsPerTx <= ++op) {
                op = 0
                tx.commit()
                tx = graph.newTransaction()
            }
        }
        
        0 < op ? tx.commit() : tx.rollback()
    }
    
    
    protected void standardIndexEdgeTask(closure) {
        final int keyCount = gen.getEdgePropKeys()
        
        for (int i = 0; i < keyCount; i++) {
            def tx = graph.newTransaction()
            closure(tx, gen.getEdgePropertyName(i), 0)
            tx.commit()
        }
    }
    
    protected void standardIndexVertexTask(closure) {
        final int keyCount = gen.getVertexPropKeys()
        
        for (int i = 0; i < keyCount; i++) {
            def tx = graph.newTransaction()
            closure(tx, gen.getVertexPropertyName(i), 0)
            tx.commit()
        }
    }
    
    protected void rollbackTx(closure) {
        doTx(closure, { tx -> tx.rollback() })
    }
    
    protected void commitTx(closure) {
        doTx(closure, { tx -> tx.commit() })
    }

    protected void doTx(txWork, afterWork) {
        def tx
        for (int t = 0; t < DEFAULT_TX_COUNT; t++) {
            tx = graph.newTransaction()
            txWork.call(t, tx)
            afterWork.call(tx)
        }
    }
    
    protected void noTx(closure) {
        for (int t = 0; t < DEFAULT_TX_COUNT; t++) {
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