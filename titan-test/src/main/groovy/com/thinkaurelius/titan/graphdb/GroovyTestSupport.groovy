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
import com.thinkaurelius.titan.testutil.gen.Schema
import com.thinkaurelius.titan.testutil.gen.GraphGenerator
import com.tinkerpop.gremlin.groovy.Gremlin
import com.thinkaurelius.titan.diskstorage.StorageException

import java.util.zip.GZIPInputStream
import java.io.IOException
import java.io.FileInputStream

abstract class GroovyTestSupport {
    
    private static final Logger LOG = LoggerFactory.getLogger(GroovyTestSupport)
    
    // Graph generation settings
    public static final int VERTEX_COUNT = 10 * 100
    public static final int EDGE_COUNT = VERTEX_COUNT * 5
    
    // Query execution setting defaults
    public static final int DEFAULT_TX_COUNT = 3
    public static final int DEFAULT_OPS_PER_TX = 100
    public static final int DEFAULT_ITERATIONS = DEFAULT_TX_COUNT * DEFAULT_OPS_PER_TX
    
    public static final String RELATION_FILE = "data/v10k.graphml.gz"
    
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
        if (null == schema)
            schema = getSchema()
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
    protected abstract Schema getSchema();

    /*
     * Helper methods
     */
    
    protected void sequentialUidTask(int txCount = DEFAULT_TX_COUNT, int opsPerTx = DEFAULT_OPS_PER_TX, closure) {
        def uids = new SequentialLongIterator(txCount * opsPerTx)
        multiVertexTask(uids, txCount, opsPerTx, closure)
    }
    
    protected void randomUidTask(int txCount = DEFAULT_TX_COUNT, int opsPerTx = DEFAULT_OPS_PER_TX, closure) {
        def uids = new RandomLongIterator(txCount * opsPerTx, schema.getMaxUid(), random)
        multiVertexTask(uids, txCount, opsPerTx, closure)
    }

    protected void supernodeTask(int repeat = 1, closure) {
        for (int i = 0; i < repeat; i++) {
            long uid = schema.getSupernodeUid()
            String label = schema.getSupernodeOutLabel()
            assertNotNull(label)
            String pkey  = schema.getPrimaryKeyForLabel(label)
            assertNotNull(pkey)
            def v = graph.V(Schema.UID_PROP, uid).next()
            assertNotNull(v)
            
            closure(v, label, pkey)
        }
    }
    
    protected void multiVertexTask(LongIterator uids, int txCount = DEFAULT_TX_COUNT, int opsPerTx = DEFAULT_OPS_PER_TX, closure) {
        Preconditions.checkNotNull(uids)
        
        int op = 0
        def tx = graph.newTransaction()
        
        while (uids.hasNext()) {
            long u = uids.next()
            Vertex v = tx.getVertex(Schema.UID_PROP, u)
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
    
    
    protected void standardIndexEdgeTask(int repeat = 1, closure) {
        final int keyCount = schema.getEdgePropKeys()
        
        for (int i = 0; i < keyCount * repeat; i++) {
            def tx = graph.newTransaction()
            closure(tx, schema.getEdgePropertyName(i % keyCount), 0)
            tx.commit()
        }
    }
    
    protected void standardIndexVertexTask(int repeat = 1, closure) {
        final int keyCount = schema.getVertexPropKeys()
        
        for (int i = 0; i < keyCount * repeat; i++) {
            def tx = graph.newTransaction()
            closure(tx, schema.getVertexPropertyName(i % keyCount), 0)
            tx.commit()
        }
    }
    
    protected void initializeGraph(TitanGraph g) throws StorageException {
        LOG.info("Initializing graph...");
        long before = System.currentTimeMillis()
        Schema schema = getSchema();
        GraphGenerator generator = new GraphGenerator(schema);
        GraphDatabaseConfiguration graphconfig = new GraphDatabaseConfiguration(conf);
        graphconfig.getBackend().clearStorage();
//        generator.generate(g);
        try {
            generator.generateTypesAndLoadData(g, new GZIPInputStream(new FileInputStream(RELATION_FILE)))
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long after = System.currentTimeMillis()
        long duration = after - before
        if (15 * 1000 <= duration) {
            LOG.warn("Initialized graph (" + duration + " ms).")
        } else {
            LOG.info("Initialized graph (" + duration + " ms).")
        }
    }
}