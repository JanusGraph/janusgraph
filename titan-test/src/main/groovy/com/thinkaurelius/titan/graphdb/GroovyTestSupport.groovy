package com.thinkaurelius.titan.graphdb

import static org.junit.Assert.*

import org.apache.commons.configuration.Configuration
import org.junit.After
import org.junit.Before
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.google.common.base.Preconditions
import com.tinkerpop.blueprints.Vertex
import com.thinkaurelius.titan.core.TitanVertex
import com.thinkaurelius.titan.core.TitanGraph
import com.thinkaurelius.titan.diskstorage.StorageException
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph
import com.thinkaurelius.titan.testutil.JUnitBenchmarkProvider;
import com.thinkaurelius.titan.testutil.gen.Schema
import com.thinkaurelius.titan.testutil.gen.GraphGenerator
import com.tinkerpop.gremlin.groovy.Gremlin
import com.thinkaurelius.titan.diskstorage.StorageException

import java.util.zip.GZIPInputStream
import java.io.IOException
import java.io.FileInputStream
import java.util.HashMap

abstract class GroovyTestSupport {
    
    private static final Logger log = LoggerFactory.getLogger(GroovyTestSupport)
    
    @Rule public TestName testName = new TestName()
    
    // Graph generation settings
    public static final int VERTEX_COUNT = 10 * 100
    public static final int EDGE_COUNT = VERTEX_COUNT * 5
    
    // Query execution setting defaults
    public static final int DEFAULT_TX_COUNT = 3
    public static final int DEFAULT_OPS_PER_TX = 100
    public static final int DEFAULT_ITERATIONS = DEFAULT_TX_COUNT * DEFAULT_OPS_PER_TX
    
    public static final String RELATION_FILE = "../titan-test/data/v10k.graphml.gz"
    
    // Mutable state

    /*  JUnit constructs a new test class instance before executing each test method. 
     * Ergo, each test method gets its own Random instance. 
     * The seed is arbitrary and carries no special significance,
     * but we keep the see fixed for repeatability.
     */
    protected Random random = new Random(7) 
    protected GraphGenerator gen
    protected Schema schema
    protected TitanGraph graph
    protected Configuration conf
    private static final Map<String, Double> iterationScalars = new HashMap<String, Double>()

    static {
        Gremlin.load()
        
        // Load iteration scalars from a file
        iterationScalars.clear()
        iterationScalars.putAll(JUnitBenchmarkProvider.loadScalarsFromEnvironment())
    }
    
    GroovyTestSupport(Configuration conf) throws StorageException {
        this.conf = conf
    }    
    
    @Before
    void open() {
//        Preconditions.checkArgument(TX_COUNT * DEFAULT_OPS_PER_TX <= VERTEX_COUNT);
        
        if (null == graph) {
            try {
                graph = getGraph()
            } catch (StorageException e) {
                throw new RuntimeException(e)
            }
        }
        if (null == schema) {
            schema = getSchema()
        }
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
    
    protected void sequentialUidTask(closure) {
        long scale  = getDoubleScalar()
        long count  = Math.round(scale * DEFAULT_TX_COUNT * DEFAULT_OPS_PER_TX)
        long offset = Math.abs(random.nextLong()) % schema.getMaxUid()
        def uids    = new SequentialLongIterator(count, schema.getMaxUid(), offset)
        int op      = 0
        def tx      = graph.newTransaction()
        while (uids.hasNext()) {
            long u = uids.next()
            Vertex v = tx.getVertex(Schema.UID_PROP, u)
            assertNotNull(v)
            closure.call(tx, v)
            if (DEFAULT_OPS_PER_TX <= ++op) {
                op = 0
                tx.commit()
                tx = graph.newTransaction()
            }
        }
        
        0 < op ? tx.commit() : tx.rollback()
    }
    
    protected void supernodeTask(closure) {
        long uid = schema.getSupernodeUid()
        String label = schema.getSupernodeOutLabel()
        assertNotNull(label)
        String pkey  = schema.getPrimaryKeyForLabel(label)
        assertNotNull(pkey)
        final int n = getIntScalar()
        
        for (int i = 0; i < n; i++) {
            def tx = graph.newTransaction()
            def v = tx.getVertex(Schema.UID_PROP, uid)
//            def v = graph.V(Schema.UID_PROP, uid).next()
            assertNotNull(v)
            closure(v, label, pkey)
            tx.commit()
        }
    }
    
    protected void standardIndexEdgeTask(closure) {
        final int keyCount = schema.getEdgePropKeys()
        final int n = keyCount * getIntScalar()
        
        for (int i = 0; i < n; i++) {
            def tx = graph.newTransaction()
            closure(tx, schema.getEdgePropertyName(i % keyCount), 0)
            tx.commit()
        }
    }
    
    protected void standardIndexVertexTask(closure) {
        final int keyCount = schema.getVertexPropKeys()
        final int n = keyCount * getIntScalar()
        
        for (int i = 0; i < n; i++) {
            def tx = graph.newTransaction()
            closure(tx, schema.getVertexPropertyName(i % keyCount), 0)
            tx.commit()
        }
    }
    
    protected void initializeGraph(TitanGraph g) throws StorageException {
        log.info("Initializing graph...");
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
            log.warn("Initialized graph (" + duration + " ms).")
        } else {
            log.info("Initialized graph (" + duration + " ms).")
        }
    }
    
    private int getIntScalar() {
        Math.ceil(getDoubleScalar())
    }
    
    private double getDoubleScalar() {
        Preconditions.checkNotNull(testName)
        final String n = this.getClass().getCanonicalName() + "." + testName.getMethodName()
        def s = iterationScalars.get(n)
        if (null == s) {
            log.warn("No iteration scalar found for test method {}, defaulting to 1", n)
            s = 1
        } else {
            log.debug("Retrieved iteration scalar {} for method {}", s, n);
        }
        s
    }
    
}