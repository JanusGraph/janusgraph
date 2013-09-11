package com.thinkaurelius.titan.graphdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.testutil.GraphGenerator;
import com.thinkaurelius.titan.testutil.JUnitBenchmarkProvider;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.frames.FramedGraph;
import com.tinkerpop.frames.Property;

@BenchmarkOptions(warmupRounds=1, benchmarkRounds=1)
public abstract class TitanGraphSerialTest {
    
    @Rule
    public TestRule benchmark = JUnitBenchmarkProvider.get();
    
    protected static final int VERTEX_COUNT = 10 * 1000;
    protected static final int EDGE_COUNT = VERTEX_COUNT * 10;
    protected static final int VERTEX_PROP_COUNT = 20;
    protected static final int EDGE_PROP_COUNT = 10;
    
    private static final String DEFAULT_EDGE_LABEL = "el_0";
    private static final int TX_COUNT = 50;
    private static final int OPS_PER_TX = 100;
    
    private final Random random = new Random(7); // Arbitrary seed; no special significance except that it remains constant between comparable runs
    
    protected GraphGenerator gen;
    private TitanTransaction tx;
    private StandardTitanGraph graph;
    protected Configuration conf;
    
    private static final Logger log = LoggerFactory.getLogger(TitanGraphSerialTest.class);


    public TitanGraphSerialTest(Configuration conf) throws StorageException {
        this.conf = conf;
    }
    
    public void open() {
        if (null == graph)
            try {
                graph = getGraph();
            } catch (StorageException e) {
                throw new RuntimeException(e);
            }
        if (null == gen)
            gen = getGenerator();
        tx = graph.newTransaction();
    }

    protected abstract StandardTitanGraph getGraph() throws StorageException;
    protected abstract GraphGenerator getGenerator();
    
    public void close() {
        if (null != tx && tx.isOpen())
            tx.commit();

        if (null != graph)
            graph.shutdown();
    }

    public void newTx() {
        if (null != tx && tx.isOpen())
            tx.commit();
        
        tx = graph.newTransaction();
    }
    
    @Before
    public void setUp() throws Exception {
        open();
    }
    
    /**
     * Retrieve 100 vertices, each by its exact uid. Repeat the process with
     * different uids in 50 transactions. The transactions are read-only and are
     * all rolled back rather than committed.
     * 
     */
    @Test
    public void testVertexUidLookup() throws Exception {
        for (int t = 0; t < TX_COUNT; t++) {
            newTx();
            TitanKey uidKey = tx.getPropertyKey(GraphGenerator.UID_PROP);
            for (int u = 0; u < OPS_PER_TX; u++) {
                long uid = t * OPS_PER_TX + u;
                TitanVertex v = tx.getVertex(uidKey, uid);
                assertNotNull(v);
                assertEquals(uid, v.getProperty(uidKey));
            }
            tx.rollback();
            tx = null;
        }
    }

    /**
     * Same as {@link #testVertexUidLookup}, except add or modify a single property
     * on every vertex retrieved and commit the changes in each transaction
     * instead of rolling back.
     * 
     */
    @Test
    public void testVertexPropertyModification() {
        
        for (int t = 0; t < TX_COUNT; t++) {
            newTx();
            TitanKey uidKey = tx.getPropertyKey(GraphGenerator.UID_PROP);
            for (int u = 0; u < OPS_PER_TX; u++) {
                long uid = t * OPS_PER_TX + u;
                TitanVertex v = tx.getVertex(uidKey, uid);
                assertNotNull(v);
                assertEquals(uid, v.getProperty(uidKey));
                Set<String> props = ImmutableSet.copyOf(v.getPropertyKeys());
                String propKeyToModify = gen.getVertexPropertyName(random.nextInt(VERTEX_PROP_COUNT));
                if (props.contains(propKeyToModify)) {
                    v.removeProperty(propKeyToModify);
                    v.setProperty(propKeyToModify, random.nextInt(GraphGenerator.MAX_VERTEX_PROP_VALUE));
                }
            }
            tx.commit();
            tx = null;
        }
    }

    /**
     * Retrieve a vertex by randomly chosen uid, then delete its first edge.
     * 
     */
    @Test
    public void testEdgeRemoval() {
        int deleted = 0;
        for (int t = 0; t < TX_COUNT; t++) {
            newTx();
            TitanKey uidKey = tx.getPropertyKey(GraphGenerator.UID_PROP);
            for (int u = 0; u < OPS_PER_TX; u++) {
                long uid = Math.abs(random.nextLong()) % gen.getMaxUid();
                TitanVertex v = tx.getVertex(uidKey, uid);
                assertNotNull(v);
                Iterable<TitanEdge> edges = v.getEdges();
                assertNotNull(edges);
                TitanEdge e = Iterables.getFirst(edges, null);
                if (null == e) {
                    u--;
                    continue;
                }
                e.remove();
                deleted++;
            }
            tx.commit();
            tx = null;
        }
        assertEquals(TX_COUNT * OPS_PER_TX, deleted);
    }

    /**
     * Retrieve a vertex by randomly chosen uid, then remove the vertex. After
     * removing all vertices, add new vertices with the same uids as those
     * removed (but no incident edges or properties besides uid)
     * 
     */
    @Test
    public void testVertexRemoval() {
        Set<Long> visited = new HashSet<Long>(TX_COUNT);
        for (int t = 0; t < TX_COUNT * 10; t++) {
            newTx();
            long uid = Math.abs(random.nextLong()) % gen.getMaxUid();
            TitanKey uidKey = tx.getPropertyKey(GraphGenerator.UID_PROP);
            TitanVertex v = tx.getVertex(uidKey, uid);
            if (null == v) {
                t--;
                continue;
            }
            assertNotNull(v);
            tx.removeVertex(v);
            visited.add(uid);
            tx.commit();
            tx = null;
        }
        
        newTx();
        // Insert new vertices with the same uids as removed vertices, but no edges or properties besides uid
        TitanKey uidKey = tx.getPropertyKey(GraphGenerator.UID_PROP);
        for (long uid : visited) {
            TitanVertex v = tx.addVertex();
            v.setProperty(uidKey, uid);
        }
        tx.commit();
    }
    
    @Test
    public void testNoop() {
        // Do nothing
    }
    
    /**
     * Same query as in {@link #testEdgePropertyQuery()}, except with limit(1).
     * 
     */
    @Test
    public void testLimitedEdgeQuery() {
        for (int t = 0; t < TX_COUNT; t++) {
            newTx();
            for (int u = 0; u < OPS_PER_TX; u++) {
                int n = Iterables.size(tx.query().limit(1).has(gen.getEdgePropertyName(0), 0).edges());
                assertTrue(0 <= n);
                assertTrue(n <= 1);
            }
        }
    }
    
    /**
     * Retrieve all vertices with an OUT-unique standard-indexed property and limit(1).
     * 
     */
    @Test
    public void testLimitedVertexQuery() {
        for (int t = 0; t < TX_COUNT; t++) {
            newTx();
            for (int u = 0; u < OPS_PER_TX; u++) {
                int n = Iterables.size(tx.query().limit(1).has(gen.getVertexPropertyName(0), 0).vertices());
                assertTrue(0 <= n);
                assertTrue(n <= 1);
            }
        }
    }
    
    /**
     * Retrieve all vertices with uid equal to a randomly chosen value. Note
     * that uid is standard-indexed and BOTH-unique, so this query should return
     * one vertex in practice, but no limit is specified.
     * 
     */
    @Test
    public void testVertexPropertyQuery() {
        for (int t = 0; t < TX_COUNT; t++) {
            newTx();
            for (int u = 0; u < OPS_PER_TX; u++) {
                int n = Iterables.size(tx.query().has("uid", Math.abs(random.nextLong()) % gen.getMaxUid()).vertices());
                assertTrue(1 == n);
            }
        }
    }
    
    /**
     * Retrieve all edges with a single OUT-unique standard-indexed property. No limit.
     * 
     */
    @Test
    public void testEdgePropertyQuery() {
        for (int t = 0; t < TX_COUNT; t++) {
            newTx();
            int n = Iterables.size(tx.query().has(gen.getEdgePropertyName(0), 0).edges());
            assertTrue(0 < n);
        }
    }
    
    /**
     * Retrieve all edges matching on has(...) clause and one hasNot(...)
     * clause, both on OUT-unique standard-indexed properties. No limit.
     * 
     */
    @Test
    public void testHasAndHasNotEdgeQuery() {
        for (int t = 0; t < TX_COUNT; t++) {
            newTx();
            int n = Iterables.size(tx.query().has(gen.getEdgePropertyName(0), 0).hasNot(gen.getEdgePropertyName(1), 0).edges());
            assertTrue(0 < n);
        }
    }
    
    /**
     * Retrieve all vertices matching on has(...) clause and one hasNot(...)
     * clause, both on OUT-unique standard-indexed properties. No limit.
     * 
     */
    @Test
    public void testHasAndHasNotVertexQuery() {
        for (int t = 0; t < TX_COUNT; t++) {
            newTx();
            for (int u = 0; u < OPS_PER_TX; u++) {
                int n = Iterables.size(tx.query().has(gen.getVertexPropertyName(0), 0).hasNot(gen.getVertexPropertyName(1), 0).vertices());
                assertTrue(0 < n);
            }
        }
    }
    
    /**
     * Retrieve vertices by uid, then retrieve their associated properties. All
     * access is done through a FramedGraph interface. This is inspired by part
     * of the ONLAB benchmark, but written separately ("from scratch").
     * 
     */
    @Test
    public void testFramedUidAndPropertyLookup() {
        FramedGraph<TitanGraph> fg = new FramedGraph<TitanGraph>(graph);
        int totalNonNullProps = 0;
        for (int t = 0; t < TX_COUNT; t++) {
            for (int u = 0; u < OPS_PER_TX; u++) {
                Long uid = (long)t * OPS_PER_TX + u;
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
     * Repeatedly call {@code tx.getEdges(TitanKey, Integer)}, closing and
     * reopening the enclosing transaction every OPS_PER_TX calls. The TitanKey
     * is randomly chosen from the set of keys created by the
     * {@code GraphGenerator}. The int value is randomly chosen from [0,
     * GraphGenerator.MAX_EDGE_PROP_VALUE). We count the returned edges, but
     * assert only that the count is greater than zero.
     */
//    @Test
//    public void testEdgePropertyLookup() {
//        for (int t = 0; t < TX_COUNT; t++) {
//            newTx();
//            TitanKey key = tx.getPropertyKey(gen.getEdgePropertyName(random.nextInt(EDGE_PROP_COUNT)));
//            for (int u = 0; u < 1; u++) {
//                int value = random.nextInt(GraphGenerator.MAX_EDGE_PROP_VALUE);
//                Iterable<TitanEdge> edges = tx.getEdges(key, value);
//                assertNotNull(edges);
//                int n = Iterables.size(edges);
//                assertTrue(0 < n);
//            }
//            tx.rollback();
//            tx = null;
//        }
//    }
    /* This test has a bug or exposes a bug.  It causes
     * java.lang.IllegalArgumentException: expected one element but was: <e[2BGf-2Sc-5g][11048-el_0->11048], e[2BGf-2Sc-5g][11048-el_0->11048]>
     * at com.google.common.collect.Iterators.getOnlyElement(Iterators.java:340)
     * ...
     * at com.thinkaurelius.titan.graphdb.TitanGraphSerialTest.testEdgePropertyModification(TitanGraphSerialTest.java:155)
     * ...
     */
//    @Test
//    public void testEdgePropertyModification() {
//        for (int t = 0; t < TX_COUNT; t++) {
//            newTx();
//            TitanKey key = tx.getPropertyKey(gen.getEdgePropertyName(random.nextInt(EDGE_PROP_COUNT)));
//            int n = 0;
//            for (int u = 0; u < OPS_PER_TX; u++) {
//                int value = random.nextInt(GraphGenerator.MAX_EDGE_PROP_VALUE);
//                Iterable<TitanEdge> edges = tx.getEdges(key, value);
//                assertNotNull(edges);
//                for (TitanEdge e : edges) {
//                    if (null == e)
//                        continue;
//                    Integer oldValue = e.getProperty(key);
//                    if (null == oldValue)
//                        continue;
//                    int newValue;
//                    do {
//                        newValue = random.nextInt(GraphGenerator.MAX_EDGE_PROP_VALUE);
//                    } while (oldValue == newValue);
//                    assertTrue(oldValue != newValue);
//                    e.removeProperty(key);
////                    e.setProperty(key, newValue);
//                    n++;
//                }
//            }
//
//            assertTrue(0 < n);
//            tx.commit();
//            tx = null;
//        }
//    }
    

//    @Test
//    public void testInTxIndex() throws Exception {
//        int trials = 2; int numV = 2000; int offset = 10000;
//        newTx();
//
//        for (int t=0;t<trials;t++) {
//            for (int i=offset;i<offset+numV;i++) {
//                if (Iterables.isEmpty(tx.getVertices("uid",String.valueOf(i) + "-foo"))) {
//                    TitanVertex v = tx.addVertex();
//                    v.addProperty("uid", String.valueOf(i + "-foo"));
//                }
//            }
//        }
//        assertEquals(numV + VERTEX_COUNT,Iterables.size(tx.getVertices()));
//    }
    
//    @Test
//    public void testEdgeInsertion() throws Exception {
//        
//        final int noNodes = 50 * 1000;
//        final int noEdgesPerNode = 10;
//        
//        TitanKey weight = tx.makeType().name("weight").
//                unique(Direction.OUT).
//                dataType(Double.class).
//                makePropertyKey();
//        TitanKey uid = tx.getPropertyKey("uid");
//        TitanLabel knows = tx.makeType().name("knows").
//                primaryKey(uid).signature(weight).directed().makeEdgeLabel();
//        TitanKey name = tx.makeType().name("name").unique(Direction.OUT)
//                .indexed(Vertex.class).dataType(String.class).makePropertyKey();
//        
//        newTx();
//        weight = tx.getPropertyKey("weight");
//        uid = tx.getPropertyKey("uid");
//        knows = tx.getEdgeLabel("knows");
//        name = tx.getPropertyKey("name");
//
//        String[] names = new String[noNodes];
//        TitanVertex[] nodes = new TitanVertex[noNodes];
//        for (int i = 0; i < noNodes; i++) {
//            names[i]="Node"+i;
//            nodes[i] = tx.addVertex();
//            nodes[i].addProperty(name, names[i]);
//            nodes[i].addProperty(uid, String.valueOf("ei:" + i));
//        }
//        log.info("Nodes loaded");
//        int offsets[] = {-99, -71, -20, -17, -13, 2, 7, 15, 33, 89};
//        assert offsets.length == noEdgesPerNode;
//
//        for (int i = 0; i < noNodes; i++) {
//            TitanVertex n = nodes[i];
//            for (int e = 0; e < noEdgesPerNode; e++) {
//                TitanVertex n2 = nodes[wrapAround(i + offsets[e], noNodes)];
//                TitanEdge r = n.addEdge(knows, n2);
//                r.setProperty(uid, RandomGenerator.randomInt(0, Integer.MAX_VALUE));
//                r.setProperty(weight, Math.random());
//            }
//            if ((i + 1) % 10000 == 0)
//                log.debug("Nodes connected: " + (i + 1));
//        }
//        
//        tx.commit();
//        clopen();
//        
//        //Verify that data was written
//        TitanVertex v1 = (TitanVertex) Iterables.getOnlyElement(tx.getVertices("uid", "ei:50"));
//        TitanVertex v2 = (TitanVertex) Iterables.getOnlyElement(tx.getVertices("uid", "ei:150"));
//        assertTrue(v1.query().count() > 0);
//        assertEquals(v1.query().count(), v2.query().count());
//    }
    
    /**
     * Generate a scale-free graph using the Barabasi-Albert algorithm. The
     * gamma of the resulting graph should be about 2.9. This algorithm is
     * technically random, but we use a random number generator with an
     * arbitrary constant seed (7) to make its output reproducible.
     * <p>
     * Each vertex has one both-unique standard-indexed string property called
     * uid. It also has between 0 and 5 additional standard-indexed, out-unique
     * String properties chosen from a total of 20 property keys named prop_0
     * through _19.
     * <p>
     * The values of both uid and the prop_0 ... prop_19 keys are the same for
     * any given vertex: the decimal string representing the serial number in
     * which the vertex was created.
     * <p>
     * Edges are also chosen in a uniform random distribution over
     * {@link #EDGE_PROP_COUNT} edge labels.
     * <p>
     * This implementation holds all edges in memory, limiting the size of the
     * generated graph.
     */
    private void generateBAGraph() {
        final int m0 = 9; // Initial/seed vertex count
        final int m  = 3; // Number of edges to add in each step
        final int initialEdgeCount = 10;
        
        Preconditions.checkArgument(m <= m0);
        Preconditions.checkArgument(VERTEX_COUNT > m0);
        
        int curEdges = initialEdgeCount;
        TitanEdge edges[] = new TitanEdge[EDGE_COUNT];
        
        Random random = new Random(7);
        
        newTx();
        
        // Create uid property key
        tx.makeType().name("uid").dataType(String.class).indexed(Vertex.class).unique(Direction.BOTH).makePropertyKey();
        // Create vertex property keys
        for (int i = 0; i < VERTEX_PROP_COUNT; i++) {
            tx.makeType().name("prop_" + i).dataType(String.class).indexed(Vertex.class).unique(Direction.OUT).makePropertyKey();
        }
        // Create edge labels
        for (int i = 0; i < EDGE_PROP_COUNT; i++) {
            tx.makeType().name("label_" + i).makeEdgeLabel();
        }
        newTx();

        generateBASeedVertices(edges);

        // Load property key objects in current tx
        TitanKey props[] = new TitanKey[VERTEX_PROP_COUNT];
        for (int i = 0; i < VERTEX_PROP_COUNT; i++) {
            props[i] = tx.getPropertyKey("prop_" + i);
        }
        
        // Load edge label objects in current tx
        TitanLabel labels[] = new TitanLabel[EDGE_PROP_COUNT];
        for (int i = 0; i < EDGE_PROP_COUNT; i++) {
            labels[i] = tx.getEdgeLabel("label_" + i);
        }
        
        for (int i = 0; i < VERTEX_COUNT - m0; i++) {
            // Create a vertex
            TitanVertex newVertex = tx.addVertex();
            // Set its uid
            String uidString = String.valueOf(i);
            newVertex.addProperty("uid", uidString);
            // Set its bonus properties
            int pCount = random.nextInt(6);
            for (int p = 0; p < pCount; p++) {
                int propIndex = random.nextInt(VERTEX_PROP_COUNT);
                newVertex.setProperty(props[propIndex], uidString); 
            }
            
            // Create edges
            for (int e = 0; e < m; e++) {
                /*
                 * Choose an edge from the current graph with uniform
                 * probability, then randomly choose one of its incident
                 * vertices without regard to edge direction. I took this
                 * technique from Yoo & Henderson's Parallel Barabasi-Albert
                 * algorithm.
                 */
                TitanEdge sample = edges[random.nextInt(curEdges)];
                TitanVertex nodeToConnect;
                
                // Randomly choose an edge label
                int labelIndex = random.nextInt(EDGE_PROP_COUNT);
                
                // Randomly choose one of two vertices on the edge with equal
                // probability (directional blindness)
                if (random.nextBoolean()) {
                    nodeToConnect = sample.getVertex(Direction.IN);
                    edges[curEdges++] = newVertex.addEdge(labels[labelIndex], nodeToConnect);
                } else {
                    nodeToConnect = sample.getVertex(Direction.OUT);
                    edges[curEdges++] = newVertex.addEdge(labels[labelIndex], nodeToConnect);
                }
                
                if (0 == curEdges % 1000) {
                    log.info("Loaded {} edges", curEdges);
                }
            }
        }
    }
    
    /**
     * Generate a hierarchical, 9-node, 10-edge graph as a seed for
     * Barabasi-Albert. The structure is fixed/deterministic. It has a gamma of
     * about 1.6.
     * 
     * @param edges
     *            A non-null array of edges of at least size 10. The first ten
     *            elements will be overwritten with edges created between
     *            vertices created by this method.
     */
    private void generateBASeedVertices(TitanEdge[] edges) {
        TitanLabel l = tx.getEdgeLabel(DEFAULT_EDGE_LABEL);
        TitanVertex root = tx.addVertex();
        
        int ei = 0;
        
        TitanVertex rootsChildren[] = new TitanVertex[6];
        for (int i = 0; i < 6; i++) {
            rootsChildren[i] = tx.addVertex();
            edges[ei++] = tx.addEdge(rootsChildren[i], root, l);
        }
        
        int child = 0;
        TitanVertex leftInterior  = tx.addVertex();
        edges[ei++] = tx.addEdge(rootsChildren[child++], leftInterior, l);
        edges[ei++] = tx.addEdge(rootsChildren[child++], leftInterior, l);
        
        TitanVertex rightInterior = tx.addVertex();
        edges[ei++] = tx.addEdge(rootsChildren[child++], rightInterior, l);
        edges[ei++] = tx.addEdge(rootsChildren[child++], rightInterior, l);
        
        // Don't connect [5] or [6] to anything besides root
    }
    
    /**
     * Generate a random graph. This is not scale-free. Edges in the graph have
     * a uniform probability of incidence on any vertex in the graph.
     */
    private void generateRandomGraph() {
        TitanLabel defaultLabel = tx.getEdgeLabel(DEFAULT_EDGE_LABEL);
        newTx();
        
        Random random = new Random(7L);
        TitanVertex vertices[] = new TitanVertex[VERTEX_COUNT];
        for (int i = 0; i < VERTEX_COUNT; i++) {
            vertices[i] = tx.addVertex();
        }
        for (int i = 0; i < EDGE_COUNT; i++) {
            int r1 = random.nextInt(VERTEX_COUNT);
            int r2 = random.nextInt(VERTEX_COUNT);
            if (r1 == r2) {
                r2 *= 10;
                r2 %= VERTEX_COUNT;
                Preconditions.checkArgument(r1 != r2);
            }
            vertices[r1].addEdge(defaultLabel, vertices[r2]);
        }
        newTx();
    }
    
    private static interface FakeVertex {
        @Property("uid")
        public Long getUid();
        
        @Property("vp_0")
        public Integer getProp0();
        
        @Property("vp_1")
        public Integer getProp1();

        @Property("vp_2")
        public Integer getProp2();
    }
}
