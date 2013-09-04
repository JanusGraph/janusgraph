package com.thinkaurelius.titan.graphdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.testutil.RandomGenerator;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.frames.FramedGraph;
import com.tinkerpop.frames.Property;

//@BenchmarkOptions(warmupRounds=0, benchmarkRounds=3)
public class TitanGraphSerialTest extends TitanGraphTestCommon {
    
//    @Rule
//    public TestRule benchmark = JUnitBenchmarkProvider.get();
    
//    private final int threadPoolSize;
//    private final int runnableCount;
//    private final ExecutorService executor;
    
    private static final int VERTEX_COUNT = 10 * 1000;
    private static final int EDGE_COUNT = VERTEX_COUNT * 10;
    private static final int PROP_KEY_COUNT = 20;
    private static final int EDGE_LABEL_COUNT = 10;
    private static final String DEFAULT_EDGE_LABEL = "label_0";
    private static final Logger log = LoggerFactory.getLogger(TitanGraphSerialTest.class);

    public TitanGraphSerialTest(Configuration config, int threadPoolSize, int runnableCount) throws StorageException {
        super(config);
//        this.threadPoolSize = threadPoolSize;
//        this.runnableCount = runnableCount;
//        this.executor = Executors.newFixedThreadPool(threadPoolSize);
        
        GraphDatabaseConfiguration graphconfig = new GraphDatabaseConfiguration(config);
        graphconfig.getBackend().clearStorage();
        open();
        generateBAGraph();
        close();
    }
    
    @Override
    @Before
    public void setUp() {
        open();
    }
    
    public TitanGraphSerialTest(Configuration config) throws StorageException {
        this(config, 4, 40);
    }
//    
//    @BeforeClass
//    public static void generateGraph() throws Exception {
//        createTypes();
//        generateBAGraph();
//    }
    
    // Deleted testMultipleDatabases() since its runtime had no significance
    
    /**
     * Retrieve 100 vertices, each by its exact uid. Repeat the process with
     * different uids in 50 transactions. The transactions are read-only and are
     * all rolled back rather than committed.
     */
    @Test
    public void testUidLookup() throws Exception {
        for (int t = 0; t < 50; t++) {
            newTx();
            TitanKey uidKey = tx.getPropertyKey("uid");
            for (int u = 0; u < 100; u++) {
                String uid = String.valueOf(t * 100 + u);
                TitanVertex v = tx.getVertex(uidKey, uid);
                assertEquals(uid, v.getProperty(uidKey));
            }
            tx.rollback();
            tx = null;
        }
    }

    /**
     * Same as {@link #testUidLookup}, except add or modify a singlex property
     * on every vertex retrieved and commit the changes in each transaction
     * instead of rolling back.
     */
    @Test
    public void testUidLookupAndPropertyModification() {
        Random random = new Random(7);
        
        for (int t = 0; t < 50; t++) {
            newTx();
            TitanKey uidKey = tx.getPropertyKey("uid");
            for (int u = 0; u < 100; u++) {
                String uid = String.valueOf(t * 100 + u);
                TitanVertex v = tx.getVertex(uidKey, uid);
                assertEquals(uid, v.getProperty(uidKey));
                Set<String> props = ImmutableSet.copyOf(v.getPropertyKeys());
                String propKeyToModify = "prop_" + random.nextInt(PROP_KEY_COUNT);
                if (props.contains(propKeyToModify)) {
                    v.removeProperty(propKeyToModify);
                    v.addProperty(propKeyToModify, "updated:" + uid);
                }
            }
        }
    }
    
    /**
     * Retrieve vertices by uid, then retrieve their associated properties. All
     * access is done through a FramedGraph interface. This is inspired by part
     * of the ONLAB benchmark, but written separately ("from scratch").
     */
    @Test
    public void testUidAndPropertyLookup() {
        FramedGraph<TitanGraph> fg = new FramedGraph<TitanGraph>(graph);
        int totalNonNullProps = 0;
        for (int t = 0; t < 50; t++) {
            for (int u = 0; u < 100; u++) {
                String uid = String.valueOf(t * 100 + u);
                Iterable<FakeVertex> iter = fg.getVertices("uid", uid, FakeVertex.class);
                for (FakeVertex fv : iter) {
                    assertEquals(uid, fv.getUid());
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
                }
            }
        }
        // The chance of this going to zero during random scale-free graph
        // generation (for a graph of non-trivial size) is insignificant.
        assertTrue(0 < totalNonNullProps);
    }

    @Test
    public void testInTxIndex() throws Exception {
        int trials = 2; int numV = 2000; int offset = 10000;
        newTx();

        for (int t=0;t<trials;t++) {
            for (int i=offset;i<offset+numV;i++) {
                if (Iterables.isEmpty(tx.getVertices("uid",String.valueOf(i) + "-foo"))) {
                    TitanVertex v = tx.addVertex();
                    v.addProperty("uid", String.valueOf(i + "-foo"));
                }
            }
        }
        assertEquals(numV + VERTEX_COUNT,Iterables.size(tx.getVertices()));
    }
    
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
     * {@link #EDGE_LABEL_COUNT} edge labels.
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
        for (int i = 0; i < PROP_KEY_COUNT; i++) {
            tx.makeType().name("prop_" + i).dataType(String.class).indexed(Vertex.class).unique(Direction.OUT).makePropertyKey();
        }
        // Create edge labels
        for (int i = 0; i < EDGE_LABEL_COUNT; i++) {
            tx.makeType().name("label_" + i).makeEdgeLabel();
        }
        newTx();

        generateBASeedVertices(edges);

        // Load property key objects in current tx
        TitanKey props[] = new TitanKey[PROP_KEY_COUNT];
        for (int i = 0; i < PROP_KEY_COUNT; i++) {
            props[i] = tx.getPropertyKey("prop_" + i);
        }
        
        // Load edge label objects in current tx
        TitanLabel labels[] = new TitanLabel[EDGE_LABEL_COUNT];
        for (int i = 0; i < EDGE_LABEL_COUNT; i++) {
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
                int propIndex = random.nextInt(PROP_KEY_COUNT);
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
                int labelIndex = random.nextInt(EDGE_LABEL_COUNT);
                
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
        public String getUid();
        
        @Property("prop_0")
        public String getProp0();
        
        @Property("prop_1")
        public String getProp1();

        @Property("prop_2")
        public String getProp2();
    }
}
