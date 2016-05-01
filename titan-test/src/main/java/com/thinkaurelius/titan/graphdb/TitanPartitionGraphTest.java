package com.thinkaurelius.titan.graphdb;


import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.database.idassigner.VertexIDAssigner;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.PropertyPlacementStrategy;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.olap.computer.FulgoraGraphComputer;
import com.thinkaurelius.titan.core.schema.VertexLabelMaker;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.olap.OLAPTest;
import com.thinkaurelius.titan.testcategory.OrderedKeyStoreTests;
import com.thinkaurelius.titan.testcategory.UnorderedKeyStoreTests;
import com.thinkaurelius.titan.util.datastructures.AbstractLongListUtil;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.thinkaurelius.titan.testutil.TitanAssert.*;
import static org.junit.Assert.*;

/**
 * Tests graph and vertex partitioning
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanPartitionGraphTest extends TitanGraphBaseTest {

    private static final Logger log =
            LoggerFactory.getLogger(TitanPartitionGraphTest.class);

    final static Random random = new Random();
    final static int numPartitions = 8;

    public abstract WriteConfiguration getBaseConfiguration();

    @Override
    public WriteConfiguration getConfiguration() {
        WriteConfiguration config = getBaseConfiguration();
        ModifiableConfiguration mconf = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,config, BasicConfiguration.Restriction.NONE);
        // Let GraphDatabaseConfiguration's config freezer set CLUSTER_PARTITION
        //mconf.set(GraphDatabaseConfiguration.CLUSTER_PARTITION,true);
        mconf.set(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS,numPartitions);
        //uses SimpleBulkPlacementStrategy by default
        mconf.set(SimpleBulkPlacementStrategy.CONCURRENT_PARTITIONS,3*numPartitions);
        return config;
    }

    private IDManager idManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        idManager = graph.getIDManager();
    }

//    @Test
//    @Category({ OrderedKeyStoreTests.class })
//    public void testOrderedConfig() {
//        assertTrue(graph.getConfiguration().getStoreFeatures().isKeyOrdered());
//    }
//
//    @Test
//    @Category({ UnorderedKeyStoreTests.class })
//    public void testUnorderedConfig() {
//        assertFalse(graph.getConfiguration().getStoreFeatures().isKeyOrdered());
//    }

    @Test
    public void testPartitionHashes() {
        assertEquals(8, idManager.getPartitionBound());
        Set<Long> hashs = Sets.newHashSet();
        for (long i=1;i<idManager.getPartitionBound()*2;i++) hashs.add(idManager.getPartitionHashForId(i));
        assertTrue(hashs.size()>idManager.getPartitionBound()/2);
        assertNotEquals(idManager.getPartitionHashForId(101),idManager.getPartitionHashForId(102));
    }

    @Test
    public void testVertexPartitioning() throws Exception {
        Object[] options = {option(GraphDatabaseConfiguration.IDS_FLUSH), false};
        clopen(options);

        PropertyKey gid = makeVertexIndexedUniqueKey("gid", Integer.class);
        PropertyKey sig = makeKey("sig", Integer.class);
        PropertyKey name = mgmt.makePropertyKey("name").cardinality(Cardinality.LIST).dataType(String.class).make();
        EdgeLabel knows = makeLabel("knows");
        EdgeLabel base = makeLabel("base");
        EdgeLabel one = mgmt.makeEdgeLabel("one").multiplicity(Multiplicity.ONE2ONE).make();

        VertexLabel person = mgmt.makeVertexLabel("person").make();
        VertexLabel group = mgmt.makeVertexLabel("group").partition().make();

        finishSchema();
        final Set<String> names = ImmutableSet.of("Marko", "Dan", "Stephen", "Daniel", "Josh", "Thad", "Pavel", "Matthias");
        final int numG = 10;
        final long[] gids = new long[numG];

        for (int i = 0; i < numG; i++) {
            TitanVertex g = tx.addVertex("group");
            g.property(VertexProperty.Cardinality.single, "gid", i);
            g.property(VertexProperty.Cardinality.single, "sig", 0);
            for (String n : names) {
                g.property("name", n);
            }
            assertEquals(i, g.<Integer>value("gid").intValue());
            assertEquals(0, g.<Integer>value("sig").intValue());
            assertEquals("group", g.label());
            assertCount(names.size(), g.properties("name"));
            assertTrue(getId(g) > 0);
            gids[i] = getId(g);
            if (i > 0) {
                g.addEdge("base", getV(tx, gids[0]));
            }
            if (i % 2 == 1) {
                g.addEdge("one", getV(tx, gids[i - 1]));
            }
        }

        for (int i = 0; i < numG; i++) {
            TitanVertex g = getV(tx, gids[i]);
            assertCount(1, g.query().direction(Direction.BOTH).labels("one").edges());
            assertCount(1, g.query().direction(i % 2 == 0 ? Direction.IN : Direction.OUT).labels("one").edges());
            assertCount(0, g.query().direction(i % 2 == 1 ? Direction.IN : Direction.OUT).labels("one").edges());
            if (i > 0) {
                assertCount(1, g.query().direction(Direction.OUT).labels("base").edges());
            } else {
                assertCount(numG - 1, g.query().direction(Direction.IN).labels("base").edges());
            }
        }


        newTx();

        for (int i = 0; i < numG; i++) {
            long gId = gids[i];
            assertTrue(idManager.isPartitionedVertex(gId));
            assertEquals(idManager.getCanonicalVertexId(gId), gId);
            TitanVertex g = getV(tx, gId);
            final int canonicalPartition = getPartitionID(g);
            assertEquals(g, (Vertex) getOnlyElement(tx.query().has("gid", i).vertices()));
            assertEquals(i, g.<Integer>value("gid").intValue());
            assertCount(names.size(), g.properties("name"));

            //Verify that properties are distributed correctly
            TitanVertexProperty p = (TitanVertexProperty) getOnlyElement(g.properties("gid"));
            assertEquals(canonicalPartition, getPartitionID(p));
            for (Iterator<VertexProperty<Object>> niter = g.properties("name"); niter.hasNext(); ) {
                assertEquals(canonicalPartition,getPartitionID((TitanVertex) niter.next().element()));
            }

            //Copied from above
            assertCount(1, g.query().direction(Direction.BOTH).labels("one").edges());
            assertCount(1, g.query().direction(i % 2 == 0 ? Direction.IN : Direction.OUT).labels("one").edges());
            assertCount(0, g.query().direction(i % 2 == 1 ? Direction.IN : Direction.OUT).labels("one").edges());
            if (i > 0) {
                assertCount(1, g.query().direction(Direction.OUT).labels("base").edges());
            } else {
                assertCount(numG - 1, g.query().direction(Direction.IN).labels("base").edges());
            }
        }


        clopen(options);

        final int numTx = 100;
        final int vPerTx = 10;
        Multiset<Integer> partitions = HashMultiset.create();

        for (int t = 1; t <= numTx; t++) {
            TitanVertex g1 = getV(tx, gids[0]), g2 = getV(tx, gids[1]);
            assertNotNull(g1);
            TitanVertex[] vs = new TitanVertex[vPerTx];
            for (int vi = 0; vi < vPerTx; vi++) {
                vs[vi] = tx.addVertex("person");
                vs[vi].property(VertexProperty.Cardinality.single, "sig", t);
                Edge e = vs[vi].addEdge("knows", g1);
                e.property("sig", t);
                e = g1.addEdge("knows", vs[vi]);
                e.property("sig", t);
                if (vi % 2 == 0) {
                    e = vs[vi].addEdge("knows", g2);
                    e.property("sig", t);
                }
            }
            newTx();
            //Verify that all elements are in the same partition
            TitanTransaction txx = graph.buildTransaction().readOnly().start();
            g1 = getV(tx, gids[0]);
            g2 = getV(tx, gids[1]);
            int partition = -1;
            for (int vi = 0; vi < vPerTx; vi++) {
                assertTrue(vs[vi].hasId());
                int pid = getPartitionID(vs[vi]);
                if (partition < 0) partition = pid;
                else assertEquals(partition, pid);
                int numRels = 0;
                TitanVertex v = getV(txx, vs[vi].longId());
                for (TitanRelation r : v.query().relations()) {
                    numRels++;
                    assertEquals(partition, getPartitionID(r));
                    if (r instanceof TitanEdge) {
                        TitanVertex o = ((TitanEdge) r).otherVertex(v);
                        assertTrue(o.equals(g1) || o.equals(g2));
                    }
                }
                assertEquals(3 + (vi % 2 == 0 ? 1 : 0), numRels);
            }
            partitions.add(partition);
            txx.commit();
        }
        //Verify spread across partitions; this number is a pessimistic lower bound but might fail since it is probabilistic
        assertTrue(partitions.elementSet().size() >= 3); //

        newTx();
        //Verify edge querying across partitions
        TitanVertex g1 = getV(tx, gids[0]);
        assertEquals(0, g1.<Integer>value("gid").intValue());
        assertEquals("group", g1.label());
        assertCount(names.size(), g1.properties("name"));
        assertCount(numTx * vPerTx, g1.query().direction(Direction.OUT).labels("knows").edges());
        assertCount(numTx * vPerTx, g1.query().direction(Direction.IN).labels("knows").edges());
        assertCount(numTx * vPerTx * 2, g1.query().direction(Direction.BOTH).labels("knows").edges());
        assertCount(numTx * vPerTx + numG, tx.query().vertices());

        newTx();
        //Restrict to partitions
        for (int t = 0; t < 10; t++) {
            int numP = random.nextInt(3) + 1;
            Set<Integer> parts = Sets.newHashSet();
            int numV = 0;
            while (parts.size() < numP) {
                int part = Iterables.get(partitions.elementSet(), random.nextInt(partitions.elementSet().size()));
                if (parts.add(part)) numV += partitions.count(part);
            }
            numV *= vPerTx;
            int[] partarr = new int[numP];
            int i = 0;
            for (Integer part : parts) partarr[i++] = part;
            TitanTransaction tx2 = graph.buildTransaction().restrictedPartitions(partarr).readOnly().start();
            //Copied from above
            g1 = getV(tx2, gids[0]);
            assertEquals(0, g1.<Integer>value("gid").intValue());
            assertEquals("group", g1.label());
            assertTrue(names.size() >= size(g1.properties("name")));
            assertCount(numV, g1.query().direction(Direction.OUT).labels("knows").edges());
            assertCount(numV, g1.query().direction(Direction.IN).labels("knows").edges());
            assertCount(numV * 2, g1.query().direction(Direction.BOTH).labels("knows").edges());

            //Test local intersection
            TitanVertex g2 = getV(tx2, gids[1]);
            VertexList v1 = g1.query().direction(Direction.IN).labels("knows").vertexIds();
            VertexList v2 = g2.query().direction(Direction.IN).labels("knows").vertexIds();
            assertEquals(numV, v1.size());
            assertEquals(numV / 2, v2.size());
            v1.sort();
            v2.sort();
            LongArrayList al1 = v1.getIDs();
            LongArrayList al2 = v2.getIDs();
            assertTrue(AbstractLongListUtil.isSorted(al1));
            assertTrue(AbstractLongListUtil.isSorted(al2));
            LongArrayList alr = AbstractLongListUtil.mergeJoin(al1, al2, false);
            assertEquals(numV / 2, alr.size());

            tx2.commit();
        }
    }

    private enum CommitMode { BATCH, PER_VERTEX, PER_CLUSTER }

    private int setupGroupClusters(int[] groupDegrees, CommitMode commitMode) {
        mgmt.makeVertexLabel("person").make();
        mgmt.makeVertexLabel("group").partition().make();
        makeVertexIndexedKey("groupid", String.class);
        makeKey("name", String.class);
        makeKey("clusterId",String.class);
        makeLabel("member");
        makeLabel("contain");

        finishSchema();

        int numVertices = 0;
        TitanVertex[] groups = new TitanVertex[groupDegrees.length];
        for (int i = 0; i < groupDegrees.length; i++) {
            groups[i]=tx.addVertex("group");
            groups[i].property("groupid","group"+i);
            numVertices++;
            if (commitMode==CommitMode.PER_VERTEX) newTx();
            for (int noEdges = 0; noEdges < groupDegrees[i]; noEdges++) {
                TitanVertex g = vInTx(groups[i],tx);
                TitanVertex p = tx.addVertex("name","person"+i+":"+noEdges,"clusterId","group"+i);
                numVertices++;
                p.addEdge("member",g);
                g.addEdge("contain", p);
                if (commitMode==CommitMode.PER_VERTEX) newTx();
            }
            if (commitMode==CommitMode.PER_CLUSTER) newTx();
        }
        newTx();
        return numVertices;
    }

    private static TitanVertex vInTx(TitanVertex v, TitanTransaction tx) {
        if (!v.hasId()) return v;
        else return tx.getVertex(v.longId());
    }

    @Test
    public void testPartitionSpreadFlushBatch() {
        testPartitionSpread(true,true);
    }

    @Test
    public void testPartitionSpreadFlushNoBatch() {
        testPartitionSpread(true,false);
    }

    @Test
    public void testPartitionSpreadNoFlushBatch() {
        testPartitionSpread(false,true);
    }

    @Test
    public void testPartitionSpreadNoFlushNoBatch() {
        testPartitionSpread(false,false);
    }

    private void testPartitionSpread(boolean flush, boolean batchCommit) {
        Object[] options = {option(GraphDatabaseConfiguration.IDS_FLUSH), flush};
        clopen(options);

        int[] groupDegrees = {10,15,10,17,10,4,7,20,11};
        int numVertices = setupGroupClusters(groupDegrees,batchCommit?CommitMode.BATCH:CommitMode.PER_VERTEX);

        IntSet partitionIds = new IntHashSet(numVertices); //to track the "spread" of partition ids
        for (int i=0;i<groupDegrees.length;i++) {
            TitanVertex g = getOnlyVertex(tx.query().has("groupid","group"+i));
            assertCount(groupDegrees[i],g.edges(Direction.OUT,"contain"));
            assertCount(groupDegrees[i],g.edges(Direction.IN,"member"));
            assertCount(groupDegrees[i],g.query().direction(Direction.OUT).edges());
            assertCount(groupDegrees[i],g.query().direction(Direction.IN).edges());
            assertCount(groupDegrees[i]*2,g.query().edges());
            for (TitanVertex v : g.query().direction(Direction.IN).labels("member").vertices()) {
                int pid = getPartitionID(v);
                partitionIds.add(pid);
                assertEquals(g, getOnlyElement(v.query().direction(Direction.OUT).labels("member").vertices()));
                VertexList vlist = v.query().direction(Direction.IN).labels("contain").vertexIds();
                assertEquals(1,vlist.size());
                assertEquals(pid,idManager.getPartitionId(vlist.getID(0)));
                assertEquals(g,vlist.get(0));
            }
        }
        if (flush || !batchCommit) { //In these cases we would expect significant spread across partitions
            assertTrue(partitionIds.size()>numPartitions/2); //This is a probabilistic test that might fail
        } else {
            assertEquals(1,partitionIds.size()); //No spread in this case
        }
    }

    @Test
    public void testVertexPartitionOlapBatch() throws Exception {
        testVertexPartitionOlap(CommitMode.BATCH);
    }

    @Test
    public void testVertexPartitionOlapCluster() throws Exception {
        testVertexPartitionOlap(CommitMode.PER_CLUSTER);
    }

    @Test
    public void testVertexPartitionOlapIndividual() throws Exception {
        testVertexPartitionOlap(CommitMode.PER_VERTEX);
    }

    private void testVertexPartitionOlap(CommitMode commitMode) throws Exception {
        Object[] options = {option(GraphDatabaseConfiguration.IDS_FLUSH), false};
        clopen(options);

//        int[] groupDegrees = {10,20,30};
        int[] groupDegrees = {2};

        int numVertices = setupGroupClusters(groupDegrees,commitMode);

        Map<Long,Integer> degreeMap = new HashMap<>(groupDegrees.length);
        for (int i = 0; i < groupDegrees.length; i++) {
            degreeMap.put(getOnlyVertex(tx.query().has("groupid","group"+i)).longId(),groupDegrees[i]);
        }

        clopen(options);

        //Test OLAP works with partitioned vertices
        FulgoraGraphComputer computer = graph.compute(FulgoraGraphComputer.class);
        computer.resultMode(FulgoraGraphComputer.ResultMode.NONE);
        computer.workers(1);
        computer.program(new OLAPTest.DegreeCounter());
        computer.mapReduce(new OLAPTest.DegreeMapper());
        ComputerResult result = computer.submit().get();

        assertTrue(result.memory().exists(OLAPTest.DegreeMapper.DEGREE_RESULT));
        Map<Long,Integer> degrees = result.memory().get(OLAPTest.DegreeMapper.DEGREE_RESULT);
        assertNotNull(degrees);
        assertEquals(numVertices,degrees.size());
        final IDManager idManager = graph.getIDManager();

        for (Map.Entry<Long,Integer> entry : degrees.entrySet()) {
            long vid = entry.getKey();
            Integer degree = entry.getValue();
            if (idManager.isPartitionedVertex(vid)) {
//                System.out.println("Partitioned: " + degree );
                assertEquals(degreeMap.get(vid),degree);
            } else {
                assertEquals(1, (long) degree);
            }
        }
    }

    @Test
    public void testVLabelOnOrderedStorage() {
        final String label = "pl";
        mgmt.makeVertexLabel(label).partition().make();
        mgmt.commit();
        graph.tx().rollback();
        graph.addVertex(label);
        graph.tx().commit();
        mgmt = graph.openManagement();
        VertexLabel vl = mgmt.getVertexLabel(label);
        assertTrue(vl.isPartitioned());
        mgmt.rollback();
    }

    @Test
    public void testKeybasedGraphPartitioning() {
        Object[] options = {option(GraphDatabaseConfiguration.IDS_FLUSH), false,
                            option(VertexIDAssigner.PLACEMENT_STRATEGY), PropertyPlacementStrategy.class.getName(),
                option(PropertyPlacementStrategy.PARTITION_KEY), "clusterId"};
        clopen(options);

        int[] groupDegrees = {5,5,5,5,5,5,5,5};
        int numVertices = setupGroupClusters(groupDegrees,CommitMode.PER_VERTEX);

        IntSet partitionIds = new IntHashSet(numVertices); //to track the "spread" of partition ids
        for (int i=0;i<groupDegrees.length;i++) {
            TitanVertex g = getOnlyVertex(tx.query().has("groupid","group"+i));
            int partitionId = -1;
            for (TitanVertex v : g.query().direction(Direction.IN).labels("member").vertices()) {
                if (partitionId<0) partitionId = getPartitionID(v);
                assertEquals(partitionId,getPartitionID(v));
                partitionIds.add(partitionId);
            }
        }
        assertTrue(partitionIds.size()>numPartitions/2); //This is a probabilistic test that might fail
    }


    public int getPartitionID(TitanVertex vertex) {
        long p = idManager.getPartitionId(vertex.longId());
        assertTrue(p>=0 && p<idManager.getPartitionBound() && p<Integer.MAX_VALUE);
        return (int)p;
    }

    public int getPartitionID(TitanRelation relation) {
        long p = relation.longId() & (idManager.getPartitionBound()-1);
        assertTrue(p>=0 && p<idManager.getPartitionBound() && p<Integer.MAX_VALUE);
        return (int)p;
    }

}
