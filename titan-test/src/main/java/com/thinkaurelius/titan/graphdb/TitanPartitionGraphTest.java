package com.thinkaurelius.titan.graphdb;


import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.olap.OLAPJobBuilder;
import com.thinkaurelius.titan.core.olap.OLAPResult;
import com.thinkaurelius.titan.core.schema.VertexLabelMaker;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy;
import com.thinkaurelius.titan.graphdb.fulgora.FulgoraBuilder;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.olap.OLAPTest;
import com.thinkaurelius.titan.testcategory.OrderedKeyStoreTests;
import com.thinkaurelius.titan.testcategory.UnorderedKeyStoreTests;
import com.thinkaurelius.titan.util.datastructures.AbstractLongListUtil;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.Set;

import static com.thinkaurelius.titan.testutil.TitanAssert.assertCount;
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

    protected <S> OLAPJobBuilder<S> getOLAPBuilder(StandardTitanGraph graph, Class<S> clazz) {
        return new FulgoraBuilder<S>(graph);
    }


    @Override
    public WriteConfiguration getConfiguration() {
        WriteConfiguration config = getBaseConfiguration();
        ModifiableConfiguration mconf = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,config, BasicConfiguration.Restriction.NONE);
        // Let GraphDatabaseConfiguration's config freezer set CLUSTER_PARTITION
        //mconf.set(GraphDatabaseConfiguration.CLUSTER_PARTITION,true);
        mconf.set(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS,numPartitions);
        //uses SimpleBulkPlacementStrategy by default
        mconf.set(SimpleBulkPlacementStrategy.CONCURRENT_PARTITIONS,16);
        return config;
    }

    @Test
    @Category({ OrderedKeyStoreTests.class })
    public void testOrderedConfig() {
        assertTrue(graph.getConfiguration().isClusterPartitioned());
    }

    @Test
    @Category({ UnorderedKeyStoreTests.class })
    public void testUnorderedConfig() {
        assertFalse(graph.getConfiguration().isClusterPartitioned());
    }

    @Test
    @Category({ OrderedKeyStoreTests.class })
    public void testSetup() {
        final IDManager idManager = graph.getIDManager();
        assertEquals(8, idManager.getPartitionBound());
        Set<Long> hashs = Sets.newHashSet();
        for (long i=1;i<idManager.getPartitionBound()*2;i++) hashs.add(idManager.getPartitionHashForId(i));
        assertTrue(hashs.size()>idManager.getPartitionBound()/2);
        assertNotEquals(idManager.getPartitionHashForId(101),idManager.getPartitionHashForId(102));
    }

    @Test
    @Category({ OrderedKeyStoreTests.class })
    public void testVertexPartitioning() throws Exception {
        Object[] options = {option(GraphDatabaseConfiguration.IDS_FLUSH),false};
        clopen(options);

        PropertyKey gid = makeVertexIndexedUniqueKey("gid",Integer.class);
        PropertyKey sig = makeKey("sig",Integer.class);
        PropertyKey name = mgmt.makePropertyKey("name").cardinality(Cardinality.LIST).dataType(String.class).make();
        EdgeLabel knows = makeLabel("knows");
        EdgeLabel base = makeLabel("base");
        EdgeLabel one = mgmt.makeEdgeLabel("one").multiplicity(Multiplicity.ONE2ONE).make();

        VertexLabel person = mgmt.makeVertexLabel("person").make();
        VertexLabel group = mgmt.makeVertexLabel("group").partition().make();

        finishSchema();
        final IDManager idManager = graph.getIDManager();
        final Set<String> names = ImmutableSet.of("Marko","Dan","Stephen","Daniel","Josh","Thad","Pavel","Matthias");
        final int numG = 10;
        final long[] gids = new long[numG];

        for (int i = 0; i < numG; i++) {
            Vertex g = tx.addVertex("group");
            g.singleProperty("gid", i);
            g.singleProperty("sig", 0);
            for (String n : names) {
                g.property("name", n);
            }
            assertEquals(i,g.<Integer>value("gid").intValue());
            assertEquals(0,g.<Integer>value("sid").intValue());
            assertEquals("group",g.label());
            assertCount(names.size(), g.properties("name"));
            assertTrue(getId(g)>0);
            gids[i]=getId(g);
            if (i>0) {
                g.addEdge("base",tx.v(gids[0]));
            }
            if (i%2==1) {
                g.addEdge("one",tx.v(gids[i-1]));
            }
        }

        for (int i = 0; i < numG; i++) {
            Vertex g = tx.v(gids[i]);
            assertCount(1,g.bothE("one"));
            assertCount(1, g.toE(i % 2 == 0 ? Direction.IN : Direction.OUT, "one"));
            assertCount(0, g.toE(i % 2 == 1 ? Direction.IN : Direction.OUT, "one"));
            if (i>0) {
                assertCount(1,g.outE("base"));
            } else {
                assertCount(numG-1,g.inE("base"));
            }
        }


        newTx();

        for (int i = 0; i < numG; i++) {
            long gId = gids[i];
            assertTrue(idManager.isPartitionedVertex(gId));
            assertEquals(idManager.getCanonicalVertexId(gId),gId);
            TitanVertex g = tx.v(gId);
            final int canonicalPartition = getPartitionID(g,idManager);
            assertEquals(g,(Vertex)getOnlyElement(tx.V().has("gid",i)));
            assertEquals(i,g.<Integer>value("gid").intValue());
            assertCount(names.size(),g.properties("name"));

            //Verify that properties are distributed correctly
            TitanVertexProperty p = (TitanVertexProperty)getOnlyElement(g.properties("gid"));
            assertEquals(canonicalPartition,getPartitionID(p,idManager));
            Set<Integer> propPartitions = Sets.newHashSet();
            for (VertexProperty n : g.properties("name").toList()) {
                propPartitions.add(getPartitionID((TitanVertex)n.getElement(),idManager));
            }
            //Verify spread across partitions; this number is a pessimistic lower bound but might fail since it is probabilistic
            assertTrue(propPartitions.size()>=3);

            //Copied from above
            assertCount(1, g.bothE("one"));
            assertCount(1, g.toE(i % 2 == 0 ? Direction.IN : Direction.OUT, "one"));
            assertCount(0, g.toE(i % 2 == 1 ? Direction.IN : Direction.OUT, "one"));
            if (i>0) {
                assertCount(1, g.outE("base"));
            } else {
                assertCount(numG - 1, g.inE("base"));
            }
        }


        clopen(options);

        final int numTx = 100;
        final int vPerTx = 10;
        Multiset<Integer> partitions = HashMultiset.create();

        for (int t=1;t<=numTx;t++) {
            TitanVertex g1 = tx.v(gids[0]), g2 = tx.v(gids[1]);
            assertNotNull(g1);
            TitanVertex[] vs = new TitanVertex[vPerTx];
            for (int vi=0;vi<vPerTx;vi++) {
                vs[vi] = tx.addVertex("person");
                vs[vi].singleProperty("sig", t);
                Edge e = vs[vi].addEdge("knows",g1);
                e.property("sig", t);
                e = g1.addEdge("knows",vs[vi]);
                e.property("sig", t);
                if (vi%2==0) {
                    e = vs[vi].addEdge("knows",g2);
                    e.property("sig", t);
                }
            }
            newTx();
            //Verify that all elements are in the same partition
            TitanTransaction txx = graph.buildTransaction().readOnly().start();
            g1 = tx.v(gids[0]);
            g2 = tx.v(gids[1]);
            int partition = -1;
            for (int vi=0;vi<vPerTx;vi++) {
                assertTrue(vs[vi].hasId());
                int pid = getPartitionID(vs[vi],idManager);
                if (partition<0) partition=pid;
                else assertEquals(partition,pid);
                int numRels = 0;
                TitanVertex v = txx.v(vs[vi].longId());
                for (TitanRelation r : v.query().relations()) {
                    numRels++;
                    assertEquals(partition,getPartitionID(r,idManager));
                    if (r instanceof TitanEdge) {
                        TitanVertex o = ((TitanEdge)r).otherVertex(v);
                        assertTrue(o.equals(g1) || o.equals(g2));
                    }
                }
                assertEquals(3+(vi%2==0?1:0),numRels);
            }
            partitions.add(partition);
            txx.commit();
        }
        //Verify spread across partitions; this number is a pessimistic lower bound but might fail since it is probabilistic
        assertTrue(partitions.elementSet().size()>=3); //

        newTx();
        //Verify edge querying across partitions
        TitanVertex g1 = tx.v(gids[0]);
        assertEquals(0, g1.<Integer>value("gid").intValue());
        assertEquals("group", g1.label());
        assertCount(names.size(), g1.properties("name"));
        assertCount(numTx * vPerTx, g1.outE("knows"));
        assertCount(numTx * vPerTx, g1.inE("knows"));
        assertCount(numTx * vPerTx * 2, g1.bothE("knows"));
        assertCount(numTx * vPerTx + numG, tx.V());

        newTx();
        //Restrict to partitions
        for (int t=0; t<10; t++) {
            int numP = random.nextInt(3)+1;
            Set<Integer> parts = Sets.newHashSet();
            int numV = 0;
            while(parts.size()<numP) {
                int part = Iterables.get(partitions.elementSet(),random.nextInt(partitions.elementSet().size()));
                if (parts.add(part)) numV+=partitions.count(part);
            }
            numV*=vPerTx;
            int[] partarr = new int[numP]; int i=0;
            for (Integer part : parts) partarr[i++]=part;
            TitanTransaction tx2 = graph.buildTransaction().restrictedPartitions(partarr).readOnly().start();
            //Copied from above
            g1 = tx2.v(gids[0]);
            assertEquals(0, g1.<Integer>value("gid").intValue());
            assertEquals("group", g1.label());
            assertTrue(names.size() >= g1.properties("name").count().next());
            assertEquals(numV, g1.outE("knows"));
            assertEquals(numV, g1.inE("knows"));
            assertEquals(numV * 2, g1.bothE("knows"));

            //Test local intersection
            TitanVertex g2 = tx2.v(gids[1]);
            VertexList v1 = g1.query().direction(Direction.IN).labels("knows").vertexIds();
            VertexList v2 = g2.query().direction(Direction.IN).labels("knows").vertexIds();
            assertEquals(numV, v1.size());
            assertEquals(numV/2, v2.size());
            v1.sort();
            v2.sort();
            LongArrayList al1 = v1.getIDs();
            LongArrayList al2 = v2.getIDs();
            assertTrue(AbstractLongListUtil.isSorted(al1));
            assertTrue(AbstractLongListUtil.isSorted(al2));
            LongArrayList alr = AbstractLongListUtil.mergeJoin(al1,al2,false);
            assertEquals(numV/2,alr.size());

            tx2.commit();
        }

        clopen(options);

        //Test OLAP works with partitioned vertices
        final OLAPJobBuilder<OLAPTest.Degree> builder = getOLAPBuilder(graph,OLAPTest.Degree.class);
        OLAPResult<OLAPTest.Degree> degrees = OLAPTest.computeDegree(builder,"name","sig");
        assertNotNull(degrees);
        assertEquals(numTx*vPerTx+numG,degrees.size());
        for (Map.Entry<Long,OLAPTest.Degree> entry : degrees.entries()) {
            long vid = entry.getKey();
            OLAPTest.Degree degree = entry.getValue();
            assertEquals(degree.in+degree.out,degree.both);
            if (idManager.isPartitionedVertex(vid)) {
                if (vid==gids[0]) {
                    assertEquals(numTx*vPerTx + (numG-1) + 1,degree.in);
                    assertEquals(numTx*vPerTx,degree.out);
                } else if (vid==gids[1]) {
                    assertEquals(numTx*vPerTx/2,degree.in);
                    assertEquals(2,degree.out);
                } else {
                    assertEquals(2,degree.in+degree.out);
                }
                assertEquals(names.size(),degree.prop);
            } else {
                assertEquals(1,degree.in);
                assertTrue(1<=degree.out && degree.out<=2);
                assertEquals(0,degree.prop);
            }
        }
    }

    @Test
    @Category({ UnorderedKeyStoreTests.class })
    public void testVLabelOnUnorderedStorage() {
        final String label = "pl";
        VertexLabelMaker maker = mgmt.makeVertexLabel(label);
        try {
            // Exception should be thrown in one of these two methods
            maker.partition().make();
            fail("Partitioned label must be rejected on unordered key stores");
        } catch (IllegalArgumentException e) {
            log.debug("Caught expected exception", e);
        }
    }

    @Test
    @Category({ OrderedKeyStoreTests.class })
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

    public static int getPartitionID(TitanVertex vertex, IDManager idManager) {
        long p = idManager.getPartitionId(vertex.longId());
        assertTrue(p>=0 && p<idManager.getPartitionBound() && p<Integer.MAX_VALUE);
        return (int)p;
    }

    public static int getPartitionID(TitanRelation relation, IDManager idManager) {
        long p = relation.longId() & (idManager.getPartitionBound()-1);
        assertTrue(p>=0 && p<idManager.getPartitionBound() && p<Integer.MAX_VALUE);
        return (int)p;
    }

}
