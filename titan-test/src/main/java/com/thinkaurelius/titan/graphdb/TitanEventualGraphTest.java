package com.thinkaurelius.titan.graphdb;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Decimal;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.core.attribute.Timestamp;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.diskstorage.util.TestLockerManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.testcategory.SerialTests;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.thinkaurelius.titan.testutil.TitanAssert.assertCount;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

@Category({ SerialTests.class })
public abstract class TitanEventualGraphTest extends TitanGraphBaseTest {

    private Logger log = LoggerFactory.getLogger(TitanEventualGraphTest.class);

    @Test
    public void verifyEligibility() {
        Preconditions.checkArgument(!graph.getConfiguration().getBackend().getStoreFeatures().hasTxIsolation(),
                "This test suite only applies to eventually consistent data stores");
    }

    @Test
    public void concurrentIndexTest() {
        makeVertexIndexedUniqueKey("uid", String.class);
        makeVertexIndexedKey("value", Object.class);
        finishSchema();


        Vertex v = tx.addVertex("uid", "v");

        clopen();

        //Concurrent index addition
        TitanTransaction tx1 = graph.newTransaction();
        TitanTransaction tx2 = graph.newTransaction();
        getVertex(tx1, "uid", "v").singleProperty("value", 11);
        getVertex(tx2, "uid", "v").singleProperty("value", 11);
        tx1.commit();
        tx2.commit();

        assertEquals("v", getOnlyElement(tx.V().has("value",11).values("uid")));

    }

    /**
     * Tests the correct interpretation of the commit time and that timestamps can be read
     */
    @Test
    public void testTimestampSetting() {
        clopen(option(GraphDatabaseConfiguration.STORE_META_TIMESTAMPS,"edgestore"),true,
                option(GraphDatabaseConfiguration.STORE_META_TTL,"edgestore"),true);
        final TimeUnit unit = TimeUnit.SECONDS;

        // Transaction 1: Init graph with two vertices, having set "name" and "age" properties
        TitanTransaction tx1 = graph.buildTransaction().commitTime(100, unit).start();
        String name = "name";
        String age = "age";
        String address = "address";

        Vertex v1 = tx1.addVertex(name, "a");
        Vertex v2 = tx1.addVertex(age, "14", name, "b", age, "42");
        tx1.commit();

        // Fetch vertex ids
        long id1 = getId(v1);
        long id2 = getId(v2);

        // Transaction 2: Remove "name" property from v1, set "address" property; create
        // an edge v2 -> v1
        TitanTransaction tx2 = graph.buildTransaction().commitTime(1000, unit).start();
        v1 = getV(tx2,id1);
        v2 = getV(tx2,id2);
        for (VertexProperty prop : v1.properties(name).toList()) {
            if (features.hasTimestamps()) {
                Timestamp t = prop.value("^timestamp");
                assertEquals(100,t.sinceEpoch(unit));
                assertEquals(TimeUnit.MICROSECONDS.convert(100,TimeUnit.SECONDS)+1,t.sinceEpoch(TimeUnit.MICROSECONDS));
            }
            if (features.hasCellTTL()) {
                Duration d = prop.value("^ttl");
                assertEquals(0l,d.getLength(unit));
                assertTrue(d.isZeroLength());
            }
        }
        v1.property(name).remove();
        v1.singleProperty(address, "xyz");
        Edge edge = v2.addEdge("parent",v1);
        tx2.commit();
        Object edgeId = edge.id();

        Vertex afterTx2 = getV(graph,id1);

        // Verify that "name" property is gone
        assertFalse(afterTx2.keys().contains(name));
        // Verify that "address" property is set
        assertEquals("xyz", afterTx2.value(address));
        // Verify that the edge is properly registered with the endpoint vertex
        assertCount(1, afterTx2.inE("parent"));
        // Verify that edge is registered under the id
        assertNotNull(getE(graph,edgeId));
        graph.tx().commit();

        // Transaction 3: Remove "address" property from v1 with earlier timestamp than
        // when the value was set
        TitanTransaction tx3 = graph.buildTransaction().commitTime(200, unit).start();
        v1 = getV(tx3,id1);
        v1.property(address).remove();
        tx3.commit();

        Vertex afterTx3 = getV(graph,id1);
        graph.tx().commit();
        // Verify that "address" is still set
        assertEquals("xyz", afterTx3.value(address));

        // Transaction 4: Modify "age" property on v2, remove edge between v2 and v1
        TitanTransaction tx4 = graph.buildTransaction().commitTime(2000, unit).start();
        v2 = getV(tx4,id2);
        v2.singleProperty(age, "15");
        getE(tx4,edgeId).remove();
        tx4.commit();

        Vertex afterTx4 = getV(graph,id2);
        // Verify that "age" property is modified
        assertEquals("15", afterTx4.value(age));
        // Verify that edge is no longer registered with the endpoint vertex
        assertCount(0, afterTx4.outE("parent"));
        // Verify that edge entry disappeared from id registry
        assertNull(getE(graph,edgeId));

        // Transaction 5: Modify "age" property on v2 with earlier timestamp
        TitanTransaction tx5 = graph.buildTransaction().commitTime(1500, unit).start();
        v2 = getV(tx5,id2);
        v2.singleProperty(age, "16");
        tx5.commit();
        Vertex afterTx5 = getV(graph,id2);

        // Verify that the property value is unchanged
        assertEquals("15", afterTx5.value(age));
    }

    /**
     * Tests that batch-loading will ignore locks
     */
    @Test
    public void testBatchLoadingNoLock() {
        testBatchLoadingLocking(true);
    }

    /**
     * Tests that without batch-loading locks will be correctly applied (and therefore the tx fails)
     */
    @Test
    public void testLockException() {
        try {
            testBatchLoadingLocking(false);
            fail();
        } catch (TitanException e) {
            Throwable cause = e;
            while (cause.getCause()!=null) cause=cause.getCause();
            assertEquals(UnsupportedOperationException.class,cause.getClass());
        }
    }

    public void testBatchLoadingLocking(boolean batchloading) {
        PropertyKey uid = makeKey("uid",Long.class);
        TitanGraphIndex uidIndex = mgmt.buildIndex("uid",Vertex.class).unique().addKey(uid).buildCompositeIndex();
        mgmt.setConsistency(uid, ConsistencyModifier.LOCK);
        mgmt.setConsistency(uidIndex,ConsistencyModifier.LOCK);
        EdgeLabel knows = mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.ONE2ONE).make();
        mgmt.setConsistency(knows,ConsistencyModifier.LOCK);
        finishSchema();

        TestLockerManager.ERROR_ON_LOCKING=true;
        clopen(option(GraphDatabaseConfiguration.STORAGE_BATCH),batchloading,
                option(GraphDatabaseConfiguration.LOCK_BACKEND),"test");


        int numV = 10000;
        long start = System.currentTimeMillis();
        for (int i=0;i<numV;i++) {
            Vertex v = tx.addVertex("uid",i+1);
            v.addEdge("knows",v);
        }
        clopen();
//        System.out.println("Time: " + (System.currentTimeMillis()-start));

        for (int i=0;i<Math.min(numV,300);i++) {
            assertCount(1, graph.V().has("uid", i + 1));
            assertCount(1, ((Vertex)getOnlyElement(graph.V().has("uid", i + 1))).outE("knows"));
        }
    }

    /**
     * Tests that consistency modes are correctly interpreted in the absence of locks (or tx isolation)
     */
    @Test
    public void testConsistencyModifier() throws InterruptedException {
        PropertyKey sig = makeKey("sig",Integer.class);
        PropertyKey weight = makeKey("weight",Decimal.class);
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SET).make();
        PropertyKey value = mgmt.makePropertyKey("value").dataType(Integer.class).cardinality(Cardinality.LIST).make();
        PropertyKey valuef = mgmt.makePropertyKey("valuef").dataType(Integer.class).cardinality(Cardinality.LIST).make();
        mgmt.setConsistency(valuef,ConsistencyModifier.FORK);

        EdgeLabel em = mgmt.makeEdgeLabel("em").multiplicity(Multiplicity.MULTI).make();
        EdgeLabel emf = mgmt.makeEdgeLabel("emf").multiplicity(Multiplicity.MULTI).make();
        mgmt.setConsistency(emf,ConsistencyModifier.FORK);
        EdgeLabel es = mgmt.makeEdgeLabel("es").multiplicity(Multiplicity.SIMPLE).make();
        EdgeLabel o2o = mgmt.makeEdgeLabel("o2o").multiplicity(Multiplicity.ONE2ONE).make();
        EdgeLabel o2m = mgmt.makeEdgeLabel("o2m").multiplicity(Multiplicity.ONE2MANY).make();

        finishSchema();

        TitanVertex u = tx.addVertex(), v = tx.addVertex();
        TitanRelation[] rs = new TitanRelation[9];
        final int txid = 1;
        rs[0]=sign(v.property("weight",5.0),txid);
        rs[1]=sign(v.property("name","John"),txid);
        rs[2]=sign(v.property("value",2),txid);
        rs[3]=sign(v.property("valuef",2),txid);

        rs[6]=sign(v.addEdge("es",u),txid);
        rs[7]=sign(v.addEdge("o2o",u),txid);
        rs[8]=sign(v.addEdge("o2m",u),txid);
        rs[4]=sign(v.addEdge("em",u),txid);
        rs[5]=sign(v.addEdge("emf",u),txid);

        newTx();
        long vid = getId(v), uid = getId(u);

        TitanTransaction tx1 = graph.newTransaction();
        TitanTransaction tx2 = graph.newTransaction();
        final int wintx = 20;
        processTx(tx1,wintx-10,vid,uid);
        processTx(tx2,wintx,vid,uid);
        tx1.commit();
        Thread.sleep(5);
        tx2.commit(); //tx2 should win using time-based eventual consistency

        newTx();
        v = getV(tx,vid);
        assertEquals(6.0,v.<Decimal>value("weight").doubleValue(),0.00001);
        VertexProperty p = getOnlyElement(v.properties("weight"));
        assertEquals(wintx,p.<Integer>value("sig").intValue());
        p = getOnlyElement(v.properties("name"));
        assertEquals("Bob",p.value());
        assertEquals(wintx,p.<Integer>value("sig").intValue());
        p = getOnlyElement(v.properties("value"));
        assertEquals(rs[2].longId(),getId(p));
        assertEquals(wintx,p.<Integer>value("sig").intValue());
        assertCount(2,v.properties("valuef"));
        for (VertexProperty pp : v.properties("valuef").toList()) {
            assertNotEquals(rs[3].longId(),getId(pp));
            assertEquals(2,pp.value());
        }

        Edge e = getOnlyElement(v.outE("es"));
        assertEquals(wintx,e.<Integer>value("sig").intValue());
        assertNotEquals(rs[6].longId(),getId(e));

        e = getOnlyElement(v.outE("o2o"));
        assertEquals(wintx,e.<Integer>value("sig").intValue());
        assertNotEquals(rs[7].longId(),getId(e));
        e = getOnlyElement(v.outE("o2m"));
        assertEquals(wintx,e.<Integer>value("sig").intValue());
        assertNotEquals(rs[8].longId(),getId(e));
        e = getOnlyElement(v.outE("em"));
        assertEquals(wintx,e.<Integer>value("sig").intValue());
        assertNotEquals(rs[4].longId(),getId(e));
        for (Edge ee : v.outE("emf").toList()) {
            assertNotEquals(rs[5].longId(),getId(ee));
            assertEquals(uid,getOnlyElement(ee.inV()).id());
        }
    }


    private void processTx(TitanTransaction tx, int txid, long vid, long uid) {
        TitanVertex v = getV(tx,vid);
        TitanVertex u = getV(tx,uid);
        assertEquals(5.0,v.<Decimal>value("weight").doubleValue(),0.00001);
        VertexProperty p = getOnlyElement(v.properties("weight"));
        assertEquals(1,p.<Integer>value("sig").intValue());
        sign(v.property("weight",6.0),txid);
        p = getOnlyElement(v.properties("name"));
        assertEquals(1,p.<Integer>value("sig").intValue());
        assertEquals("John",p.value());
        p.remove();
        sign(v.property("name","Bob"),txid);
        for (String pkey : new String[]{"value","valuef"}) {
            p = getOnlyElement(v.properties(pkey));
            assertEquals(1,p.<Integer>value("sig").intValue());
            assertEquals(2,p.value());
            sign((TitanVertexProperty)p,txid);
        }

        Edge e = getOnlyElement(v.outE("es"));
        assertEquals(1,e.<Integer>value("sig").intValue());
        e.remove();
        sign(v.addEdge("es",u),txid);
        e = getOnlyElement(v.outE("o2o"));
        assertEquals(1,e.<Integer>value("sig").intValue());
        sign((TitanEdge)e,txid);
        e = getOnlyElement(v.outE("o2m"));
        assertEquals(1,e.<Integer>value("sig").intValue());
        e.remove();
        sign(v.addEdge("o2m",u),txid);
        for (String label : new String[]{"em","emf"}) {
            e = getOnlyElement(v.outE(label));
            assertEquals(1,e.<Integer>value("sig").intValue());
            sign((TitanEdge)e,txid);
        }
    }


    private TitanRelation sign(TitanRelation r, int id) {
        r.property("sig",id);
        return r;
    }





}
