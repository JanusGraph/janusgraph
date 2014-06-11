package com.thinkaurelius.titan.graphdb;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Decimal;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.core.attribute.Timestamp;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.util.CacheMetricsAction;
import com.thinkaurelius.titan.diskstorage.util.TestLockerManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.testcategory.SerialTests;
import com.thinkaurelius.titan.util.stats.MetricManager;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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


        TitanVertex v = tx.addVertex();
        v.setProperty("uid", "v");

        clopen();

        //Concurrent index addition
        TitanTransaction tx1 = graph.newTransaction();
        TitanTransaction tx2 = graph.newTransaction();
        getVertex(tx1, "uid", "v").setProperty("value", 11);
        getVertex(tx2, "uid", "v").setProperty("value", 11);
        tx1.commit();
        tx2.commit();

        assertEquals("v", Iterables.getOnlyElement(tx.getVertices("value", 11)).getProperty("uid"));

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
        TitanTransaction tx1 = graph.buildTransaction().setCommitTime(100, unit).start();
        String name = "name";
        String age = "age";
        String address = "address";

        TitanVertex v1 = tx1.addVertex();
        TitanVertex v2 = tx1.addVertex();
        v1.setProperty(name, "a");
        v2.setProperty(age, "14");
        v2.setProperty(name, "b");
        v2.setProperty(age, "42");
        tx1.commit();

        // Fetch vertex ids
        long id1 = v1.getID();
        long id2 = v2.getID();

        // Transaction 2: Remove "name" property from v1, set "address" property; create
        // an edge v2 -> v1
        TitanTransaction tx2 = graph.buildTransaction().setCommitTime(1000, unit).start();
        v1 = tx2.getVertex(id1);
        v2 = tx2.getVertex(id2);
        for (TitanProperty prop : v1.getProperties(name)) {
            if (features.hasTimestamps()) {
                Timestamp t = prop.getProperty("$timestamp");
                assertEquals(100,t.sinceEpoch(unit));
                assertEquals(TimeUnit.MICROSECONDS.convert(100,TimeUnit.SECONDS)+1,t.sinceEpoch(TimeUnit.MICROSECONDS));
            }
            if (features.hasTTL()) {
                Duration d = prop.getProperty("$ttl");
                assertEquals(0l,d.getLength(unit));
                assertTrue(d.isZeroLength());
            }
        }
        v1.removeProperty(name);
        v1.setProperty(address, "xyz");
        Edge edge = tx2.addEdge(1, v2, v1, "parent");
        tx2.commit();
        Object edgeId = edge.getId();

        Vertex afterTx2 = graph.getVertex(id1);

        // Verify that "name" property is gone
        assertFalse(afterTx2.getPropertyKeys().contains(name));
        // Verify that "address" property is set
        assertEquals("xyz", afterTx2.getProperty(address));
        // Verify that the edge is properly registered with the endpoint vertex
        assertEquals(1, Iterables.size(afterTx2.getEdges(Direction.IN, "parent")));
        // Verify that edge is registered under the id
        assertNotNull(graph.getEdge(edgeId));
        graph.commit();

        // Transaction 3: Remove "address" property from v1 with earlier timestamp than
        // when the value was set
        TitanTransaction tx3 = graph.buildTransaction().setCommitTime(200, unit).start();
        v1 = tx3.getVertex(id1);
        v1.removeProperty(address);
        tx3.commit();

        Vertex afterTx3 = graph.getVertex(id1);
        graph.commit();
        // Verify that "address" is still set
        assertEquals("xyz", afterTx3.getProperty(address));

        // Transaction 4: Modify "age" property on v2, remove edge between v2 and v1
        TitanTransaction tx4 = graph.buildTransaction().setCommitTime(2000, unit).start();
        v2 = tx4.getVertex(id2);
        v2.setProperty(age, "15");
        tx4.removeEdge(tx4.getEdge(edgeId));
        tx4.commit();

        Vertex afterTx4 = graph.getVertex(id2);
        // Verify that "age" property is modified
        assertEquals("15", afterTx4.getProperty(age));
        // Verify that edge is no longer registered with the endpoint vertex
        assertEquals(0, Iterables.size(afterTx4.getEdges(Direction.OUT, "parent")));
        // Verify that edge entry disappeared from id registry
        assertNull(graph.getEdge(edgeId));

        // Transaction 5: Modify "age" property on v2 with earlier timestamp
        TitanTransaction tx5 = graph.buildTransaction().setCommitTime(1500, unit).start();
        v2 = tx5.getVertex(id2);
        v2.setProperty(age, "16");
        tx5.commit();
        Vertex afterTx5 = graph.getVertex(id2);

        // Verify that the property value is unchanged
        assertEquals("15", afterTx5.getProperty(age));
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
        TitanGraphIndex uidIndex = mgmt.buildIndex("uid",Vertex.class).unique().indexKey(uid).buildInternalIndex();
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
            TitanVertex v = tx.addVertex();
            v.setProperty("uid",i+1);
            v.addEdge("knows",v);
        }
        clopen();
//        System.out.println("Time: " + (System.currentTimeMillis()-start));

        for (int i=0;i<Math.min(numV,300);i++) {
            assertEquals(1, Iterables.size(graph.query().has("uid", i + 1).vertices()));
            assertEquals(1, Iterables.size(graph.query().has("uid", i + 1).vertices().iterator().next().getEdges(Direction.OUT, "knows")));
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
        rs[0]=sign(v.addProperty("weight",5.0),txid);
        rs[1]=sign(v.addProperty("name","John"),txid);
        rs[2]=sign(v.addProperty("value",2),txid);
        rs[3]=sign(v.addProperty("valuef",2),txid);

        rs[6]=sign(v.addEdge("es",u),txid);
        rs[7]=sign(v.addEdge("o2o",u),txid);
        rs[8]=sign(v.addEdge("o2m",u),txid);
        rs[4]=sign(v.addEdge("em",u),txid);
        rs[5]=sign(v.addEdge("emf",u),txid);

        newTx();
        long vid = v.getID(), uid = u.getID();

        TitanTransaction tx1 = graph.newTransaction();
        TitanTransaction tx2 = graph.newTransaction();
        final int wintx = 20;
        processTx(tx1,wintx-10,vid,uid);
        processTx(tx2,wintx,vid,uid);
        tx1.commit();
        Thread.sleep(5);
        tx2.commit(); //tx2 should win using time-based eventual consistency

        newTx();
        v = tx.getVertex(vid);
        assertEquals(6.0,v.<Decimal>getProperty("weight").doubleValue(),0.00001);
        TitanProperty p = Iterables.getOnlyElement(v.getProperties("weight"));
        assertEquals(wintx,p.getProperty("sig"));
        p = Iterables.getOnlyElement(v.getProperties("name"));
        assertEquals("Bob",p.getValue());
        assertEquals(wintx,p.getProperty("sig"));
        p = Iterables.getOnlyElement(v.getProperties("value"));
        assertEquals(rs[2].getID(),p.getID());
        assertEquals(wintx,p.getProperty("sig"));
        assertEquals(2,Iterables.size(v.getProperties("valuef")));
        for (TitanProperty pp : v.getProperties("valuef")) {
            assertNotEquals(rs[3].getID(),pp.getID());
            assertEquals(2,pp.getValue());
        }

        TitanEdge e = (TitanEdge)Iterables.getOnlyElement(v.getEdges(Direction.OUT,"es"));
        assertEquals(wintx,e.getProperty("sig"));
        assertNotEquals(rs[6].getID(),e.getID());
        e = (TitanEdge)Iterables.getOnlyElement(v.getEdges(Direction.OUT,"o2o"));
        assertEquals(wintx,e.getProperty("sig"));
        assertEquals(rs[7].getID(),e.getID());
        e = (TitanEdge)Iterables.getOnlyElement(v.getEdges(Direction.OUT,"o2m"));
        assertEquals(wintx,e.getProperty("sig"));
        assertNotEquals(rs[8].getID(),e.getID());
        e = (TitanEdge)Iterables.getOnlyElement(v.getEdges(Direction.OUT,"em"));
        assertEquals(wintx,e.getProperty("sig"));
        assertEquals(rs[4].getID(),e.getID());
        for (Edge ee : v.getEdges(Direction.OUT,"emf")) {
            assertNotEquals(rs[5].getID(),ee.getId());
            assertEquals(uid,ee.getVertex(Direction.IN).getId());
        }
    }


    private void processTx(TitanTransaction tx, int txid, long vid, long uid) {
        TitanVertex v = tx.getVertex(vid);
        TitanVertex u = tx.getVertex(uid);
        assertEquals(5.0,v.<Decimal>getProperty("weight").doubleValue(),0.00001);
        TitanProperty p = Iterables.getOnlyElement(v.getProperties("weight"));
        assertEquals(1,p.getProperty("sig"));
        sign(v.addProperty("weight",6.0),txid);
        p = Iterables.getOnlyElement(v.getProperties("name"));
        assertEquals(1,p.getProperty("sig"));
        assertEquals("John",p.getValue());
        p.remove();
        sign(v.addProperty("name","Bob"),txid);
        for (String pkey : new String[]{"value","valuef"}) {
            p = Iterables.getOnlyElement(v.getProperties(pkey));
            assertEquals(1,p.getProperty("sig"));
            assertEquals(2,p.getValue());
            sign(p,txid);
        }

        TitanEdge e = (TitanEdge)Iterables.getOnlyElement(v.getEdges(Direction.OUT,"es"));
        assertEquals(1,e.getProperty("sig"));
        e.remove();
        sign(v.addEdge("es",u),txid);
        e = (TitanEdge)Iterables.getOnlyElement(v.getEdges(Direction.OUT,"o2o"));
        assertEquals(1,e.getProperty("sig"));
        sign(e,txid);
        e = (TitanEdge)Iterables.getOnlyElement(v.getEdges(Direction.OUT,"o2m"));
        assertEquals(1,e.getProperty("sig"));
        e.remove();
        sign(v.addEdge("o2m",u),txid);
        for (String label : new String[]{"em","emf"}) {
            e = (TitanEdge)Iterables.getOnlyElement(v.getEdges(Direction.OUT,label));
            assertEquals(1,e.getProperty("sig"));
            sign(e,txid);
        }
    }


    private TitanRelation sign(TitanRelation r, int id) {
        r.setProperty("sig",id);
        return r;
    }





}
