package com.thinkaurelius.titan.graphdb;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.util.TestLockerManager;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.cache.ExpirationStoreCache;
import com.thinkaurelius.titan.testcategory.SerialTests;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TitanEventualGraphTest extends TitanGraphTestCommon {

    private Logger log = LoggerFactory.getLogger(TitanEventualGraphTest.class);

    public TitanEventualGraphTest(Configuration config) {
        super(config);
    }

    public void clopen(Map<String, ? extends Object> settings) {
        super.close();

        BaseConfiguration newConfig = new BaseConfiguration();
        newConfig.copy(config);
        for (Map.Entry<String,? extends Object> entry : settings.entrySet())
            newConfig.addProperty(entry.getKey(),entry.getValue());

        graph = (StandardTitanGraph) TitanFactory.open(newConfig);
        tx = graph.newTransaction();

    }

    @Test
    public void concurrentIndexTest() {
        TitanKey id = tx.makeKey("uid").single().unique().indexed(Vertex.class).dataType(String.class).make();
        TitanKey value = tx.makeKey("value").single(TypeMaker.UniquenessConsistency.NO_LOCK).dataType(Object.class).indexed(Vertex.class).make();

        TitanVertex v = tx.addVertex();
        v.setProperty(id, "v");

        clopen();

        //Concurrent index addition
        TitanTransaction tx1 = graph.newTransaction();
        TitanTransaction tx2 = graph.newTransaction();
        tx1.getVertex(id, "v").setProperty("value", 11);
        tx2.getVertex(id, "v").setProperty("value", 11);
        tx1.commit();
        tx2.commit();

        assertEquals("v", Iterables.getOnlyElement(tx.getVertices("value", 11)).getProperty(id.getName()));

    }

    @Test
    public void testTimestampSetting() {
        // Transaction 1: Init graph with two vertices, having set "name" and "age" properties
        TitanTransaction tx1 = graph.buildTransaction().setTimestamp(100).start();
        String name = "name";
        String age = "age";
        String address = "address";

        Vertex v1 = tx1.addVertex();
        Vertex v2 = tx1.addVertex();
        v1.setProperty(name, "a");
        v2.setProperty(age, "14");
        v2.setProperty(name, "b");
        v2.setProperty(age, "42");
        tx1.commit();

        // Fetch vertex ids
        Object id1 = v1.getId();
        Object id2 = v2.getId();

        // Transaction 2: Remove "name" property from v1, set "address" property; create
        // an edge v2 -> v1
        TitanTransaction tx2 = graph.buildTransaction().setTimestamp(1000).start();
        v1 = tx2.getVertex(id1);
        v2 = tx2.getVertex(id2);
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
        TitanTransaction tx3 = graph.buildTransaction().setTimestamp(200).start();
        v1 = tx3.getVertex(id1);
        v1.removeProperty(address);
        tx3.commit();

        Vertex afterTx3 = graph.getVertex(id1);
        graph.commit();
        // Verify that "address" is still set
        assertEquals("xyz", afterTx3.getProperty(address));

        // Transaction 4: Modify "age" property on v2, remove edge between v2 and v1
        TitanTransaction tx4 = graph.buildTransaction().setTimestamp(2000).start();
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
        TitanTransaction tx5 = graph.buildTransaction().setTimestamp(1500).start();
        v2 = tx5.getVertex(id2);
        v2.setProperty(age, "16");
        tx5.commit();
        Vertex afterTx5 = graph.getVertex(id2);

        // Verify that the property value is unchanged
        assertEquals("15", afterTx5.getProperty(age));
    }

    @Test
    public void testBatchLoadingNoLock() {
        testBatchLoadingLocking(true);
    }

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
        tx.makeKey("uid").dataType(Long.class).indexed(Vertex.class).single(TypeMaker.UniquenessConsistency.LOCK).unique(TypeMaker.UniquenessConsistency.LOCK).make();
        tx.makeLabel("knows").oneToOne(TypeMaker.UniquenessConsistency.LOCK).make();
        newTx();

        TestLockerManager.ERROR_ON_LOCKING=true;
        clopen(ImmutableMap.of("storage.batch-loading", batchloading, "storage.lock-backend", "test"));


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
            assertEquals(1,Iterables.size(graph.query().has("uid",i+1).vertices()));
            assertEquals(1,Iterables.size(graph.query().has("uid",i+1).vertices().iterator().next().getEdges(Direction.OUT,"knows")));
        }
    }

    @Test
    @Category({ SerialTests.class })
    public void testCacheConcurrency() throws InterruptedException {
        Map<String,? extends Object> newConfig = ImmutableMap.of("cache.db-cache",true,"cache.db-cache-time",0,"cache.db-cache-clean-wait",0,"cache.db-cache-size",0.25);
        clopen(newConfig);
        final String prop = "property";
        graph.makeKey(prop).dataType(Integer.class).single(TypeMaker.UniquenessConsistency.NO_LOCK).make();

        final int numV = 100;
        final long[] vids = new long[numV];
        for (int i=0;i<numV;i++) {
            TitanVertex v = graph.addVertex(null);
            v.setProperty(prop,0);
            graph.commit();
            vids[i]=v.getID();
        }
        clopen(newConfig);
        ExpirationStoreCache.resetGlobablCounts();

        final AtomicBoolean[] precommit = new AtomicBoolean[numV];
        final AtomicBoolean[] postcommit = new AtomicBoolean[numV];
        for (int i=0;i<numV;i++) {
            precommit[i]=new AtomicBoolean(false);
            postcommit[i]=new AtomicBoolean(false);
        }
        final AtomicInteger lookups = new AtomicInteger(0);
        final Random random = new Random();
        final int updateSleepTime = 40;
        final int readSleepTime = 2;
        final int numReads = Math.round((numV*updateSleepTime)/readSleepTime*2.0f);

        Thread reader = new Thread(new Runnable() {
            @Override
            public void run() {
                int reads = 0;
                while (reads<numReads) {
                    final int pos = random.nextInt(vids.length);
                    long vid = vids[pos];
                    TitanVertex v = graph.getVertex(vid);
                    assertNotNull(v);
                    boolean postCommit = postcommit[pos].get();
                    Integer value = v.getProperty(prop);
                    lookups.incrementAndGet();
                    assertNotNull("On pos ["+pos+"]",value);
                    if (!precommit[pos].get()) assertEquals(0,value.intValue());
                    else if (postCommit) assertEquals(1,value.intValue());
                    graph.commit();
                    try {
                        Thread.sleep(readSleepTime);
                    } catch (InterruptedException e) {
                        return;
                    }
                    reads++;
                }
            }
        });
        reader.start();

        Thread updater = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i=0;i<numV;i++) {
                    try {
                        TitanVertex v = graph.getVertex(vids[i]);
                        v.setProperty(prop,1);
                        precommit[i].set(true);
                        graph.commit();
                        postcommit[i].set(true);
                        Thread.sleep(updateSleepTime);
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Unexpected interruption",e);
                    }
                }
            }
        });
        updater.start();
        updater.join();
//        reader.start();
        reader.join();

        System.out.println("Retrievals: " + ExpirationStoreCache.getGlobalCacheRetrievals());
        System.out.println("Hits: " + ExpirationStoreCache.getGlobalCacheHits());
        System.out.println("Misses: " + ExpirationStoreCache.getGlobalCacheMisses());
        assertEquals(numReads,lookups.get());
        assertEquals(4*numV+2,ExpirationStoreCache.getGlobalCacheMisses());
    }


    @Test
    @Category({ SerialTests.class })
    public void testCachePerformance() {
//        Map<String,? extends Object> newConfig = ImmutableMap.of();
        Map<String,? extends Object> newConfig = ImmutableMap.of("cache.db-cache",true,"cache.db-cache-time",0);
        clopen(newConfig);

        int numV = 1000;

        TitanVertex previous = null;
        for (int i=0;i<numV;i++) {
            TitanVertex v = graph.addVertex(null);
            v.setProperty("name", "v" + i);
            if (previous!=null)
                v.addEdge("knows",previous);
            previous = v;
        }
        graph.commit();
        long vertexId = previous.getID();
        assertEquals(numV, Iterables.size(graph.getVertices()));

        clopen(newConfig);

        double timecoldglobal=0, timewarmglobal=0,timehotglobal=0;

        int outerRepeat = 20;
        int measurements = 10;
        assertTrue(measurements<outerRepeat);
        int innerRepeat = 2;
        for (int c=0;c<outerRepeat;c++) {

            double timecold = testAllVertices(vertexId,numV);

            double timewarm = 0;
            double timehot = 0;
            for (int i = 0;i<innerRepeat;i++) {
                graph.commit();
                timewarm += testAllVertices(vertexId,numV);
                for (int j=0;j<innerRepeat;j++) {
                    timehot += testAllVertices(vertexId,numV);
                }
            }
            timewarm = timewarm / innerRepeat;
            timehot = timehot / (innerRepeat*innerRepeat);

            if (c>=(outerRepeat-measurements)) {
                timecoldglobal += timecold;
                timewarmglobal += timewarm;
                timehotglobal  += timehot;
            }
//            System.out.println(timecold + "\t" + timewarm + "\t" + timehot);
            clopen(newConfig);
        }
        timecoldglobal = timecoldglobal/measurements;
        timewarmglobal = timewarmglobal/measurements;
        timehotglobal = timehotglobal/measurements;

        System.out.println(round(timecoldglobal) + "\t" + round(timewarmglobal) + "\t" + round(timehotglobal));
        assertTrue(timecoldglobal + " vs " + timewarmglobal, timecoldglobal>timewarmglobal*2);
        assertTrue(timewarmglobal + " vs " + timehotglobal, timewarmglobal>timehotglobal*1.2);
    }

    private double testAllVertices(long vid, int numV) {
        long start = System.nanoTime();
        Vertex v = graph.getVertex(vid);
        for (int i=1; i<numV; i++) {
            v = Iterables.getOnlyElement(v.getVertices(Direction.OUT, "knows"));
        }
        return ((System.nanoTime()-start)/1000000.0);
    }

    @Test
    @Category({ SerialTests.class })
    public void testCacheExpirationTimeOut() throws InterruptedException {
        testCacheExpiration(4000,6000);
    }

    @Test
    @Category({ SerialTests.class })
    public void testCacheExpirationNoTimeOut() throws InterruptedException {
        testCacheExpiration(0,5000);
    }


    public void testCacheExpiration(final int timeOutTime, final int waitTime) throws InterruptedException {
        Preconditions.checkArgument(timeOutTime==0 || timeOutTime<waitTime);
        final int cleanTime = 400;
        final int numV = 10;
        final int edgePerV = 10;
        Map<String,? extends Object> newConfig = ImmutableMap.of("cache.db-cache",true,"cache.db-cache-time",timeOutTime,"cache.db-cache-clean-wait",cleanTime);
        clopen(newConfig);
        long[] vs = new long[numV];
        for (int i=0;i<numV;i++) {
            TitanVertex v = graph.addVertex(null);
            v.setProperty("name", "v" + i);
            for (int t=0;t<edgePerV;t++) {
                TitanEdge e = v.addEdge("knows",v);
                e.setProperty("time",t);
            }
            graph.commit();
            vs[i]=v.getID();
        }
        clopen(newConfig);
        ExpirationStoreCache.resetGlobablCounts();
        int labelcalls = 2;
        int calls = numV*2+labelcalls; // numV * (vertex existence + loading edges) + 2 getting "knows" definition
        for (int i=0;i<numV;i++) assertEquals(edgePerV,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyCacheCalls(calls,calls,0);
        graph.commit();
        for (int i=0;i<numV;i++) assertEquals(edgePerV,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyCacheCalls(calls*2,calls,calls);
        //Nothing changes without commit => hitting transactional caches
        for (int i=0;i<numV;i++) assertEquals(edgePerV,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyCacheCalls(calls*2,calls,calls);

        clopen(newConfig);
        ExpirationStoreCache.resetGlobablCounts();
        for (int i=0;i<numV;i++) assertEquals(edgePerV,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyCacheCalls(calls,calls,0);
        for (int i=0;i<numV;i++) {
            graph.getVertex(vs[i]).addEdge("knows",graph.getVertex(vs[i]));
        }
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyCacheCalls(calls,calls,0); //Everything served out of tx cache
        graph.commit();
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyCacheCalls(calls+labelcalls,calls,labelcalls); //Due to invalid cache, only edge label is served from it
        graph.commit();
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyCacheCalls(calls+2*labelcalls,calls,2*labelcalls); //Things are still invalid....
        graph.commit();
        Thread.sleep(cleanTime*2); //until we wait for the expiration threshold, now the next lookup should trigger a clean
        verifyCacheCalls(calls+2*labelcalls,calls,2*labelcalls);
        ExpirationStoreCache.resetGlobablCounts();

        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        graph.commit();
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        assertTrue(ExpirationStoreCache.getGlobalCacheRetrievals()>=calls-1); //Things are somewhat non-deterministic here due to the parallel cleanup thread
        assertEquals(calls-labelcalls,ExpirationStoreCache.getGlobalCacheMisses());
        graph.commit();
        ExpirationStoreCache.resetGlobablCounts();
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyCacheCalls(calls,0,calls);


        //Same as above - verify reset after shutdown
        clopen(newConfig);
        ExpirationStoreCache.resetGlobablCounts();
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyCacheCalls(calls,calls,0);
        graph.commit();
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyCacheCalls(calls*2,calls,calls);
        //Nothing changes without commit => hitting transactional caches
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyCacheCalls(calls*2,calls,calls);
        graph.commit();

        //Time the cache out
        Thread.sleep(waitTime);
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        if (timeOutTime==0)
            verifyCacheCalls(calls*3,calls,calls*2);
        else
            verifyCacheCalls(calls*3,calls*2,calls);
    }

    private void verifyCacheCalls(int total, int misses, int hits) {
        assertEquals(total, ExpirationStoreCache.getGlobalCacheRetrievals());
        assertEquals(misses,ExpirationStoreCache.getGlobalCacheMisses());
        assertEquals(hits, ExpirationStoreCache.getGlobalCacheHits());
    }


    public static double round(double d) {
        return Math.round(d*1000.0)/1000.0;
    }


}
