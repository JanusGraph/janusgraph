package com.thinkaurelius.titan.graphdb;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
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

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

@Category({ SerialTests.class })
public abstract class TitanEventualGraphTest extends TitanGraphBaseTest {

    private Logger log = LoggerFactory.getLogger(TitanEventualGraphTest.class);

//    public void clopen(Map<String, ? extends Object> settings) {
//        super.close();
//
//        BaseConfiguration newConfig = new BaseConfiguration();
//        newConfig.copy(config);
//        for (Map.Entry<String,? extends Object> entry : settings.entrySet())
//            newConfig.addProperty(entry.getKey(),entry.getValue());
//
//        graph = (StandardTitanGraph) TitanFactory.open(newConfig);
//        tx = graph.newTransaction();
//
//    }

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
                Timestamp t = prop.getProperty("_timestamp");
                assertEquals(100,t.sinceEpoch(unit));
                assertEquals(TimeUnit.MICROSECONDS.convert(100,TimeUnit.SECONDS)+1,t.sinceEpoch(TimeUnit.MICROSECONDS));
            }
            if (features.hasTTL()) {
                Duration d = prop.getProperty("_ttl");
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

    @Test
    @Ignore //TODO: Fix up an include
    public void testCacheConcurrency() throws InterruptedException {
        metricsPrefix = "evgt1";
        Object[] newConfig = {option(GraphDatabaseConfiguration.DB_CACHE),true,
                option(GraphDatabaseConfiguration.DB_CACHE_TIME),0,
                option(GraphDatabaseConfiguration.DB_CACHE_CLEAN_WAIT),0,
                option(GraphDatabaseConfiguration.DB_CACHE_SIZE),0.25,
                option(GraphDatabaseConfiguration.BASIC_METRICS),true,
                option(GraphDatabaseConfiguration.METRICS_MERGE_STORES),false,
                option(GraphDatabaseConfiguration.METRICS_PREFIX),metricsPrefix};
        clopen(newConfig);
        final String prop = "property";
        graph.makePropertyKey(prop).dataType(Integer.class).make();

        final int numV = 100;
        final long[] vids = new long[numV];
        for (int i=0;i<numV;i++) {
            TitanVertex v = graph.addVertex(null);
            v.setProperty(prop,0);
            graph.commit();
            vids[i]=v.getID();
        }
        clopen(newConfig);
        resetEdgeCacheCounts();

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

        System.out.println("Retrievals: " + getEdgeCacheRetrievals());
        System.out.println("Hits: " + (getEdgeCacheRetrievals()-getEdgeCacheMisses()));
        System.out.println("Misses: " + getEdgeCacheMisses());
        assertEquals(numReads,lookups.get());
        assertEquals(2*numReads+2*numV + 2, getEdgeCacheRetrievals());
        int minMisses = 4*numV+2;
        assertTrue("Min misses ["+minMisses+"] vs actual ["+getEdgeCacheMisses()+"]",minMisses<=getEdgeCacheMisses() && 4*minMisses>=getEdgeCacheMisses());
    }


    @Test
    public void testCachePerformance() {
        Object[] newConfig = {option(GraphDatabaseConfiguration.DB_CACHE),true,
                              option(GraphDatabaseConfiguration.DB_CACHE_TIME),0};
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
    @Ignore //TODO: Ignore for now until everything is stable - then do the counting
    public void testCacheExpirationTimeOut() throws InterruptedException {
        testCacheExpiration(4000,6000);
    }

    @Test
    @Ignore //TODO: Ignore for now until everything is stable - then do the counting
    public void testCacheExpirationNoTimeOut() throws InterruptedException {
        testCacheExpiration(0,5000);
    }


    public void testCacheExpiration(final int timeOutTime, final int waitTime) throws InterruptedException {
        Preconditions.checkArgument(timeOutTime==0 || timeOutTime<waitTime);
        metricsPrefix="evgt2";
        final int cleanTime = 400;
        final int numV = 10;
        final int edgePerV = 10;
        Object[] newConfig = {option(GraphDatabaseConfiguration.DB_CACHE),true,
                              option(GraphDatabaseConfiguration.DB_CACHE_TIME),timeOutTime,
                              option(GraphDatabaseConfiguration.DB_CACHE_CLEAN_WAIT),cleanTime,
                              option(GraphDatabaseConfiguration.BASIC_METRICS),true,
                              option(GraphDatabaseConfiguration.METRICS_MERGE_STORES),false,
                              option(GraphDatabaseConfiguration.METRICS_PREFIX),metricsPrefix};
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
        resetEdgeCacheCounts();
        int labelcalls = 1; //getting "knows" definition
        int calls = numV*2; // numV * (vertex existence + loading edges)
        for (int i=0;i<numV;i++) assertEquals(edgePerV,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyEdgeCache(calls+labelcalls, calls+labelcalls);
        graph.commit();
        for (int i=0;i<numV;i++) assertEquals(edgePerV,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyEdgeCache(calls * 2+labelcalls, calls+labelcalls);
        //Nothing changes without commit => hitting transactional caches
        for (int i=0;i<numV;i++) assertEquals(edgePerV,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyEdgeCache(calls * 2+labelcalls, calls+labelcalls);

        clopen(newConfig);
        resetEdgeCacheCounts();
        for (int i=0;i<numV;i++) assertEquals(edgePerV,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyEdgeCache(calls+labelcalls, calls+labelcalls); //after data base re-open and reset, everything pulled from disk
        for (int i=0;i<numV;i++) {
            graph.getVertex(vs[i]).addEdge("knows",graph.getVertex(vs[i]));
        }
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyEdgeCache(calls+labelcalls, calls+labelcalls); //Everything served out of tx cache
        graph.commit();
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyEdgeCache(2*calls + labelcalls, 2* calls+labelcalls); //Due to invalid cache, only edge label is served from it
        graph.commit();
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyEdgeCache(3*calls + labelcalls, 3*calls + labelcalls); //Things are still invalid....
        graph.commit();
        Thread.sleep(cleanTime*2); //until we wait for the expiration threshold, now the next lookup should trigger a clean
        verifyEdgeCache(3*calls + labelcalls, 3*calls + labelcalls);
        resetEdgeCacheCounts();

        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        graph.commit();
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        assertTrue(getEdgeCacheRetrievals()>=calls-1); //Things are somewhat non-deterministic here due to the parallel cleanup thread
        assertTrue(getEdgeCacheMisses()>=calls);
        graph.commit();
        resetEdgeCacheCounts();
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyEdgeCache(calls, 0);


        //Same as above - verify reset after shutdown
        clopen(newConfig);
        resetEdgeCacheCounts();
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyEdgeCache(calls+labelcalls, calls+labelcalls);
        graph.commit();
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyEdgeCache(calls*2 + labelcalls, calls+labelcalls);
        //Nothing changes without commit => hitting transactional caches
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        verifyEdgeCache(calls*2 + labelcalls, calls+labelcalls);
        graph.commit();

        //Time the cache out
        Thread.sleep(waitTime);
        for (int i=0;i<numV;i++) assertEquals(edgePerV+1,Iterables.size(graph.getVertex(vs[i]).getVertices(Direction.OUT,"knows")));
        if (timeOutTime==0)
            verifyEdgeCache(calls*3 + labelcalls, calls + labelcalls);
        else
            verifyEdgeCache(calls*3 + labelcalls, calls*2 + labelcalls);
    }

    private String metricsPrefix;

    private void verifyEdgeCache(int total, int misses) {
        assert metricsPrefix!=null;
        assertEquals(total, getEdgeCacheRetrievals());
        assertEquals(misses, getEdgeCacheMisses());
    }

    private long getEdgeCacheRetrievals() {
        return MetricManager.INSTANCE.getCounter(metricsPrefix, "edgeStore" + Backend.METRICS_CACHE_SUFFIX, CacheMetricsAction.RETRIEVAL.getName()).getCount();
    }

    private long getEdgeCacheMisses() {
        return MetricManager.INSTANCE.getCounter(metricsPrefix, "edgeStore" + Backend.METRICS_CACHE_SUFFIX, CacheMetricsAction.MISS.getName()).getCount();
    }

    private void resetEdgeCacheCounts() {
        Counter counter = MetricManager.INSTANCE.getCounter(metricsPrefix, "edgeStore" + Backend.METRICS_CACHE_SUFFIX, CacheMetricsAction.RETRIEVAL.getName());
        counter.dec(counter.getCount());
        counter = MetricManager.INSTANCE.getCounter(metricsPrefix, "edgeStore" + Backend.METRICS_CACHE_SUFFIX, CacheMetricsAction.MISS.getName());
        counter.dec(counter.getCount());
    }


    public static double round(double d) {
        return Math.round(d*1000.0)/1000.0;
    }


}
