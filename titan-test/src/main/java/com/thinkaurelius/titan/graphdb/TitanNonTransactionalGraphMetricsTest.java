package com.thinkaurelius.titan.graphdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.CachedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.util.MetricInstrumentedStore;
import static com.thinkaurelius.titan.diskstorage.util.MetricInstrumentedStore.*;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.testcategory.SerialTests;
import com.thinkaurelius.titan.util.stats.MetricManager;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@Category({ SerialTests.class })
public abstract class TitanNonTransactionalGraphMetricsTest {

    public StandardTitanGraph graph;
    public MetricManager metric;
    public StoreFeatures features;

    public final String SYSTEM_METRICS  = GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT;

    public abstract Configuration getConfiguration();

    public Configuration getMetricsConfiguration() {
        Configuration config = getConfiguration();
        Configuration storeconfig = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        storeconfig.setProperty(GraphDatabaseConfiguration.BASIC_METRICS,true);
        storeconfig.setProperty(GraphDatabaseConfiguration.MERGE_BASIC_METRICS_KEY,false);
        config.setProperty(GraphDatabaseConfiguration.PROPERTY_PREFETCHING_KEY,false);
        return config;
    }

    @Before
    public void before() throws StorageException {
        GraphDatabaseConfiguration graphconfig = new GraphDatabaseConfiguration(getConfiguration());
        graphconfig.getBackend().clearStorage();
        features = graphconfig.getStoreFeatures();
        open(getMetricsConfiguration());
    }

    public void open(Configuration config) {
        graph = (StandardTitanGraph)TitanFactory.open(config);
        metric = MetricManager.INSTANCE;
        CachedKeyColumnValueStore.resetGlobalMetrics();
    }

    @After
    public void close() {
        graph.shutdown();
    }

    public void clopen(Map<String,? extends Object> settings) {
        close();
        Configuration config = getMetricsConfiguration();
        for (Map.Entry<String,? extends Object> entry : settings.entrySet()) {
            config.setProperty(entry.getKey(),entry.getValue());
        }
        open(config);
    }

    public static final List<String> STORE_NAMES =
            ImmutableList.of("edgeStore", "vertexIndexStore", "edgeIndexStore", "idStore");

    @Test
    public void testKCVSAccess1() throws InterruptedException {
        CachedKeyColumnValueStore.resetGlobalMetrics();
        METRICS = "metrics1";

        TitanTransaction tx = graph.buildTransaction().setMetricsPrefix(METRICS).start();
        TitanVertex v = tx.addVertex(null);
        verifyMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 2l, M_GET_SLICE, 4l));
        ElementHelper.setProperties(v, "age", 25, "name", "john");
        TitanVertex u = tx.addVertex(null);
        ElementHelper.setProperties(u, "age", 35, "name", "mary");
        v.addEdge("knows", u);
        tx.commit();
//        printAllMetrics();
//        printAllMetrics(SYSTEM_METRICS);
        verifyMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l));
        verifyMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyMetrics(STORE_NAMES.get(2));
        Thread.sleep(500);
        verifyMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 4l, M_GET_SLICE, 8l));
        assertEquals(3, CachedKeyColumnValueStore.getGlobalCacheMisses());
        assertEquals(0, CachedKeyColumnValueStore.getGlobalCacheHits());

        //Check type name & definition caching
        tx = graph.buildTransaction().setMetricsPrefix(METRICS).start();
        v = tx.getVertex(v.getID());
        assertEquals(2,Iterables.size(v.getProperties()));
        verifyMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 6l)); //1 verify vertex existence, 1 for query, 2 for each of the 2 types (getName/Definition)
        verifyMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyMetrics(STORE_NAMES.get(2));
        verifyMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 4l, M_GET_SLICE, 8l));
        assertEquals(7, CachedKeyColumnValueStore.getGlobalCacheMisses());
        assertEquals(0, CachedKeyColumnValueStore.getGlobalCacheHits());
        tx.commit();

        tx = graph.buildTransaction().setMetricsPrefix(METRICS).start();
        v = tx.getVertex(v.getID());
        assertEquals(2,Iterables.size(v.getProperties()));
        verifyMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 12l));
        verifyMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyMetrics(STORE_NAMES.get(2));
        verifyMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 4l, M_GET_SLICE, 8l));
        assertEquals(7, CachedKeyColumnValueStore.getGlobalCacheMisses());
        assertEquals(4, CachedKeyColumnValueStore.getGlobalCacheHits());
        tx.commit();

        //Check type index lookup caching
        tx = graph.buildTransaction().setMetricsPrefix(METRICS).start();
        v = tx.getVertex(v.getID());
        assertNotNull(v.getProperty("age"));
        assertNotNull(v.getProperty("name"));
        verifyMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 19l));
        verifyMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 5l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyMetrics(STORE_NAMES.get(2));
        verifyMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 4l, M_GET_SLICE, 8l));
        assertEquals(9, CachedKeyColumnValueStore.getGlobalCacheMisses());
        assertEquals(8, CachedKeyColumnValueStore.getGlobalCacheHits());
        tx.commit();

        tx = graph.buildTransaction().setMetricsPrefix(METRICS).start();
        v = tx.getVertex(v.getID());
        Iterable<TitanRelation> relations = v.query().relations();
        Iterator<TitanRelation> relationsIter = relations.iterator();
        while (relationsIter.hasNext()) {
            relationsIter.next();
        }
        verifyMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 27l));
        verifyMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 5l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyMetrics(STORE_NAMES.get(2));
        verifyMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 4l, M_GET_SLICE, 8l));
        assertEquals(11, CachedKeyColumnValueStore.getGlobalCacheMisses());
        assertEquals(12, CachedKeyColumnValueStore.getGlobalCacheHits());
        tx.commit();
    }

    @Test
    public void testKCVSAccess2() throws InterruptedException {
        CachedKeyColumnValueStore.resetGlobalMetrics();
        METRICS = "metrics2";

        TitanTransaction tx = graph.buildTransaction().setMetricsPrefix(METRICS).start();
        TitanVertex parentVertex = tx.addVertex();
        parentVertex.setProperty("name", "vParent");
        parentVertex.setProperty("other-prop-key1", "other-prop-value1");
        parentVertex.setProperty("other-prop-key2", "other-prop-value2");

        TitanVertex parentVertex2 = tx.addVertex();
        parentVertex2.setProperty("name", "vParent2");
        parentVertex2.setProperty("other-prop-key1", "other-prop-value12");
        parentVertex2.setProperty("other-prop-key2", "other-prop-value22");

        tx.commit();
        verifyMetrics("edgeStore", ImmutableMap.of(M_MUTATE, 8l));
        verifyMetrics("vertexIndexStore", ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        assertEquals(3, CachedKeyColumnValueStore.getGlobalCacheMisses());
        assertEquals(0, CachedKeyColumnValueStore.getGlobalCacheHits());
        //==> 3 lookups in vertexIndex to see if types already exist, then 6 mutations (3+3 for lock) and 3 lock applications to create them
        //==> 8 mutations in edgeStore to create vertices and types
        //3 cache misses when doing the index lookup for the type names (since they are not yet defined)

        tx = graph.buildTransaction().setMetricsPrefix(METRICS).start();
        Iterable<TitanRelation> relations = ((TitanVertexQuery)tx.getVertex(parentVertex).query()).relations();
        Iterator<TitanRelation> relationsIter = relations.iterator();

        while (relationsIter.hasNext()) {
            relationsIter.next();
        }

        tx.commit();
        verifyMetrics("edgeStore", ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 8l));
        verifyMetrics("vertexIndexStore", ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        assertEquals(9, CachedKeyColumnValueStore.getGlobalCacheMisses());
        assertEquals(0, CachedKeyColumnValueStore.getGlobalCacheHits());
        //==> 8 edgeStore.getSlice (1 for vertex existence, 1 to retrieve all relations, 2 call per type (name+definition) for all 3 types)
        //==> of those, the 6 type related calls go through the cache which is empty at this point ==> 6 (additional) misses
        //all other stats remain unchanged

        tx = graph.buildTransaction().setMetricsPrefix(METRICS).start();
        relations = ((TitanVertexQuery)tx.getVertex(parentVertex2).query()).relations();
        Iterator<TitanRelation> relationsIter2 = relations.iterator();

        while (relationsIter2.hasNext()) {
            relationsIter2.next();
        }
        verifyMetrics("edgeStore", ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 16l));
        verifyMetrics("vertexIndexStore", ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        assertEquals(9, CachedKeyColumnValueStore.getGlobalCacheMisses());
        assertEquals(6, CachedKeyColumnValueStore.getGlobalCacheHits());
        //==> 8 edgeStore.getSlice (1 for vertex existence, 1 to retrieve all relations, 2 call per type (name+definition) for all 3 types)
        //==> of those, the 6 type related calls go through the cache which is loaded at this point ==> 6 cache hits, no misses
        //==> there are only 2 getSlice calls that hit the storage backend
        //all other stats remain unchanged
    }

    @Test
    public void checkFastPropertyTrue() {
        checkFastPropertyAndLocking(true);
    }

    @Test
    public void checkFastPropertyFalse() {
        checkFastPropertyAndLocking(false);
    }


    public void checkFastPropertyAndLocking(boolean fastProperty) {
        clopen(ImmutableMap.of("fast-property",fastProperty));
        CachedKeyColumnValueStore.resetGlobalMetrics();
        METRICS = "metrics3"+fastProperty;

        TitanTransaction tx = graph.buildTransaction().setMetricsPrefix(METRICS).start();
        tx.makeKey("name").dataType(String.class).single(TypeMaker.UniquenessConsistency.NO_LOCK).make();
        tx.makeKey("age").dataType(Integer.class).single(TypeMaker.UniquenessConsistency.NO_LOCK).make();
        tx.makeKey("uid").dataType(String.class).single(TypeMaker.UniquenessConsistency.NO_LOCK)
                .unique(TypeMaker.UniquenessConsistency.LOCK).indexed(Vertex.class).make();
        TitanVertex v = tx.addVertex(null);
        ElementHelper.setProperties(v, "uid", "v1", "age", 25, "name", "john");
        tx.commit();
        verifyMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 7l));
        verifyMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 4l, M_MUTATE, 7l, M_ACQUIRE_LOCK, 4l));
        assertEquals(3, CachedKeyColumnValueStore.getGlobalCacheMisses());
        assertEquals(0, CachedKeyColumnValueStore.getGlobalCacheHits());

        tx = graph.buildTransaction().setMetricsPrefix(METRICS).start();
        v = tx.getVertex(v.getID());
        v.setProperty("age",35);
        v.setProperty("name","johnny");
        tx.commit();
        if (fastProperty)
            verifyMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 8l));
        else
            verifyMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 7l));
        verifyMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 6l, M_MUTATE, 7l, M_ACQUIRE_LOCK, 4l));
        if (fastProperty)
            assertEquals(11, CachedKeyColumnValueStore.getGlobalCacheMisses());
        else
            assertEquals(9, CachedKeyColumnValueStore.getGlobalCacheMisses());
        assertEquals(0, CachedKeyColumnValueStore.getGlobalCacheHits());

        tx = graph.buildTransaction().setMetricsPrefix(METRICS).start();
        v = tx.getVertex(v.getID());
        v.setProperty("age",45);
        v.setProperty("name","johnnie");
        tx.commit();
        if (fastProperty)
            verifyMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 9l, M_GET_SLICE, 16l));
        else
            verifyMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 9l, M_GET_SLICE, 14l));
        verifyMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 8l, M_MUTATE, 7l, M_ACQUIRE_LOCK, 4l));
        if (fastProperty) {
            assertEquals(11, CachedKeyColumnValueStore.getGlobalCacheMisses());
            assertEquals(8, CachedKeyColumnValueStore.getGlobalCacheHits());
        } else {
            assertEquals(9, CachedKeyColumnValueStore.getGlobalCacheMisses());
            assertEquals(6, CachedKeyColumnValueStore.getGlobalCacheHits());
        }

        //Check no further locks on read all
        tx = graph.buildTransaction().setMetricsPrefix(METRICS).start();
        v = tx.getVertex(v.getID());
        for (TitanProperty p : v.getProperties()) {
            assertNotNull(p.getValue());
            assertNotNull(p.getPropertyKey());
        }
        tx.commit();
        verifyMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 8l, M_MUTATE, 7l, M_ACQUIRE_LOCK, 4l));

    }

    private String METRICS;

    public void verifyMetrics(String storeName) {
        verifyMetrics(storeName,new HashMap<String,Long>(0));
    }

    public void verifyMetrics(String storeName, Map<String,Long> operationCounts) {
        verifyMetrics(storeName, METRICS,operationCounts);
    }

    public void verifyMetrics(String storeName, String prefix, Map<String,Long> operationCounts) {
        for (String operation : OPERATION_NAMES) {
            Long count = operationCounts.get(operation);
            if (count==null) count = 0l;
            assertEquals("On "+storeName+"-"+operation,count.longValue(), metric.getCounter(prefix, storeName, operation, MetricInstrumentedStore.M_CALLS).getCount());
        }
    }

    public void printAllMetrics() {
        printAllMetrics(METRICS);
    }

    public void printAllMetrics(String prefix) {
        for (String store : STORE_NAMES) {
            System.out.println("######## Store: " + store + " (" + prefix + ")");
            for (String operation : MetricInstrumentedStore.OPERATION_NAMES) {
                System.out.println("-- Operation: " + operation);
                System.out.print("\t"); System.out.println(metric.getCounter(prefix, store, operation, MetricInstrumentedStore.M_CALLS).getCount());
                System.out.print("\t"); System.out.println(metric.getTimer(prefix, store, operation, MetricInstrumentedStore.M_TIME).getMeanRate());
                if (operation==MetricInstrumentedStore.M_GET_SLICE) {
                    System.out.print("\t"); System.out.println(metric.getCounter(prefix, store, operation, MetricInstrumentedStore.M_ENTRIES_COUNT).getCount());
                }
            }
        }
    }

}
