package com.thinkaurelius.titan.graphdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.util.CacheMetricsAction;
import com.thinkaurelius.titan.diskstorage.util.MetricInstrumentedStore;
import static com.thinkaurelius.titan.diskstorage.util.MetricInstrumentedStore.*;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import static com.thinkaurelius.titan.graphdb.database.cache.MetricInstrumentedSchemaCache.*;
import com.thinkaurelius.titan.testcategory.SerialTests;
import com.thinkaurelius.titan.util.stats.MetricManager;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@Category({ SerialTests.class })
public abstract class TitanNonTransactionalGraphMetricsTest extends TitanGraphBaseTest {

//    public StandardTitanGraph graph;
//    public StoreFeatures features;


    public MetricManager metric;
    public final String SYSTEM_METRICS  = GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT;

    public abstract WriteConfiguration getBaseConfiguration();

    @Override
    public WriteConfiguration getConfiguration() {
        WriteConfiguration config = getBaseConfiguration();
        ModifiableConfiguration mconf = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,config, BasicConfiguration.Restriction.NONE);
        mconf.set(GraphDatabaseConfiguration.BASIC_METRICS,true);
        mconf.set(GraphDatabaseConfiguration.METRICS_MERGE_STORES,false);
        mconf.set(GraphDatabaseConfiguration.PROPERTY_PREFETCHING,false);
        mconf.set(GraphDatabaseConfiguration.DB_CACHE,false);
        return config;
    }

    @Override
    public void open(WriteConfiguration config) {
        metric = MetricManager.INSTANCE;
        super.open(config);
    }


    public static final List<String> STORE_NAMES =
            ImmutableList.of("edgeStore", "vertexIndexStore", "edgeIndexStore", "idStore");

    @Test
    @Ignore //TODO: Ignore for now until everything is stable - then do the counting
    public void testKCVSAccess1() throws InterruptedException {
        metricsPrefix = "metrics1";

        TitanTransaction tx = graph.buildTransaction().setGroupName(metricsPrefix).start();
        TitanVertex v = tx.addVertex();
        verifyStoreMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 2l, M_GET_SLICE, 4l));
        ElementHelper.setProperties(v, "age", 25, "name", "john");
        TitanVertex u = tx.addVertex();
        ElementHelper.setProperties(u, "age", 35, "name", "mary");
        v.addEdge("knows", u);
        tx.commit();
//        printAllMetrics();
//        printAllMetrics(SYSTEM_METRICS);
        verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l));
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyStoreMetrics(STORE_NAMES.get(2));
        Thread.sleep(500);
        verifyStoreMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 4l, M_GET_SLICE, 8l));
        verifyTypeCacheMetrics(3, 3, 0, 0);

        //Check type name & definition caching
        tx = graph.buildTransaction().setGroupName(metricsPrefix).start();
        v = tx.getVertex(v.getID());
        assertEquals(2,Iterables.size(v.getProperties()));
        verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 4l)); //1 verify vertex existence, 1 for query, 1 for each of the 2 types (Definition)
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyStoreMetrics(STORE_NAMES.get(2));
        verifyStoreMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 4l, M_GET_SLICE, 8l));
        verifyTypeCacheMetrics(3, 3, 2, 2);
        tx.commit();

        tx = graph.buildTransaction().setGroupName(metricsPrefix).start();
        v = tx.getVertex(v.getID());
        assertEquals(2, Iterables.size(v.getProperties()));
        verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 6l));
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyStoreMetrics(STORE_NAMES.get(2));
        verifyStoreMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 4l, M_GET_SLICE, 8l));
        verifyTypeCacheMetrics(3, 3, 4, 2);
        tx.commit();

        //Check type index lookup caching
        tx = graph.buildTransaction().setGroupName(metricsPrefix).start();
        v = tx.getVertex(v.getID());
        assertNotNull(v.getProperty("age"));
        assertNotNull(v.getProperty("name"));
        verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 11l));
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 5l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyStoreMetrics(STORE_NAMES.get(2));
        verifyStoreMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 4l, M_GET_SLICE, 8l));
        verifyTypeCacheMetrics(9, 5, 8, 4);
        tx.commit();

        tx = graph.buildTransaction().setGroupName(metricsPrefix).start();
        v = tx.getVertex(v.getID());
        assertEquals(1,Iterables.size(v.getEdges(Direction.BOTH)));
        assertEquals(2, Iterables.size(v.getProperties()));
        verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 15l));
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 5l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyStoreMetrics(STORE_NAMES.get(2));
        verifyStoreMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 4l, M_GET_SLICE, 8l));
        verifyTypeCacheMetrics(9, 5, 11, 5);
        tx.commit();
    }

    @Test
    @Ignore //TODO: Ignore for now until everything is stable - then do the counting
    public void testKCVSAccess2() throws InterruptedException {
        metricsPrefix = "metrics2";

        TitanTransaction tx = graph.buildTransaction().setGroupName(metricsPrefix).start();
        TitanVertex parentVertex = tx.addVertex();
        parentVertex.setProperty("name", "vParent");
        parentVertex.setProperty("other-prop-key1", "other-prop-value1");
        parentVertex.setProperty("other-prop-key2", "other-prop-value2");

        TitanVertex parentVertex2 = tx.addVertex();
        parentVertex2.setProperty("name", "vParent2");
        parentVertex2.setProperty("other-prop-key1", "other-prop-value12");
        parentVertex2.setProperty("other-prop-key2", "other-prop-value22");

        tx.commit();
        verifyStoreMetrics("edgeStore", ImmutableMap.of(M_MUTATE, 8l));
        verifyStoreMetrics("vertexIndexStore", ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyTypeCacheMetrics(3, 3, 0, 0);
        //==> 3 lookups in vertexIndex to see if types already exist, then 6 mutations (3+3 for lock) and 3 lock applications to create them
        //==> 8 mutations in edgeStore to create vertices and types
        //3 cache misses when doing the index lookup for the type names (since they are not yet defined)

        tx = graph.buildTransaction().setGroupName(metricsPrefix).start();
        assertEquals(3,Iterables.size(tx.getVertex(parentVertex.getID()).getProperties()));
        tx.commit();
        verifyStoreMetrics("edgeStore", ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 5l));
        verifyStoreMetrics("vertexIndexStore", ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyTypeCacheMetrics(3, 3, 3, 3);
        //==> 5 edgeStore.getSlice (1 for vertex existence, 1 to retrieve all relations, 1 call per type (name+definition) for all 3 types)
        //==> of those, the 3 type related calls go through the cache which is empty at this point ==> 3 (additional) misses
        //all other stats remain unchanged

        tx = graph.buildTransaction().setGroupName(metricsPrefix).start();
        assertEquals(3,Iterables.size(tx.getVertex(parentVertex.getID()).getProperties()));
        verifyStoreMetrics("edgeStore", ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 7l));
        verifyStoreMetrics("vertexIndexStore", ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyTypeCacheMetrics(3, 3, 6, 3);
        //==> 2 edgeStore.getSlice (1 for vertex existence, 1 to retrieve all relations)
        //==> of those, the 3 type related calls go through the cache which is loaded at this point ==> 3 cache hits, no misses
        //==> there are only 2 getSlice calls that hit the storage backend
        //all other stats remain unchanged
    }

    @Test
    @Ignore //TODO: Ignore for now until everything is stable - then do the counting
    public void checkFastPropertyTrue() {
        checkFastPropertyAndLocking(true);
    }

    @Test
    @Ignore //TODO: Ignore for now until everything is stable - then do the counting
    public void checkFastPropertyFalse() {
        checkFastPropertyAndLocking(false);
    }


    public void checkFastPropertyAndLocking(boolean fastProperty) {
        PropertyKey uid = makeKey("uid",String.class);
        TitanGraphIndex index = mgmt.buildIndex("uid",Vertex.class).unique().indexKey(uid).buildInternalIndex();
        mgmt.setConsistency(index, ConsistencyModifier.LOCK);
        finishSchema();

        clopen(option(GraphDatabaseConfiguration.PROPERTY_PREFETCHING), fastProperty);
        metricsPrefix = "metrics3"+fastProperty;

        TitanTransaction tx = graph.buildTransaction().setGroupName(metricsPrefix).start();
        tx.makePropertyKey("name").dataType(String.class).make();
        tx.makePropertyKey("age").dataType(Integer.class).make();
        TitanVertex v = tx.addVertex();
        ElementHelper.setProperties(v, "uid", "v1", "age", 25, "name", "john");
        tx.commit();
        verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 7l));
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 4l, M_MUTATE, 7l, M_ACQUIRE_LOCK, 3l));
        verifyTypeCacheMetrics(0, 0, 0, 0);

        tx = graph.buildTransaction().setGroupName(metricsPrefix).start();
        v = tx.getVertex(v.getID());
        v.setProperty("age",35);
        v.setProperty("name","johnny");
        tx.commit();
        if (fastProperty)
            verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 7l));
        else
            verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 7l));
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 6l, M_MUTATE, 7l, M_ACQUIRE_LOCK, 4l));
        if (fastProperty)
            verifyTypeCacheMetrics(6, 2, 5, 5);
        else
            verifyTypeCacheMetrics(6, 2, 4, 4);

        tx = graph.buildTransaction().setGroupName(metricsPrefix).start();
        v = tx.getVertex(v.getID());
        v.setProperty("age",45);
        v.setProperty("name","johnnie");
        tx.commit();
        if (fastProperty)
            verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 9l, M_GET_SLICE, 9l));
        else
            verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 9l, M_GET_SLICE, 10l));
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 6l, M_MUTATE, 7l, M_ACQUIRE_LOCK, 4l));
        if (fastProperty) {
            verifyTypeCacheMetrics(12, 2, 10, 5);
        } else {
            verifyTypeCacheMetrics(12, 2, 8, 4);
        }

        //Check no further locks on read all
        tx = graph.buildTransaction().setGroupName(metricsPrefix).start();
        v = tx.getVertex(v.getID());
        for (TitanProperty p : v.getProperties()) {
            assertNotNull(p.getValue());
            assertNotNull(p.getPropertyKey());
        }
        tx.commit();
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 6l, M_MUTATE, 7l, M_ACQUIRE_LOCK, 4l));

    }

    private String metricsPrefix;

    public void verifyStoreMetrics(String storeName) {
        verifyStoreMetrics(storeName, new HashMap<String, Long>(0));
    }

    public void verifyStoreMetrics(String storeName, Map<String, Long> operationCounts) {
        verifyStoreMetrics(storeName, metricsPrefix, operationCounts);
    }

    public void verifyStoreMetrics(String storeName, String prefix, Map<String, Long> operationCounts) {
        for (String operation : OPERATION_NAMES) {
            Long count = operationCounts.get(operation);
            if (count==null) count = 0l;
            assertEquals("On "+storeName+"-"+operation,count.longValue(), metric.getCounter(prefix, storeName, operation, MetricInstrumentedStore.M_CALLS).getCount());
        }
    }

    public void verifyTypeCacheMetrics(int nameRetrievals, int nameMisses, int relationRetrievals, int relationMisses) {
        verifyTypeCacheMetrics(metricsPrefix,nameRetrievals,nameMisses,relationRetrievals,relationMisses);
    }

    public void verifyTypeCacheMetrics(String prefix, int nameRetrievals, int nameMisses, int relationRetrievals, int relationMisses) {
        assertEquals("On type cache name retrievals",nameRetrievals, metric.getCounter(prefix, METRICS_NAME, METRICS_TYPENAME, CacheMetricsAction.RETRIEVAL.getName()).getCount());
        assertEquals("On type cache name misses",nameMisses, metric.getCounter(prefix, METRICS_NAME, METRICS_TYPENAME, CacheMetricsAction.MISS.getName()).getCount());
        assertEquals("On type cache relation retrievals",relationRetrievals, metric.getCounter(prefix, METRICS_NAME, METRICS_RELATIONS, CacheMetricsAction.RETRIEVAL.getName()).getCount());
        assertEquals("On type cache relation misses", relationMisses, metric.getCounter(prefix, METRICS_NAME, METRICS_RELATIONS, CacheMetricsAction.MISS.getName()).getCount());
    }

//    public void verifyCacheMetrics(String storeName) {
//        verifyCacheMetrics(storeName,0,0);
//    }
//
//    public void verifyCacheMetrics(String storeName, int misses, int retrievals) {
//        verifyCacheMetrics(storeName, metricsPrefix, misses, retrievals);
//    }
//
//    public void verifyCacheMetrics(String storeName, String prefix, int misses, int retrievals) {
//        assertEquals("On "+storeName+"-cache retrievals",retrievals, metric.getCounter(prefix, storeName + Backend.METRICS_CACHE_SUFFIX, CacheMetricsAction.RETRIEVAL.getName()).getCount());
//        assertEquals("On "+storeName+"-cache misses",misses, metric.getCounter(prefix, storeName + Backend.METRICS_CACHE_SUFFIX, CacheMetricsAction.MISS.getName()).getCount());
//    }

    public void printAllMetrics() {
        printAllMetrics(metricsPrefix);
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
