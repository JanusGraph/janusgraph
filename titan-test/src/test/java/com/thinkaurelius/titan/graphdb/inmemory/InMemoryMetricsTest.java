package com.thinkaurelius.titan.graphdb.inmemory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.CachedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.util.MetricInstrumentedStore;
import static com.thinkaurelius.titan.diskstorage.util.MetricInstrumentedStore.*;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.util.stats.MetricManager;
import com.tinkerpop.blueprints.util.ElementHelper;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class InMemoryMetricsTest {

    public StandardTitanGraph graph;
    public MetricManager metric;

    public final String METRICS = "metrics";
    public final String SYSTEM_METRICS  = GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT;

    public static final Configuration getConfiguration() {
        Configuration config = new BaseConfiguration();
        Configuration storeconfig = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        storeconfig.setProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "inmemory");
        storeconfig.setProperty(GraphDatabaseConfiguration.BASIC_METRICS,true);
        storeconfig.setProperty(GraphDatabaseConfiguration.MERGE_BASIC_METRICS_KEY,false);
        return config;
    }

    @Before
    public void before() {
        graph = (StandardTitanGraph)TitanFactory.open(getConfiguration());
        metric = MetricManager.INSTANCE;
        CachedKeyColumnValueStore.resetGlobalMetrics();
    }

    @After
    public void close() {
        graph.shutdown();
    }

    public static final List<String> STORE_NAMES =
            ImmutableList.of("edgeStore", "vertexIndexStore", "edgeIndexStore", "idStore");

    @Test
    public void testKCVSAccess() throws InterruptedException {
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
        assertNotNull(v.getProperty("age"));
        assertNotNull(v.getProperty("name"));
        verifyMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 26l));
        verifyMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 7l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyMetrics(STORE_NAMES.get(2));
        verifyMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 4l, M_GET_SLICE, 8l));
        assertEquals(9, CachedKeyColumnValueStore.getGlobalCacheMisses());
        assertEquals(14, CachedKeyColumnValueStore.getGlobalCacheHits());
        tx.commit();
    }

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
            assertEquals(count.longValue(), metric.getCounter(prefix, storeName, operation, MetricInstrumentedStore.M_CALLS).getCount());
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
