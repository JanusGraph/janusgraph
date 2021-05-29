// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricFilter;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.TestCategory;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.util.CacheMetricsAction;
import org.janusgraph.diskstorage.util.MetricInstrumentedStore;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertexLabel;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.util.stats.MetricManager;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.janusgraph.diskstorage.Backend.EDGESTORE_NAME;
import static org.janusgraph.diskstorage.Backend.INDEXSTORE_NAME;
import static org.janusgraph.diskstorage.Backend.LOCK_STORE_SUFFIX;
import static org.janusgraph.diskstorage.Backend.METRICS_CACHE_SUFFIX;
import static org.janusgraph.diskstorage.Backend.METRICS_STOREMANAGER_NAME;
import static org.janusgraph.diskstorage.util.MetricInstrumentedStore.M_ACQUIRE_LOCK;
import static org.janusgraph.diskstorage.util.MetricInstrumentedStore.M_GET_SLICE;
import static org.janusgraph.diskstorage.util.MetricInstrumentedStore.M_MUTATE;
import static org.janusgraph.diskstorage.util.MetricInstrumentedStore.OPERATION_NAMES;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.BASIC_METRICS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DB_CACHE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DB_CACHE_CLEAN_WAIT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DB_CACHE_TIME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDS_STORE_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.METRICS_MERGE_STORES;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.PROPERTY_PREFETCHING;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SCHEMA_CONSTRAINTS;
import static org.janusgraph.graphdb.database.cache.MetricInstrumentedSchemaCache.METRICS_NAME;
import static org.janusgraph.graphdb.database.cache.MetricInstrumentedSchemaCache.METRICS_RELATIONS;
import static org.janusgraph.graphdb.database.cache.MetricInstrumentedSchemaCache.METRICS_TYPENAME;
import static org.janusgraph.testutil.JanusGraphAssert.assertCount;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@Tag(TestCategory.SERIAL_TESTS)
public abstract class JanusGraphOperationCountingTest extends JanusGraphBaseTest {

    public MetricManager metric;
    public final String SYSTEM_METRICS  = GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT;

    public abstract WriteConfiguration getBaseConfiguration();

    public final boolean storeUsesConsistentKeyLocker() {
        return !this.features.hasLocking();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        WriteConfiguration config = getBaseConfiguration();
        ModifiableConfiguration modifiableConfiguration = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,config, BasicConfiguration.Restriction.NONE);
        modifiableConfiguration.set(BASIC_METRICS,true);
        modifiableConfiguration.set(METRICS_MERGE_STORES,false);
        modifiableConfiguration.set(PROPERTY_PREFETCHING,false);
        modifiableConfiguration.set(DB_CACHE, false);
        return config;
    }

    @Override
    public void open(WriteConfiguration config) {
        metric = MetricManager.INSTANCE;
        super.open(config);
    }


    private void verifyLockingOverwrite(long num) {
        if (storeUsesConsistentKeyLocker()) {
            verifyStoreMetrics(org.janusgraph.diskstorage.Backend.INDEXSTORE_NAME, ImmutableMap.of(M_GET_SLICE, 2*num));
            verifyStoreMetrics(org.janusgraph.diskstorage.Backend.INDEXSTORE_NAME +LOCK_STORE_SUFFIX, ImmutableMap.of(M_GET_SLICE, num, M_MUTATE, 2*num));
        } else {
            verifyStoreMetrics(org.janusgraph.diskstorage.Backend.INDEXSTORE_NAME, ImmutableMap.of(M_GET_SLICE, num, M_ACQUIRE_LOCK, num));
        }
    }

    @Test
    public void testIdCounts() {
        makeVertexIndexedUniqueKey("uid", Integer.class);
        mgmt.setConsistency(mgmt.getGraphIndex("uid"), ConsistencyModifier.LOCK);
        finishSchema();

        //Schema and relation id pools are tapped, Schema id pool twice because the renew is triggered. Each id acquisition requires 1 mutations and 2 reads
        verifyStoreMetrics(getConfig().get(IDS_STORE_NAME), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 3L, M_GET_SLICE, 6L));
    }


    @Test
    public void testReadOperations() {
        testReadOperations(false);
    }

    @Test
    public void testReadOperationsWithCache() {
        testReadOperations(true);
    }

    @SuppressWarnings("unchecked")
    public void testReadOperations(boolean cache) {
        metricsPrefix = "testReadOperations"+cache;

        resetEdgeCacheCounts();

        makeVertexIndexedUniqueKey("uid", Integer.class);
        mgmt.setConsistency(mgmt.getGraphIndex("uid"),ConsistencyModifier.LOCK);
        finishSchema();

        if (cache) clopen(option(DB_CACHE),true,option(DB_CACHE_CLEAN_WAIT),0,option(DB_CACHE_TIME),0);
        else clopen();

        JanusGraphTransaction tx = graph.buildTransaction().groupName(metricsPrefix).start();
        tx.makePropertyKey("name").dataType(String.class).make();
        tx.makeEdgeLabel("knows").make();
        tx.makeVertexLabel("person").make();
        tx.commit();
        verifyStoreMetrics(EDGESTORE_NAME);
        verifyLockingOverwrite(3);
        verifyStoreMetrics(METRICS_STOREMANAGER_NAME, ImmutableMap.of(M_MUTATE, 1L));

        resetMetrics();

        metricsPrefix=GraphDatabaseConfiguration.METRICS_SCHEMA_PREFIX_DEFAULT;

        resetMetrics();

        //Test schema caching
        for (int t=0;t<10;t++) {
            tx = graph.buildTransaction().groupName(metricsPrefix).start();
            //Retrieve name by index (one backend call each)
            assertTrue(tx.containsRelationType("name"));
            assertTrue(tx.containsRelationType("knows"));
            assertTrue(tx.containsVertexLabel("person"));
            PropertyKey name = tx.getPropertyKey("name");
            EdgeLabel knows = tx.getEdgeLabel("knows");
            VertexLabel person = tx.getVertexLabel("person");
            PropertyKey uid = tx.getPropertyKey("uid");
            //Retrieve name as property (one backend call each)
            assertEquals("name",name.name());
            assertEquals("knows",knows.name());
            assertEquals("person",person.name());
            assertEquals("uid",uid.name());
            //Looking up the definition (one backend call each)
            assertEquals(Cardinality.SINGLE,name.cardinality());
            assertEquals(Multiplicity.MULTI,knows.multiplicity());
            assertFalse(person.isPartitioned());
            assertEquals(Integer.class,uid.dataType());
            //Retrieving in and out relations for the relation types...
            InternalRelationType nameInternal = (InternalRelationType)name;
            InternalRelationType knowsInternal = (InternalRelationType)knows;
            InternalRelationType uidInternal = (InternalRelationType)uid;
            assertNull(nameInternal.getBaseType());
            assertNull(knowsInternal.getBaseType());
            IndexType index = Iterables.getOnlyElement(uidInternal.getKeyIndexes());
            assertEquals(1,index.getFieldKeys().length);
            assertEquals(ElementCategory.VERTEX,index.getElement());
            assertEquals(ConsistencyModifier.LOCK,((CompositeIndexType)index).getConsistencyModifier());
            assertEquals(1, Iterables.size(uidInternal.getRelationIndexes()));
            assertEquals(1, Iterables.size(nameInternal.getRelationIndexes()));
            assertEquals(nameInternal, Iterables.getOnlyElement(nameInternal.getRelationIndexes()));
            assertEquals(knowsInternal, Iterables.getOnlyElement(knowsInternal.getRelationIndexes()));
            //.. and vertex labels
            assertEquals(0,((InternalVertexLabel)person).getTTL());

            tx.commit();
            //Needs to read on first iteration, after that it doesn't change anymore
            verifyStoreMetrics(EDGESTORE_NAME, ImmutableMap.of(M_GET_SLICE, 19L));
            verifyStoreMetrics(INDEXSTORE_NAME,
                    ImmutableMap.of(M_GET_SLICE, 4L /* name, knows, person, uid */, M_ACQUIRE_LOCK, 0L));
        }

        //Create some graph data
        metricsPrefix = "add"+cache;

        tx = graph.buildTransaction().groupName(metricsPrefix).start();
        JanusGraphVertex v = tx.addVertex(), u = tx.addVertex("person");
        v.property(VertexProperty.Cardinality.single, "uid",  1);
        u.property(VertexProperty.Cardinality.single, "name",  "juju");
        Edge e = v.addEdge("knows",u);
        e.property("name", "edge");
        tx.commit();
        verifyStoreMetrics(EDGESTORE_NAME);
        verifyLockingOverwrite(1);

        for (int i = 1; i <= 30; i++) {
            metricsPrefix = "op"+i+cache;
            tx = graph.buildTransaction().groupName(metricsPrefix).start();
            v = getOnlyElement(tx.query().has("uid",1).vertices());
            assertEquals(1,v.<Integer>value("uid").intValue());
            u = getOnlyElement(v.query().direction(Direction.BOTH).labels("knows").vertices());
            e = getOnlyElement(u.query().direction(Direction.IN).labels("knows").edges());
            assertEquals("juju",u.value("name"));
            assertEquals("edge",e.value("name"));
            tx.commit();
            if (!cache) {
                verifyStoreMetrics(EDGESTORE_NAME, ImmutableMap.of(M_GET_SLICE, 4L));
                verifyStoreMetrics(INDEXSTORE_NAME, ImmutableMap.of(M_GET_SLICE, 1L));
            } else if (i > 20) { //Needs a couple of iterations for cache to be cleaned
                verifyStoreMetrics(EDGESTORE_NAME);
                verifyStoreMetrics(INDEXSTORE_NAME);
            }

        }


    }

    @Test
    public void testSettingProperty() {
        metricsPrefix = "testSettingProperty";

        mgmt.makePropertyKey("foo").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        finishSchema();

        clopen(option(SCHEMA_CONSTRAINTS), true);

        JanusGraphVertex v = tx.addVertex();
        v.property("foo","bar");
        tx.commit();


        JanusGraphTransaction tx = graph.buildTransaction().checkExternalVertexExistence(false).groupName(metricsPrefix).start();
        v = tx.getVertex(v.longId());
        v.property("foo", "bus");
        long numLookupPropertyConstraints = 1;
        //printAllMetrics(metricsPrefix);
        tx.commit();
        verifyStoreMetrics(EDGESTORE_NAME, ImmutableMap.of(M_GET_SLICE, numLookupPropertyConstraints));
        verifyStoreMetrics(INDEXSTORE_NAME);
        verifyStoreMetrics(METRICS_STOREMANAGER_NAME, ImmutableMap.of(M_MUTATE, 1L));

        tx = graph.buildTransaction().checkExternalVertexExistence(false).groupName(metricsPrefix).start();
        v = tx.getVertex(v.longId());
        v.property("foo", "band");
        numLookupPropertyConstraints +=1;
        assertEquals("band", v.property("foo").value());
        assertEquals(1, Iterators.size(v.properties("foo")));
        assertEquals(1, Iterators.size(v.properties()));
        tx.commit();
        verifyStoreMetrics(EDGESTORE_NAME, ImmutableMap.of(M_GET_SLICE, 2L + numLookupPropertyConstraints));
        verifyStoreMetrics(INDEXSTORE_NAME);
        verifyStoreMetrics(METRICS_STOREMANAGER_NAME, ImmutableMap.of(M_MUTATE, 2L));
        verifyStoreMetrics(getConfig().get(IDS_STORE_NAME));
    }


    @Test
    public void testKCVSAccess1() {
        metricsPrefix = "testKCVSAccess1";

        JanusGraphTransaction tx = graph.buildTransaction().groupName(metricsPrefix).start();
        JanusGraphVertex v = tx.addVertex("age", 25, "name", "john");
        JanusGraphVertex u = tx.addVertex("age", 35, "name", "mary");
        v.addEdge("knows", u);
        tx.commit();
        verifyStoreMetrics(EDGESTORE_NAME);
        verifyLockingOverwrite(3);
        verifyStoreMetrics(METRICS_STOREMANAGER_NAME, ImmutableMap.of(M_MUTATE, 1L + (features.hasTxIsolation()?0:1)));

        verifyTypeCacheMetrics(3, 0);

        //Check type name & definition caching
        tx = graph.buildTransaction().groupName(metricsPrefix).start();
        v = getV(tx,v);
        assertCount(2, v.properties());
        verifyStoreMetrics(EDGESTORE_NAME, ImmutableMap.of(M_GET_SLICE, 2L)); //1 verify vertex existence, 1 for query
        verifyTypeCacheMetrics(3, 4);
        tx.commit();

        tx = graph.buildTransaction().groupName(metricsPrefix).start();
        v = getV(tx,v);
        assertCount(2, v.properties());
        verifyStoreMetrics(EDGESTORE_NAME, ImmutableMap.of(M_GET_SLICE, 4L)); //1 verify vertex existence, 1 for query
        verifyTypeCacheMetrics(3, 4);
        tx.commit();

        //Check type index lookup caching
        tx = graph.buildTransaction().groupName(metricsPrefix).start();
        v = getV(tx,v);
        assertNotNull(v.value("age"));
        assertNotNull(v.value("name"));
        verifyStoreMetrics(EDGESTORE_NAME, ImmutableMap.of(M_GET_SLICE, 7L)); //1 verify vertex existence, 2 for query
        verifyTypeCacheMetrics(5, 8);
        tx.commit();

        tx = graph.buildTransaction().groupName(metricsPrefix).start();
        v = getV(tx,v);
        assertNotNull(v.value("age"));
        assertNotNull(v.value("name"));
        assertCount(1, v.query().direction(Direction.BOTH).edges());
        verifyStoreMetrics(EDGESTORE_NAME, ImmutableMap.of(M_GET_SLICE, 11L)); //1 verify vertex existence, 3 for query
        verifyTypeCacheMetrics(5, 10);
        tx.commit();

        verifyLockingOverwrite(3);
        verifyStoreMetrics(METRICS_STOREMANAGER_NAME, ImmutableMap.of(M_MUTATE, 1L + (features.hasTxIsolation()?0:1)));

    }

    @Test
    public void checkPropertyLockingAndIndex() {
        PropertyKey uid = makeKey("uid",String.class);
        JanusGraphIndex index = mgmt.buildIndex("uid",Vertex.class).unique().addKey(uid).buildCompositeIndex();
        mgmt.setConsistency(index, ConsistencyModifier.LOCK);
        mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.makePropertyKey("age").dataType(Integer.class).make();
        finishSchema();

        metricsPrefix = "checkPropertyLockingAndIndex";

        JanusGraphTransaction tx = graph.buildTransaction().groupName(metricsPrefix).start();
        JanusGraphVertex v = tx.addVertex("uid", "v1", "age", 25, "name", "john");
        assertEquals(25,v.property("age").value());
        tx.commit();
        verifyStoreMetrics(EDGESTORE_NAME);
        verifyLockingOverwrite(1);
        verifyStoreMetrics(METRICS_STOREMANAGER_NAME, ImmutableMap.of(M_MUTATE, 1L));

        resetMetrics();

        tx = graph.buildTransaction().groupName(metricsPrefix).start();
        v = Iterables.getOnlyElement(tx.query().has("uid", Cmp.EQUAL, "v1").vertices());
        assertEquals(25,v.property("age").value());
        tx.commit();
        verifyStoreMetrics(EDGESTORE_NAME, ImmutableMap.of(M_GET_SLICE, 1L));
        verifyStoreMetrics(INDEXSTORE_NAME, ImmutableMap.of(M_GET_SLICE, 1L));
        verifyStoreMetrics(METRICS_STOREMANAGER_NAME);
    }


    @Test
    public void checkFastPropertyTrue() {
        checkFastProperty(true);
    }

    @Test
    public void checkFastPropertyFalse() {
        checkFastProperty(false);
    }


    public void checkFastProperty(boolean fastProperty) {
        makeKey("uid",String.class);
        makeKey("name", String.class);
        makeKey("age", String.class);
        finishSchema();

        clopen(option(GraphDatabaseConfiguration.PROPERTY_PREFETCHING), fastProperty);
        metricsPrefix = "checkFastProperty"+fastProperty;

        JanusGraphTransaction tx = graph.buildTransaction().groupName(metricsPrefix).start();
        JanusGraphVertex v = tx.addVertex("uid", "v1", "age", 25, "name", "john");
        tx.commit();
        verifyStoreMetrics(EDGESTORE_NAME);
        verifyStoreMetrics(INDEXSTORE_NAME);
        verifyStoreMetrics(METRICS_STOREMANAGER_NAME, ImmutableMap.of(M_MUTATE, 1L));

        tx = graph.buildTransaction().groupName(metricsPrefix).start();
        v = getV(tx, v);
        assertEquals("v1",v.property("uid").value());
        assertEquals("25",v.property("age").value());
        assertEquals("john",v.property("name").value());
        tx.commit();
        if (fastProperty)
            verifyStoreMetrics(EDGESTORE_NAME, ImmutableMap.of(M_GET_SLICE, 2L));
        else
            verifyStoreMetrics(EDGESTORE_NAME, ImmutableMap.of(M_GET_SLICE, 4L));
        verifyStoreMetrics(INDEXSTORE_NAME);
        verifyStoreMetrics(METRICS_STOREMANAGER_NAME, ImmutableMap.of(M_MUTATE, 1L));
    }

    private String metricsPrefix;

    public void verifyStoreMetrics(String storeName) {
        verifyStoreMetrics(storeName, new HashMap<>(0));
    }

    public void verifyStoreMetrics(String storeName, Map<String, Long> operationCounts) {
        verifyStoreMetrics(storeName, metricsPrefix, operationCounts);
    }

    public void verifyStoreMetrics(String storeName, String prefix, Map<String, Long> operationCounts) {
        for (String operation : OPERATION_NAMES) {
            Long count = operationCounts.get(operation);
            if (count==null) count = 0L;
            assertEquals(count.longValue(), metric.getCounter(prefix, storeName, operation, MetricInstrumentedStore.M_CALLS).getCount(),
                Joiner.on(".").join(prefix, storeName, operation, MetricInstrumentedStore.M_CALLS));
        }
    }

    public void verifyTypeCacheMetrics(int nameMisses, int relationMisses) {
        verifyTypeCacheMetrics(metricsPrefix,nameMisses,relationMisses);
    }

    public void verifyTypeCacheMetrics(String prefix, int nameMisses, int relationMisses) {
//        assertEquals("On type cache name retrievals",nameRetrievals, metric.getCounter(GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT, METRICS_NAME, METRICS_TYPENAME, CacheMetricsAction.RETRIEVAL.getName()).getCount());
        assertEquals(nameMisses, metric.getCounter(GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT, METRICS_NAME, METRICS_TYPENAME, CacheMetricsAction.MISS.getName()).getCount(),
            "On type cache name misses");
        assertTrue(nameMisses <= metric.getCounter(GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT, METRICS_NAME, METRICS_TYPENAME, CacheMetricsAction.RETRIEVAL.getName()).getCount());
//        assertEquals("On type cache relation retrievals",relationRetrievals, metric.getCounter(GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT, METRICS_NAME, METRICS_RELATIONS, CacheMetricsAction.RETRIEVAL.getName()).getCount());
        assertEquals(relationMisses, metric.getCounter(GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT, METRICS_NAME, METRICS_RELATIONS, CacheMetricsAction.MISS.getName()).getCount(),
            "On type cache relation misses");
        assertTrue(relationMisses <= metric.getCounter(GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT, METRICS_NAME, METRICS_RELATIONS, CacheMetricsAction.RETRIEVAL.getName()).getCount());
    }

    public void printAllMetrics(String prefix) {
        List<String> storeNames = new ArrayList<>();
        storeNames.add(EDGESTORE_NAME);
        storeNames.add(INDEXSTORE_NAME);
        storeNames.add(getConfig().get(IDS_STORE_NAME));
        storeNames.add(METRICS_STOREMANAGER_NAME);
        if (storeUsesConsistentKeyLocker()) {
            storeNames.add(EDGESTORE_NAME+LOCK_STORE_SUFFIX);
            storeNames.add(INDEXSTORE_NAME+LOCK_STORE_SUFFIX);
        }

        for (String store : storeNames) {
            System.out.println("######## Store: " + store + " (" + prefix + ")");
            for (String operation : MetricInstrumentedStore.OPERATION_NAMES) {
                System.out.println("-- Operation: " + operation);
                System.out.print("\t"); System.out.println(metric.getCounter(prefix, store, operation, MetricInstrumentedStore.M_CALLS).getCount());
                System.out.print("\t"); System.out.println(metric.getTimer(prefix, store, operation, MetricInstrumentedStore.M_TIME).getMeanRate());
                if (operation.equals(M_GET_SLICE)) {
                    System.out.print("\t"); System.out.println(metric.getCounter(prefix, store, operation, MetricInstrumentedStore.M_ENTRIES_COUNT).getCount());
                }
            }
        }
    }

    @Test
    public void testCacheConcurrency() throws InterruptedException {
        metricsPrefix = "tCC";
        Object[] newConfig = {option(GraphDatabaseConfiguration.DB_CACHE),true,
                option(GraphDatabaseConfiguration.DB_CACHE_TIME),0,
                option(GraphDatabaseConfiguration.DB_CACHE_CLEAN_WAIT),0,
                option(GraphDatabaseConfiguration.DB_CACHE_SIZE),0.25,
                option(GraphDatabaseConfiguration.BASIC_METRICS),true,
                option(GraphDatabaseConfiguration.METRICS_MERGE_STORES),false,
                option(GraphDatabaseConfiguration.METRICS_PREFIX),metricsPrefix,
                option(GraphDatabaseConfiguration.SCHEMA_CONSTRAINTS),true};
        clopen(newConfig);
        final String prop = "someProp";
        makeKey(prop,Integer.class);
        finishSchema();

        final int numV = 100;
        final long[] vertexIds = new long[numV];
        for (int i=0;i<numV;i++) {
            JanusGraphVertex v = graph.addVertex(prop,0);
            graph.tx().commit();
            vertexIds[i]=getId(v);
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

        Thread reader = new Thread(() -> {
            int reads = 0;
            while (reads<numReads) {
                final int pos = random.nextInt(vertexIds.length);
                final long vid = vertexIds[pos];
                JanusGraphVertex v = getV(graph,vid);
                assertNotNull(v);
                boolean postCommit = postcommit[pos].get();
                final Integer value = v.value(prop);
                lookups.incrementAndGet();
                assertNotNull(value, "On pos [" + pos + "]");
                if (!precommit[pos].get()) assertEquals(0, value.intValue());
                else if (postCommit) assertEquals(1, value.intValue());
                graph.tx().commit();
                try {
                    Thread.sleep(readSleepTime);
                } catch (InterruptedException e) {
                    return;
                }
                reads++;
            }
        });
        reader.start();

        Thread updater = new Thread(() -> {
            for (int i=0;i<numV;i++) {
                try {
                    final JanusGraphVertex v = getV(graph,vertexIds[i]);
                    v.property(VertexProperty.Cardinality.single, prop, 1);
                    precommit[i].set(true);
                    graph.tx().commit();
                    postcommit[i].set(true);
                    Thread.sleep(updateSleepTime);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Unexpected interruption",e);
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
        assertEquals(numReads, lookups.get());
        final int numLookupPropertyConstraints = numV;
        assertEquals(2 * numReads + numV + numLookupPropertyConstraints, getEdgeCacheRetrievals());
        int minMisses = 2*numV;
        assertTrue(minMisses<=getEdgeCacheMisses() && 4*minMisses>=getEdgeCacheMisses(), "Min misses ["+minMisses+"] vs actual ["+getEdgeCacheMisses()+"]");
    }

    private long getEdgeCacheRetrievals() {
        return metric.getCounter(metricsPrefix, EDGESTORE_NAME + METRICS_CACHE_SUFFIX, CacheMetricsAction.RETRIEVAL.getName()).getCount();
    }

    private long getEdgeCacheMisses() {
        return metric.getCounter(metricsPrefix, EDGESTORE_NAME + METRICS_CACHE_SUFFIX, CacheMetricsAction.MISS.getName()).getCount();
    }

    private void resetEdgeCacheCounts() {
        Counter counter = metric.getCounter(metricsPrefix, EDGESTORE_NAME + METRICS_CACHE_SUFFIX, CacheMetricsAction.RETRIEVAL.getName());
        counter.dec(counter.getCount());
        counter = metric.getCounter(metricsPrefix, EDGESTORE_NAME + METRICS_CACHE_SUFFIX, CacheMetricsAction.MISS.getName());
        counter.dec(counter.getCount());
    }

    protected void resetMetrics() {
        MetricManager.INSTANCE.getRegistry().removeMatching(MetricFilter.ALL);
    }

    /**
     * Tests cache performance
     */
    @Test
    public void testCacheSpeedup() {
        Object[] newConfig = {option(GraphDatabaseConfiguration.DB_CACHE),true,
                option(GraphDatabaseConfiguration.DB_CACHE_TIME),0};
        clopen(newConfig);

        int numV = 1000;

        JanusGraphVertex previous = null;
        for (int i=0;i<numV;i++) {
            JanusGraphVertex v = graph.addVertex("name", "v" + i);
            if (previous!=null)
                v.addEdge("knows",previous);
            previous = v;
        }
        graph.tx().commit();
        long vertexId = getId(previous);
        assertCount(numV, graph.query().vertices());

        clopen(newConfig);

        double timeColdGlobal=0, timeWarmGlobal=0, timeHotGlobal=0;

        // measurements must be less than outerRepeat
        int outerRepeat = 20;
        int measurements = 10;
        int innerRepeat = 2;
        for (int c=0;c<outerRepeat;c++) {

            double timeCold = testAllVertices(vertexId,numV);

            double timeWarm = 0;
            double timeHot = 0;
            for (int i = 0;i<innerRepeat;i++) {
                graph.tx().commit();
                timeWarm += testAllVertices(vertexId,numV);
                for (int j=0;j<innerRepeat;j++) {
                    timeHot += testAllVertices(vertexId,numV);
                }
            }
            timeWarm = timeWarm / innerRepeat;
            timeHot = timeHot / (innerRepeat*innerRepeat);

            if (c>=(outerRepeat-measurements)) {
                timeColdGlobal += timeCold;
                timeWarmGlobal += timeWarm;
                timeHotGlobal  += timeHot;
            }
//            System.out.println(timeCold + "\t" + timeWarm + "\t" + timeHot);
            clopen(newConfig);
        }
        timeColdGlobal = timeColdGlobal/measurements;
        timeWarmGlobal = timeWarmGlobal/measurements;
        timeHotGlobal = timeHotGlobal/measurements;

        System.out.println(round(timeColdGlobal) + "\t" + round(timeWarmGlobal) + "\t" + round(timeHotGlobal));
        assertTrue(timeColdGlobal>timeWarmGlobal*2, timeColdGlobal + " vs " + timeWarmGlobal);
        //assertTrue(timeWarmGlobal + " vs " + timeHotGlobal, timeWarmGlobal>timeHotGlobal); Sometimes, this is not true
    }

    private double testAllVertices(long vid, int numV) {
        long start = System.nanoTime();
        JanusGraphVertex v = getV(graph,vid);
        for (int i=1; i<numV; i++) {
            v = Iterables.getOnlyElement(v.query().direction(Direction.OUT).labels("knows").vertices());
        }
        return ((System.nanoTime()-start)/1000000.0);
    }

}
