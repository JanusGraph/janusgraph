package com.thinkaurelius.titan.hadoop;

import static com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_KEYSPACE;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.CONNECTION_TIMEOUT;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.LOG_BACKEND;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.LOG_READ_INTERVAL;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.LOG_SEND_DELAY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.MANAGEMENT_LOG;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.PAGE_SIZE;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_CONF_FILE;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.TRANSACTION_LOG;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.TRIGGER_LOG;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.buildConfiguration;
import static com.thinkaurelius.titan.graphdb.internal.RelationCategory.PROPERTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.QueryDescription;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.TitanVertexQuery;
import com.thinkaurelius.titan.core.schema.SchemaAction;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingStore;
import com.thinkaurelius.titan.diskstorage.log.Log;
import com.thinkaurelius.titan.diskstorage.log.LogManager;
import com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLog;
import com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLogManager;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.graphdb.TitanGraphBaseTest;
import com.thinkaurelius.titan.graphdb.TitanGraphBaseTest.TestConfigOption;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.internal.OrderList;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.query.StandardQueryDescription;
import com.thinkaurelius.titan.graphdb.query.vertex.BasicVertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.types.StandardEdgeLabelMaker;
import com.thinkaurelius.titan.hadoop.formats.titan.cassandra.TitanCassandraOutputFormat;
import com.thinkaurelius.titan.testutil.TestGraphConfigs;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

public class CassandraReindexTest extends TitanGraphBaseTest {

    @Test
    public void testIndexUpdatesWithoutReindex() throws Exception {

        clopen( option(LOG_SEND_DELAY,MANAGEMENT_LOG),new StandardDuration(0,TimeUnit.MILLISECONDS),
                option(KCVSLog.LOG_READ_LAG_TIME,MANAGEMENT_LOG),new StandardDuration(50,TimeUnit.MILLISECONDS),
                option(LOG_READ_INTERVAL,MANAGEMENT_LOG),new StandardDuration(250,TimeUnit.MILLISECONDS)
        );
        //Types without index
        PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).make();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SET).make();
        PropertyKey sensor = mgmt.makePropertyKey("sensor").dataType(Double.class).cardinality(Cardinality.LIST).make();
        finishSchema();

        //Add some sensor data
        TitanVertex v = tx.addVertex();
        for (int i=0;i<10;i++) {
            v.addProperty("sensor",i).setProperty("time",i);
            v.addProperty("name","v"+i);
        }
        newTx();
        //Indexes should not yet be in use
        v = tx.getVertex(v.getLongId());
        evaluateQuery(v.query().keys("sensor").interval("time", 1, 5).orderBy("time",Order.DESC),
                PROPERTY,4,1,new boolean[]{false,false},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 101, 105).orderBy("time",Order.DESC),
                PROPERTY,0,1,new boolean[]{false,false},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(tx.query().has("name","v5"),
                ElementCategory.VERTEX,1,new boolean[]{false,true});
        evaluateQuery(tx.query().has("name","v105"),
                ElementCategory.VERTEX,0,new boolean[]{false,true});
        newTx();

        //Create indexes after the fact
        finishSchema();
        sensor = mgmt.getPropertyKey("sensor");
        time = mgmt.getPropertyKey("time");
        name = mgmt.getPropertyKey("name");
        mgmt.createPropertyIndex(sensor,"byTime",Order.DESC,time);
        mgmt.buildIndex("bySensorReading",Vertex.class).addKey(name).buildCompositeIndex();
        finishSchema();
        newTx();
        //Add some sensor data that should already be indexed even though index is not yet enabled
        v = tx.getVertex(v.getLongId());
        for (int i=100;i<110;i++) {
            v.addProperty("sensor",i).setProperty("time",i);
            v.addProperty("name","v"+i);
        }
        tx.commit();
        //Should not yet be able to enable since not yet registered
        try {
            mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType("sensor"),"byTime"), SchemaAction.ENABLE_INDEX);
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            mgmt.updateIndex(mgmt.getGraphIndex("bySensorReading"), SchemaAction.ENABLE_INDEX);
            fail();
        } catch (IllegalArgumentException e) {}
        mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType("sensor"),"byTime"), SchemaAction.REGISTER_INDEX);
        mgmt.updateIndex(mgmt.getGraphIndex("bySensorReading"), SchemaAction.REGISTER_INDEX);
        mgmt.commit();


        Thread.sleep(2000);
        finishSchema();
        mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType("sensor"),"byTime"), SchemaAction.ENABLE_INDEX);
        mgmt.updateIndex(mgmt.getGraphIndex("bySensorReading"), SchemaAction.ENABLE_INDEX);
        finishSchema();

        //Add some more sensor data
        newTx();
        v = tx.getVertex(v.getLongId());
        for (int i=200;i<210;i++) {
            v.addProperty("sensor",i).setProperty("time",i);
            v.addProperty("name","v"+i);
        }
        newTx();
        //Use indexes now but only see new data
        v = tx.getVertex(v.getLongId());
        evaluateQuery(v.query().keys("sensor").interval("time", 1, 5).orderBy("time",Order.DESC),
                PROPERTY,0,1,new boolean[]{true,true},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 101, 105).orderBy("time",Order.DESC),
                PROPERTY,4,1,new boolean[]{true,true},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 201, 205).orderBy("time",Order.DESC),
                PROPERTY,4,1,new boolean[]{true,true},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(tx.query().has("name","v5"),
                ElementCategory.VERTEX,0,new boolean[]{true,true},"bySensorReading");
        evaluateQuery(tx.query().has("name","v105"),
                ElementCategory.VERTEX,1,new boolean[]{true,true},"bySensorReading");
        evaluateQuery(tx.query().has("name","v205"),
                ElementCategory.VERTEX,1,new boolean[]{true,true},"bySensorReading");

        // Run a reindex job
        Properties titanInputProperties = new Properties();
        titanInputProperties.setProperty("storage.backend", "cassandrathrift");
        String ks = getClass().getSimpleName();
        titanInputProperties.setProperty("storage.keyspace", cleanKeyspaceName(ks));
        //TitanIndexRepair.cassandraRepair(titanInputProperties, "byTime", "sensor", "org.apache.cassandra.dht.Murmur3Partitioner");
        TitanIndexRepair.cassandraRepair(titanInputProperties, "bySensorReading", "", "org.apache.cassandra.dht.Murmur3Partitioner");

        v = tx.getVertex(v.getLongId());
        evaluateQuery(v.query().keys("sensor").interval("time", 1, 5).orderBy("time",Order.DESC),
                PROPERTY,0,1,new boolean[]{true,true},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 101, 105).orderBy("time",Order.DESC),
                PROPERTY,4,1,new boolean[]{true,true},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 201, 205).orderBy("time",Order.DESC),
                PROPERTY,4,1,new boolean[]{true,true},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(tx.query().has("name","v5"),
                ElementCategory.VERTEX,0,new boolean[]{true,true},"bySensorReading");
        evaluateQuery(tx.query().has("name","v105"),
                ElementCategory.VERTEX,1,new boolean[]{true,true},"bySensorReading");
        evaluateQuery(tx.query().has("name","v205"),
                ElementCategory.VERTEX,1,new boolean[]{true,true},"bySensorReading");
    }

    public static void evaluateQuery(TitanGraphQuery query, ElementCategory resultType,
            int expectedResults, boolean[] subQuerySpecs,
            PropertyKey orderKey1, Order order1,
            String... intersectingIndexes) {
        evaluateQuery(query,resultType,expectedResults,subQuerySpecs,ImmutableMap.of(orderKey1,order1),intersectingIndexes);
    }

    public static void evaluateQuery(TitanGraphQuery query, ElementCategory resultType,
            int expectedResults, boolean[] subQuerySpecs,
            PropertyKey orderKey1, Order order1, PropertyKey orderKey2, Order order2,
            String... intersectingIndexes) {
        evaluateQuery(query,resultType,expectedResults,subQuerySpecs,ImmutableMap.of(orderKey1,order1,orderKey2,order2),intersectingIndexes);
    }

    public static void evaluateQuery(TitanGraphQuery query, ElementCategory resultType,
            int expectedResults, boolean[] subQuerySpecs,
            String... intersectingIndexes) {
        evaluateQuery(query,resultType,expectedResults,subQuerySpecs,ImmutableMap.<PropertyKey,Order>of(),intersectingIndexes);
    }

    public static void evaluateQuery(TitanVertexQuery query, RelationCategory resultType,
                               int expectedResults, int numSubQueries, boolean[] subQuerySpecs) {
        evaluateQuery(query,resultType,expectedResults,numSubQueries, subQuerySpecs, ImmutableMap.<PropertyKey,Order>of());
    }

    public static void evaluateQuery(TitanVertexQuery query, RelationCategory resultType,
                               int expectedResults, int numSubQueries, boolean[] subQuerySpecs,
                               PropertyKey orderKey, Order order) {
        evaluateQuery(query,resultType,expectedResults,numSubQueries, subQuerySpecs, ImmutableMap.of(orderKey,order));
    }


    public static void evaluateQuery(TitanGraphQuery query, ElementCategory resultType,
                               int expectedResults, boolean[] subQuerySpecs,
                               Map<PropertyKey,Order> orderMap, String... intersectingIndexes) {
        if (intersectingIndexes==null) intersectingIndexes=new String[0];
        QueryDescription qd;
        switch(resultType) {
            case PROPERTY: qd = query.describeForProperties(); break;
            case EDGE: qd = query.describeForEdges(); break;
            case VERTEX: qd = query.describeForVertices(); break;
            default: throw new AssertionError();
        }
        assertEquals(1,qd.getNoCombinedQueries());
        assertEquals(1,qd.getNoSubQueries());
        QueryDescription.SubQuery sq = qd.getSubQueries().get(0);
        assertNotNull(sq);
        if (subQuerySpecs.length==2) { //0=>fitted, 1=>ordered
            assertEquals(subQuerySpecs[0],sq.isFitted());
            assertEquals(subQuerySpecs[1],sq.isSorted());
        }
        StandardQueryDescription.StandardSubQuery ssq = (StandardQueryDescription.StandardSubQuery)sq;
        assertEquals(intersectingIndexes.length,ssq.numIntersectingQueries());
        assertEquals(Sets.newHashSet(intersectingIndexes),Sets.newHashSet(ssq.getIntersectingQueries()));
        //Check order
        OrderList orders = ((StandardQueryDescription)qd).getQueryOrder();
        assertNotNull(orders);
        assertEquals(orderMap.size(),orders.size());
        for (int i=0;i<orders.size();i++) {
            assertEquals(orderMap.get(orders.getKey(i)),orders.getOrder(i));
        }
        for (PropertyKey key : orderMap.keySet()) assertTrue(orders.containsKey(key));

        Iterable<? extends TitanElement> result;
        switch(resultType) {
            case PROPERTY: result = query.properties(); break;
            case EDGE: result = query.edges(); break;
            case VERTEX: result = query.vertices(); break;
            default: throw new AssertionError();
        }
        int no = 0;
        TitanElement previous = null;
        for (TitanElement e : result) {
            assertNotNull(e);
            no++;
            if (previous!=null && !orders.isEmpty()) {
                assertTrue(orders.compare(previous,e)<=0);
            }
            previous = e;
        }
        assertEquals(expectedResults,no);
    }


    public static void evaluateQuery(TitanVertexQuery query, RelationCategory resultType,
                               int expectedResults, int numSubQueries, boolean[] subQuerySpecs,
                               Map<PropertyKey,Order> orderMap) {
        QueryDescription qd;
        switch(resultType) {
            case PROPERTY: qd = query.describeForProperties(); break;
            case EDGE: qd = query.describeForEdges(); break;
            case RELATION: qd = ((BasicVertexCentricQueryBuilder)query).describeForRelations(); break;
            default: throw new AssertionError();
        }
        assertEquals(1,qd.getNoCombinedQueries());
        assertEquals(numSubQueries,qd.getNoSubQueries());
        List<? extends QueryDescription.SubQuery> subqs = qd.getSubQueries();
        assertEquals(numSubQueries,subqs.size());
        for (int i=0;i<numSubQueries;i++) {
            QueryDescription.SubQuery sq = subqs.get(i);
            assertNotNull(sq);
            if (subQuerySpecs.length==2) { //0=>fitted, 1=>ordered
                assertEquals(subQuerySpecs[0],sq.isFitted());
                assertEquals(subQuerySpecs[1],sq.isSorted());
            }
            assertEquals(1,((StandardQueryDescription.StandardSubQuery)sq).numIntersectingQueries());
        }
        //Check order
        OrderList orders = ((StandardQueryDescription)qd).getQueryOrder();
        assertNotNull(orders);
        assertEquals(orderMap.size(),orders.size());
        for (int i=0;i<orders.size();i++) {
            assertEquals(orderMap.get(orders.getKey(i)),orders.getOrder(i));
        }
        for (PropertyKey key : orderMap.keySet()) assertTrue(orders.containsKey(key));

        Iterable<? extends TitanElement> result;
        switch(resultType) {
            case PROPERTY: result = query.properties(); break;
            case EDGE: result = query.edges(); break;
            case RELATION: result = query.relations(); break;
            default: throw new AssertionError();
        }
        int no = 0;
        TitanElement previous = null;
        for (TitanElement e : result) {
            assertNotNull(e);
            no++;
            if (previous!=null && !orders.isEmpty()) {
                assertTrue(orders.compare(previous,e)<=0);
            }
            previous = e;
        }
        assertEquals(expectedResults,no);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        String ks = getClass().getSimpleName();
        ModifiableConfiguration config = buildConfiguration();
        config.set(STORAGE_BACKEND,"embeddedcassandra");
        config.set(STORAGE_HOSTS,new String[]{"localhost"});
        config.set(STORAGE_CONF_FILE, TitanCassandraOutputFormat.class.getResource("cassandra.yaml").toString());
        config.set(CASSANDRA_KEYSPACE, cleanKeyspaceName(ks));
        config.set(PAGE_SIZE,500);
        return config.getConfiguration();
    }

    /*
     * Cassandra only accepts keyspace names 48 characters long or shorter made
     * up of alphanumeric characters and underscores.
     */
    private static String cleanKeyspaceName(String raw) {
        Preconditions.checkNotNull(raw);
        Preconditions.checkArgument(0 < raw.length());

        if (48 < raw.length() || raw.matches("[^a-zA-Z_]")) {
            return "strhash" + String.valueOf(Math.abs(raw.hashCode()));
        } else {
            return raw;
        }
    }
}
