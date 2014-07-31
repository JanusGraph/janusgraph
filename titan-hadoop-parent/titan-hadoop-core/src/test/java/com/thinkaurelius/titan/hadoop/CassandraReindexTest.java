package com.thinkaurelius.titan.hadoop;

import static com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager.CPOOL_MAX_TOTAL;
import static com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager.CPOOL_MAX_ACTIVE;
import static com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager.CPOOL_MAX_IDLE;
import static com.thinkaurelius.titan.graphdb.TitanGraphTest.evaluateQuery;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.LOG_READ_INTERVAL;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.LOG_SEND_DELAY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.MANAGEMENT_LOG;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.PAGE_SIZE;
import static com.thinkaurelius.titan.graphdb.internal.RelationCategory.EDGE;
import static com.thinkaurelius.titan.graphdb.internal.RelationCategory.PROPERTY;
import static com.tinkerpop.blueprints.Direction.OUT;
import static org.junit.Assert.fail;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.SchemaAction;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLog;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.graphdb.TitanGraphBaseTest;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

public class CassandraReindexTest extends TitanGraphBaseTest {

    @Test
    public void testIndexUpdatesWithReindex() throws Exception {
        clopen( option(LOG_SEND_DELAY,MANAGEMENT_LOG),new StandardDuration(0,TimeUnit.MILLISECONDS),
                option(KCVSLog.LOG_READ_LAG_TIME,MANAGEMENT_LOG),new StandardDuration(50,TimeUnit.MILLISECONDS),
                option(LOG_READ_INTERVAL,MANAGEMENT_LOG),new StandardDuration(250,TimeUnit.MILLISECONDS)
        );
        //Types without index
        PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).make();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SET).make();
        EdgeLabel friend = mgmt.makeEdgeLabel("friend").multiplicity(Multiplicity.MULTI).make();
        PropertyKey sensor = mgmt.makePropertyKey("sensor").dataType(Double.class).cardinality(Cardinality.LIST).make();
        finishSchema();

        //Add some sensor & friend data
        TitanVertex v = tx.addVertex();
        for (int i=0;i<10;i++) {
            v.addProperty("sensor",i).setProperty("time",i);
            v.addProperty("name","v"+i);
            TitanVertex o = tx.addVertex();
            v.addEdge("friend",o).setProperty("time",i);
        }
        newTx();
        //Indexes should not yet be in use
        v = tx.getVertex(v.getLongId());
        evaluateQuery(v.query().keys("sensor").interval("time", 1, 5).orderBy("time",Order.DESC),
                PROPERTY,4,1,new boolean[]{false,false},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 101, 105).orderBy("time",Order.DESC),
                PROPERTY,0,1,new boolean[]{false,false},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 1, 5).orderBy("time",Order.DESC),
                EDGE,4,1,new boolean[]{false,false},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 101, 105).orderBy("time",Order.DESC),
                EDGE,0,1,new boolean[]{false,false},tx.getPropertyKey("time"),Order.DESC);
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
        friend = mgmt.getEdgeLabel("friend");
        mgmt.buildPropertyIndex(sensor, "byTime", Order.DESC, time);
        mgmt.buildEdgeIndex(friend, "byTime", Direction.OUT, Order.DESC, time);
        mgmt.buildIndex("bySensorReading",Vertex.class).addKey(name).buildCompositeIndex();
        finishSchema();
        newTx();
        //Add some sensor & friend data that should already be indexed even though index is not yet enabled
        v = tx.getVertex(v.getLongId());
        for (int i=100;i<110;i++) {
            v.addProperty("sensor",i).setProperty("time",i);
            v.addProperty("name","v"+i);
            TitanVertex o = tx.addVertex();
            v.addEdge("friend",o).setProperty("time",i);
        }
        tx.commit();
        //Should not yet be able to enable since not yet registered
        try {
            mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType("sensor"),"byTime"), SchemaAction.ENABLE_INDEX);
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType("friend"),"byTime"), SchemaAction.ENABLE_INDEX);
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            mgmt.updateIndex(mgmt.getGraphIndex("bySensorReading"), SchemaAction.ENABLE_INDEX);
            fail();
        } catch (IllegalArgumentException e) {}
        mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType("sensor"),"byTime"), SchemaAction.REGISTER_INDEX);
        mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType("friend"),"byTime"), SchemaAction.REGISTER_INDEX);
        mgmt.updateIndex(mgmt.getGraphIndex("bySensorReading"), SchemaAction.REGISTER_INDEX);
        mgmt.commit();


        Thread.sleep(2000);
        finishSchema();
        mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType("sensor"),"byTime"), SchemaAction.ENABLE_INDEX);
        mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getRelationType("friend"),"byTime"), SchemaAction.ENABLE_INDEX);
        mgmt.updateIndex(mgmt.getGraphIndex("bySensorReading"), SchemaAction.ENABLE_INDEX);
        finishSchema();

        //Add some more sensor & friend data
        newTx();
        v = tx.getVertex(v.getLongId());
        for (int i=200;i<210;i++) {
            v.addProperty("sensor",i).setProperty("time",i);
            v.addProperty("name","v"+i);
            TitanVertex o = tx.addVertex();
            v.addEdge("friend",o).setProperty("time",i);
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
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 1, 5).orderBy("time",Order.DESC),
                EDGE,0,1,new boolean[]{true,true},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 101, 105).orderBy("time",Order.DESC),
                EDGE,4,1,new boolean[]{true,true},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 201, 205).orderBy("time",Order.DESC),
                EDGE,4,1,new boolean[]{true,true},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(tx.query().has("name","v5"),
                ElementCategory.VERTEX,0,new boolean[]{true,true},"bySensorReading");
        evaluateQuery(tx.query().has("name","v105"),
                ElementCategory.VERTEX,1,new boolean[]{true,true},"bySensorReading");
        evaluateQuery(tx.query().has("name","v205"),
                ElementCategory.VERTEX,1,new boolean[]{true,true},"bySensorReading");

        // Run some reindex jobs
        Properties titanInputProperties = new Properties();
        titanInputProperties.setProperty(ConfigElement.getPath(GraphDatabaseConfiguration.STORAGE_BACKEND), "cassandrathrift");
        String ks = getClass().getSimpleName();
        titanInputProperties.setProperty(ConfigElement.getPath(AbstractCassandraStoreManager.CASSANDRA_KEYSPACE), cleanKeyspaceName(ks));
        titanInputProperties.setProperty(ConfigElement.getPath(CassandraThriftStoreManager.CPOOL_MAX_TOTAL), "-1");
        titanInputProperties.setProperty(ConfigElement.getPath(CassandraThriftStoreManager.CPOOL_MAX_ACTIVE), "1");
        titanInputProperties.setProperty(ConfigElement.getPath(CassandraThriftStoreManager.CPOOL_MAX_IDLE), "1");
        TitanIndexRepair.cassandraRepair(titanInputProperties, "byTime", "sensor", "org.apache.cassandra.dht.Murmur3Partitioner");
        TitanIndexRepair.cassandraRepair(titanInputProperties, "byTime", "friend", "org.apache.cassandra.dht.Murmur3Partitioner");
        TitanIndexRepair.cassandraRepair(titanInputProperties, "bySensorReading", "", "org.apache.cassandra.dht.Murmur3Partitioner");

        newTx();

        // Use indexes, see old and new data
        v = tx.getVertex(v.getLongId());
        evaluateQuery(v.query().keys("sensor").interval("time", 1, 5).orderBy("time",Order.DESC),
                PROPERTY,4,1,new boolean[]{true,true},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 101, 105).orderBy("time",Order.DESC),
                PROPERTY,4,1,new boolean[]{true,true},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 201, 205).orderBy("time",Order.DESC),
                PROPERTY,4,1,new boolean[]{true,true},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 1, 5).orderBy("time",Order.DESC),
                EDGE,4,1,new boolean[]{true,true},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 101, 105).orderBy("time",Order.DESC),
                EDGE,4,1,new boolean[]{true,true},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 201, 205).orderBy("time",Order.DESC),
                EDGE,4,1,new boolean[]{true,true},tx.getPropertyKey("time"),Order.DESC);
        evaluateQuery(tx.query().has("name","v5"),
                ElementCategory.VERTEX,1,new boolean[]{true,true},"bySensorReading");
        evaluateQuery(tx.query().has("name","v105"),
                ElementCategory.VERTEX,1,new boolean[]{true,true},"bySensorReading");
        evaluateQuery(tx.query().has("name","v205"),
                ElementCategory.VERTEX,1,new boolean[]{true,true},"bySensorReading");
    }

    // Need to make CassandraStorageSetup's static initializer play nice with titan-hadoop-parent before that code can be used here

    @Override
    public WriteConfiguration getConfiguration() {
        String className = getClass().getSimpleName();
        ModifiableConfiguration mc = CassandraStorageSetup.getEmbeddedConfiguration(className);
        mc.set(CPOOL_MAX_TOTAL, -1);
        mc.set(CPOOL_MAX_ACTIVE, 1);
        mc.set(CPOOL_MAX_IDLE, 1);
        mc.set(PAGE_SIZE, 500);
        return mc.getConfiguration();
    }
    
    public static String cleanKeyspaceName(String raw) {
        Preconditions.checkNotNull(raw);
        Preconditions.checkArgument(0 < raw.length());

        if (48 < raw.length() || raw.matches("[^a-zA-Z_]")) {
            return "strhash" + String.valueOf(Math.abs(raw.hashCode()));
        } else {
            return raw;
        }
    }
}
