package com.thinkaurelius.titan.graphdb;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.util.TestLockerManager;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TitanEventualGraphTest extends TitanGraphTestCommon {

    private Logger log = LoggerFactory.getLogger(TitanEventualGraphTest.class);

    public TitanEventualGraphTest(Configuration config) {
        super(config);
    }

    public void updateConfiguration(Map<String,? extends Object> settings) {
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
        updateConfiguration(ImmutableMap.of("storage.batch-loading",batchloading,"storage.lock-backend","test"));


        int numV = 100;
        for (int i=0;i<numV;i++) {
            TitanVertex v = tx.addVertex();
            v.setProperty("uid",i+1);
            v.addEdge("knows",v);
        }
        clopen();

        for (int i=0;i<numV;i++) {
            assertEquals(1,Iterables.size(graph.query().has("uid",i+1).vertices()));
            assertEquals(1,Iterables.size(graph.query().has("uid",i+1).vertices().iterator().next().getEdges(Direction.OUT,"knows")));
        }
    }


}
