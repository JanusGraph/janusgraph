package com.thinkaurelius.titan.graphdb.query;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.internal.Order;
import com.thinkaurelius.titan.graphdb.internal.OrderList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class QueryTest {

    private TitanGraph graph;
    private TitanTransaction tx;

    @Before
    public void setup() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, InMemoryStoreManager.class.getCanonicalName());
        graph = TitanFactory.open(config);
        tx = graph.newTransaction();
    }

    @After
    public void shutdown() {
        if (tx!=null && tx.isOpen()) tx.commit();
        if (graph!=null && graph.isOpen()) graph.close();
    }

    @Test
    public void testOrderList() {
        PropertyKey name = tx.makePropertyKey("name").dataType(String.class).make();
        PropertyKey weight = tx.makePropertyKey("weight").dataType(Double.class).make();
        PropertyKey time = tx.makePropertyKey("time").dataType(Long.class).make();

        OrderList ol1 = new OrderList();
        ol1.add(name, Order.DESC);
        ol1.add(weight, Order.ASC);
        ol1.makeImmutable();
        try {
            ol1.add(time, Order.DESC);
            fail();
        } catch (IllegalArgumentException e) {}
        assertEquals(2, ol1.size());
        assertEquals(name,ol1.getKey(0));
        assertEquals(weight, ol1.getKey(1));
        assertEquals(Order.DESC,ol1.getOrder(0));
        assertEquals(Order.ASC, ol1.getOrder(1));
        assertFalse(ol1.hasCommonOrder());

        OrderList ol2 = new OrderList();
        ol2.add(time,Order.ASC);
        ol2.add(weight, Order.ASC);
        ol2.add(name, Order.ASC);
        ol2.makeImmutable();
        assertTrue(ol2.hasCommonOrder());
        assertEquals(Order.ASC,ol2.getCommonOrder());

        OrderList ol3 = new OrderList();
        ol3.add(weight,Order.DESC);

        TitanVertex v1 = tx.addVertex("name","abc","time",20,"weight",2.5),
                v2 = tx.addVertex("name","bcd","time",10,"weight",2.5),
                v3 = tx.addVertex("name","abc","time",10,"weight",4.5);

        assertTrue(ol1.compare(v1,v2)>0);
        assertTrue(ol1.compare(v2,v3)<0);
        assertTrue(ol1.compare(v1,v3)<0);

        assertTrue(ol2.compare(v1,v2)>0);
        assertTrue(ol2.compare(v2,v3)<0);
        assertTrue(ol2.compare(v1,v3)>0);

        assertTrue(ol3.compare(v1,v2)==0);
        assertTrue(ol3.compare(v2,v3)>0);
        assertTrue(ol3.compare(v1,v3)>0);

    }



}
