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

package org.janusgraph.graphdb.query;

import com.google.common.collect.Iterators;
import org.janusgraph.core.*;
import org.janusgraph.core.attribute.Contain;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.internal.OrderList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class QueryTest {

    private JanusGraph graph;
    private JanusGraphTransaction tx;

    @Before
    public void setup() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, InMemoryStoreManager.class.getCanonicalName());
        graph = JanusGraphFactory.open(config);
        tx = graph.newTransaction();
    }

    @After
    public void shutdown() {
        if (tx!=null && tx.isOpen()) tx.commit();
        if (graph!=null && graph.isOpen()) graph.close();
    }

    @Test
    public void testMultipleKeysQuery() {
        tx.makePropertyKey("name").dataType(String.class).make();
        tx.addVertex("name","vertex1");
        tx.addVertex("name","vertex2");
        tx.addVertex("name","vertex3");

        int found = Iterators.size(tx.query().has("name", Contain.IN, Arrays.asList("vertex1")).vertices().iterator());
        assertEquals(1, found);

        found = Iterators.size(tx.query().has("name", Contain.IN, Arrays.asList("vertex1", "vertex2")).vertices().iterator());
        assertEquals(2, found);

        found = Iterators.size(tx.query().has("name", Contain.IN, Arrays.asList("vertex1", "vertex2", "vertex3")).vertices().iterator());
        assertEquals(3, found);

        found = Iterators.size(tx.query().has("name", Contain.IN, Arrays.asList("vertex1", "vertex2", "vertex3", "vertex4")).vertices().iterator());
        assertEquals(3, found);

        int limit = 2;
        found = Iterators.size(tx.query().has("name", Contain.IN, Arrays.asList("vertex1", "vertex2", "vertex3", "vertex4")).limit(limit).vertices().iterator());
        assertEquals(limit, found);
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

        JanusGraphVertex v1 = tx.addVertex("name","abc","time",20,"weight",2.5),
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
