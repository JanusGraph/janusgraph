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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.TestCategory;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.diskstorage.util.TestLockerManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;

import static org.apache.tinkerpop.gremlin.structure.Direction.IN;
import static org.apache.tinkerpop.gremlin.structure.Direction.OUT;
import static org.janusgraph.testutil.JanusGraphAssert.assertCount;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

@Tag(TestCategory.SERIAL_TESTS)
public abstract class JanusGraphEventualGraphTest extends JanusGraphBaseTest {

    @Test
    public void verifyEligibility() {
        Preconditions.checkArgument(!graph.getConfiguration().getBackend().getStoreFeatures().hasTxIsolation(),
                "This test suite only applies to eventually consistent data stores");
    }

    @Test
    public void concurrentIndexTest() {
        makeVertexIndexedUniqueKey("uid", String.class);
        makeVertexIndexedKey("value", Object.class);
        finishSchema();


        tx.addVertex("uid", "v");

        clopen();

        //Concurrent index addition
        JanusGraphTransaction tx1 = graph.newTransaction();
        JanusGraphTransaction tx2 = graph.newTransaction();
        getVertex(tx1, "uid", "v").property(VertexProperty.Cardinality.single, "value",  11);
        getVertex(tx2, "uid", "v").property(VertexProperty.Cardinality.single, "value",  11);
        tx1.commit();
        tx2.commit();

        assertEquals("v", Iterators.<String>getOnlyElement(Iterables.getOnlyElement(tx.query().has("value", 11).vertices()).values("uid")));
    }

    /**
     * Tests the correct interpretation of the commit time and that timestamps can be read
     */
    @Test
    public void testTimestampSetting() {
        clopen(option(GraphDatabaseConfiguration.STORE_META_TIMESTAMPS,"edgestore"),true,
                option(GraphDatabaseConfiguration.STORE_META_TTL,"edgestore"),true);


        // Transaction 1: Init graph with two vertices, having set "name" and "age" properties
        JanusGraphTransaction tx1 = graph.buildTransaction().commitTime(Instant.ofEpochSecond(100)).start();
        String name = "name";
        String age = "age";
        String address = "address";

        JanusGraphVertex v1 = tx1.addVertex(name, "a");
        JanusGraphVertex v2 = tx1.addVertex(age, "14", name, "b", age, "42");
        tx1.commit();

        // Fetch vertex ids
        long id1 = getId(v1);
        long id2 = getId(v2);

        // Transaction 2: Remove "name" property from v1, set "address" property; create
        // an edge v2 -> v1
        JanusGraphTransaction tx2 = graph.buildTransaction().commitTime(Instant.ofEpochSecond(1000)).start();
        v1 = getV(tx2,id1);
        v2 = getV(tx2,id2);
        for (Iterator<VertexProperty<Object>> propertyIterator = v1.properties(name); propertyIterator.hasNext(); ) {
            VertexProperty prop = propertyIterator.next();
            if (features.hasTimestamps()) {
                Instant t = prop.value("~timestamp");
                assertEquals(100,t.getEpochSecond());
                assertEquals(Instant.ofEpochSecond(0, 1000).getNano(),t.getNano());
            }
            if (features.hasCellTTL()) {
                Duration d = prop.value("~ttl");
                assertEquals(0L, d.getSeconds());
                assertTrue(d.isZero());
            }
        }
        assertEquals(1, v1.query().propertyCount());
        assertEquals(1, v1.query().has("~timestamp", Cmp.GREATER_THAN, Instant.ofEpochSecond(10)).propertyCount());
        assertEquals(1, v1.query().has("~timestamp", Instant.ofEpochSecond(100, 1000)).propertyCount());
        v1.property(name).remove();
        v1.property(VertexProperty.Cardinality.single, address,  "xyz");
        Edge edge = v2.addEdge("parent",v1);
        tx2.commit();
        Object edgeId = edge.id();

        JanusGraphVertex afterTx2 = getV(graph,id1);

        // Verify that "name" property is gone
        assertFalse(afterTx2.keys().contains(name));
        // Verify that "address" property is set
        assertEquals("xyz", afterTx2.value(address));
        // Verify that the edge is properly registered with the endpoint vertex
        assertCount(1, afterTx2.query().direction(IN).labels("parent").edges());
        // Verify that edge is registered under the id
        assertNotNull(getE(graph,edgeId));
        graph.tx().commit();

        // Transaction 3: Remove "address" property from v1 with earlier timestamp than
        // when the value was set
        JanusGraphTransaction tx3 = graph.buildTransaction().commitTime(Instant.ofEpochSecond(200)).start();
        v1 = getV(tx3,id1);
        v1.property(address).remove();
        tx3.commit();

        JanusGraphVertex afterTx3 = getV(graph,id1);
        graph.tx().commit();
        // Verify that "address" is still set
        assertEquals("xyz", afterTx3.value(address));

        // Transaction 4: Modify "age" property on v2, remove edge between v2 and v1
        JanusGraphTransaction tx4 = graph.buildTransaction().commitTime(Instant.ofEpochSecond(2000)).start();
        v2 = getV(tx4,id2);
        v2.property(VertexProperty.Cardinality.single, age,  "15");
        getE(tx4,edgeId).remove();
        tx4.commit();

        JanusGraphVertex afterTx4 = getV(graph,id2);
        // Verify that "age" property is modified
        assertEquals("15", afterTx4.value(age));
        // Verify that edge is no longer registered with the endpoint vertex
        assertCount(0, afterTx4.query().direction(OUT).labels("parent").edges());
        // Verify that edge entry disappeared from id registry
        assertNull(getE(graph,edgeId));

        // Transaction 5: Modify "age" property on v2 with earlier timestamp
        JanusGraphTransaction tx5 = graph.buildTransaction().commitTime(Instant.ofEpochSecond(1500)).start();
        v2 = getV(tx5,id2);
        v2.property(VertexProperty.Cardinality.single, age,  "16");
        tx5.commit();
        JanusGraphVertex afterTx5 = getV(graph,id2);

        // Verify that the property value is unchanged
        assertEquals("15", afterTx5.value(age));
    }

    /**
     * Tests that timestamped edges can be updated
     */
    @Test
    public void testTimestampedEdgeUpdates() {
        clopen(option(GraphDatabaseConfiguration.STORE_META_TIMESTAMPS, "edgestore"), true,
                option(GraphDatabaseConfiguration.STORE_META_TTL, "edgestore"), true);
        // Transaction 1: Init graph with two vertices and one edge
        JanusGraphTransaction tx = graph.buildTransaction().commitTime(Instant.ofEpochSecond(100)).start();
        JanusGraphVertex v1 = tx.addVertex();
        JanusGraphVertex v2 = tx.addVertex();
        Edge e = v1.addEdge("related",v2);
        e.property("time", 25);
        tx.commit();

        tx = graph.buildTransaction().commitTime(Instant.ofEpochSecond(200)).start();
        v1 = tx.getVertex(v1.longId());
        assertNotNull(v1);
        e = Iterators.getOnlyElement(v1.edges(Direction.OUT, "related"));
        assertNotNull(e);
        assertEquals(Integer.valueOf(25), e.value("time"));
        e.property("time", 125);
        tx.commit();

        tx = graph.buildTransaction().commitTime(Instant.ofEpochSecond(300)).start();
        v1 = tx.getVertex(v1.longId());
        assertNotNull(v1);
        e = Iterators.getOnlyElement(v1.edges(Direction.OUT, "related"));
        assertEquals(Integer.valueOf(125), e.value("time"));
        e.remove();
        tx.commit();
    }

    /**
     * Tests that batch-loading will ignore locks
     */
    @Test
    public void testBatchLoadingNoLock() {
        testBatchLoadingLocking(true);
    }

    /**
     * Tests that without batch-loading locks will be correctly applied (and therefore the tx fails)
     */
    @Test
    public void testLockException() {
        try {
            testBatchLoadingLocking(false);
            fail();
        } catch (JanusGraphException e) {
            Throwable cause = e;
            while (cause.getCause()!=null) cause=cause.getCause();
            assertEquals(UnsupportedOperationException.class,cause.getClass());
        }
    }

    public void testBatchLoadingLocking(boolean batchLoading) {
        PropertyKey uid = makeKey("uid",Long.class);
        JanusGraphIndex uidIndex = mgmt.buildIndex("uid",Vertex.class).unique().addKey(uid).buildCompositeIndex();
        mgmt.setConsistency(uid, ConsistencyModifier.LOCK);
        mgmt.setConsistency(uidIndex,ConsistencyModifier.LOCK);
        EdgeLabel knows = mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.ONE2ONE).make();
        mgmt.setConsistency(knows,ConsistencyModifier.LOCK);
        finishSchema();

        TestLockerManager.ERROR_ON_LOCKING=true;
        clopen(option(GraphDatabaseConfiguration.STORAGE_BATCH),batchLoading,
                option(GraphDatabaseConfiguration.LOCK_BACKEND),"test");


        int numV = 10000;
        for (int i=0;i<numV;i++) {
            JanusGraphVertex v = tx.addVertex("uid",i+1);
            v.addEdge("knows",v);
        }
        clopen();

        for (int i=0;i<Math.min(numV,300);i++) {
            assertEquals(1, Iterables.size(graph.query().has("uid", i + 1).vertices()));
            JanusGraphVertex v = Iterables.getOnlyElement(graph.query().has("uid", i + 1).vertices());
            assertEquals(1, Iterables.size(v.query().direction(OUT).labels("knows").edges()));
        }
    }

    /**
     * Tests that consistency modes are correctly interpreted in the absence of locks (or tx isolation)
     */
    @Test
    public void testConsistencyModifier() throws InterruptedException {
        makeKey("sig",Integer.class);
        makeKey("weight",Double.class);
        mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SET).make();
        mgmt.makePropertyKey("value").dataType(Integer.class).cardinality(Cardinality.LIST).make();
        PropertyKey valuef = mgmt.makePropertyKey("valuef").dataType(Integer.class).cardinality(Cardinality.LIST).make();
        mgmt.setConsistency(valuef,ConsistencyModifier.FORK);

        mgmt.makeEdgeLabel("em").multiplicity(Multiplicity.MULTI).make();
        EdgeLabel emf = mgmt.makeEdgeLabel("emf").multiplicity(Multiplicity.MULTI).make();
        mgmt.setConsistency(emf,ConsistencyModifier.FORK);
        mgmt.makeEdgeLabel("es").multiplicity(Multiplicity.SIMPLE).make();
        mgmt.makeEdgeLabel("o2o").multiplicity(Multiplicity.ONE2ONE).make();
        mgmt.makeEdgeLabel("o2m").multiplicity(Multiplicity.ONE2MANY).make();

        finishSchema();

        JanusGraphVertex u = tx.addVertex(), v = tx.addVertex();
        JanusGraphRelation[] rs = new JanusGraphRelation[9];
        final int transactionId = 1;
        rs[0]=sign(v.property("weight",5.0),transactionId);
        rs[1]=sign(v.property("name","John"),transactionId);
        rs[2]=sign(v.property("value",2),transactionId);
        rs[3]=sign(v.property("valuef",2),transactionId);

        rs[6]=sign(v.addEdge("es",u),transactionId);
        rs[7]=sign(v.addEdge("o2o",u),transactionId);
        rs[8]=sign(v.addEdge("o2m",u),transactionId);
        rs[4]=sign(v.addEdge("em",u),transactionId);
        rs[5]=sign(v.addEdge("emf",u),transactionId);

        newTx();
        long vid = getId(v), uid = getId(u);

        JanusGraphTransaction tx1 = graph.newTransaction();
        JanusGraphTransaction tx2 = graph.newTransaction();
        final int wintx = 20;
        processTx(tx1,wintx-10,vid,uid);
        processTx(tx2,wintx,vid,uid);
        tx1.commit();
        Thread.sleep(5);
        tx2.commit(); //tx2 should win using time-based eventual consistency

        newTx();
        v = getV(tx,vid);
        assertEquals(6.0, v.<Double>value("weight"),0.00001);
        VertexProperty p = getOnlyElement(v.properties("weight"));
        assertEquals(wintx,p.<Integer>value("sig").intValue());
        p = getOnlyElement(v.properties("name"));
        assertEquals("Bob",p.value());
        assertEquals(wintx,p.<Integer>value("sig").intValue());
        p = getOnlyElement(v.properties("value"));
        assertEquals(rs[2].longId(),getId(p));
        assertEquals(wintx,p.<Integer>value("sig").intValue());
        assertCount(2,v.properties("valuef"));
        for (Iterator<VertexProperty<Object>> ppiter = v.properties("valuef"); ppiter.hasNext(); ) {
            VertexProperty pp = ppiter.next();
            assertNotEquals(rs[3].longId(),getId(pp));
            assertEquals(2,pp.value());
        }

        Edge e = Iterables.getOnlyElement(v.query().direction(OUT).labels("es").edges());
        assertEquals(wintx,e.<Integer>value("sig").intValue());
        assertNotEquals(rs[6].longId(),getId(e));

        e = Iterables.getOnlyElement(v.query().direction(OUT).labels("o2o").edges());
        assertEquals(wintx,e.<Integer>value("sig").intValue());
        assertEquals(rs[7].longId(), getId(e));
        e = Iterables.getOnlyElement(v.query().direction(OUT).labels("o2m").edges());
        assertEquals(wintx,e.<Integer>value("sig").intValue());
        assertNotEquals(rs[8].longId(),getId(e));
        e = Iterables.getOnlyElement(v.query().direction(OUT).labels("em").edges());
        assertEquals(wintx,e.<Integer>value("sig").intValue());
        assertEquals(rs[4].longId(), getId(e));
        for (Edge o : v.query().direction(OUT).labels("emf").edges()) {
            assertNotEquals(rs[5].longId(),getId(o));
            assertEquals(uid, o.inVertex().id());
        }
    }


    private void processTx(JanusGraphTransaction tx, int transactionId, long vid, long uid) {
        JanusGraphVertex v = getV(tx,vid);
        JanusGraphVertex u = getV(tx,uid);
        assertEquals(5.0, v.<Double>value("weight"),0.00001);
        VertexProperty p = getOnlyElement(v.properties("weight"));
        assertEquals(1,p.<Integer>value("sig").intValue());
        sign(v.property("weight",6.0),transactionId);
        p = getOnlyElement(v.properties("name"));
        assertEquals(1,p.<Integer>value("sig").intValue());
        assertEquals("John",p.value());
        p.remove();
        sign(v.property("name","Bob"),transactionId);
        for (String pkey : new String[]{"value","valuef"}) {
            p = getOnlyElement(v.properties(pkey));
            assertEquals(1,p.<Integer>value("sig").intValue());
            assertEquals(2,p.value());
            sign((JanusGraphVertexProperty)p,transactionId);
        }

        JanusGraphEdge e = Iterables.getOnlyElement(v.query().direction(OUT).labels("es").edges());
        assertEquals(1,e.<Integer>value("sig").intValue());
        e.remove();
        sign(v.addEdge("es",u),transactionId);
        e = Iterables.getOnlyElement(v.query().direction(OUT).labels("o2o").edges());
        assertEquals(1,e.<Integer>value("sig").intValue());
        sign(e,transactionId);
        e = Iterables.getOnlyElement(v.query().direction(OUT).labels("o2m").edges());
        assertEquals(1,e.<Integer>value("sig").intValue());
        e.remove();
        sign(v.addEdge("o2m",u),transactionId);
        for (String label : new String[]{"em","emf"}) {
            e = Iterables.getOnlyElement(v.query().direction(OUT).labels(label).edges());
            assertEquals(1,e.<Integer>value("sig").intValue());
            sign(e,transactionId);
        }
    }


    private JanusGraphRelation sign(JanusGraphRelation r, int id) {
        r.property("sig",id);
        return r;
    }





}
