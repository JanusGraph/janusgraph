package com.thinkaurelius.titan.graphdb;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Decimal;
import com.thinkaurelius.titan.core.attribute.Precision;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.log.Log;
import com.thinkaurelius.titan.diskstorage.log.Message;
import com.thinkaurelius.titan.diskstorage.log.MessageReader;
import com.thinkaurelius.titan.diskstorage.log.ReadMarker;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.log.LogTxMeta;
import com.thinkaurelius.titan.graphdb.database.log.TransactionLogHeader;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.serializer.SpecialInt;
import com.thinkaurelius.titan.graphdb.serializer.SpecialIntSerializer;
import com.thinkaurelius.titan.graphdb.types.StandardEdgeLabelMaker;
import com.thinkaurelius.titan.graphdb.types.StandardPropertyKeyMaker;
import com.thinkaurelius.titan.testutil.TestUtil;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Vertex;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.tinkerpop.blueprints.Direction.*;
import static org.junit.Assert.*;

public abstract class TitanGraphTest extends TitanGraphBaseTest {

    private Logger log = LoggerFactory.getLogger(TitanGraphTest.class);

    @Test
    public void testOpenClose() {
    }

    @Test
    public void testBasic() {
        PropertyKey weight = tx.makePropertyKey("weight").dataType(Decimal.class).cardinality(Cardinality.SINGLE).make();
        TitanVertex n1 = tx.addVertex();
        n1.addProperty(weight, 10.5);
        assertTrue(tx.containsRelationType("weight"));
        clopen();
        long nid = n1.getID();
        assertTrue(tx.containsVertex(nid));
        assertTrue(tx.containsVertex(weight.getID()));
        assertFalse(tx.containsVertex(nid + 64));
        assertFalse(tx.containsVertex(weight.getID() + 64));
        assertTrue(tx.containsRelationType("weight"));
        weight = tx.getPropertyKey("weight");
        assertEquals(weight.getDataType(), Decimal.class);
        assertEquals(weight.getName(), "weight");
        n1 = tx.getVertex(nid);

        for (TitanProperty prop : n1.getProperties()) {
            Object o = prop.getValue();
        }
        n1.query().relations();
        assertTrue(n1.getProperty(weight).equals(10.5));
    }

    @Test
    public void simpleLogTest() throws InterruptedException {
        final String triggerName = "test";
        final Serializer serializer = graph.getDataSerializer();
        final EdgeSerializer edgeSerializer = graph.getEdgeSerializer();
        final TimestampProvider times = graph.getConfiguration().getTimestampProvider();
        final TimeUnit unit = times.getUnit();
        final long startTime = times.getTime().getTimestamp(TimeUnit.MILLISECONDS);
//        System.out.println(startTime);
        clopen(option(SYSTEM_LOG_TRANSACTIONS), true);
        testBasic();
        //Transaction with custom triggerName
        TitanTransaction tx2 = graph.buildTransaction().setLogIdentifier(triggerName).start();
        TitanVertex v1 = tx2.addVertex();
        v1.setProperty("weight",111.1);
        tx2.commit();
        tx2 = graph.buildTransaction().setLogIdentifier(triggerName).start();
        TitanVertex v2 = tx2.addVertex();
        v2.setProperty("weight",222.2);
        tx2.commit();
        //Only read tx
        tx2 = graph.buildTransaction().setLogIdentifier(triggerName).start();
        v1 = tx2.getVertex(v1.getID());
        assertEquals(111.1,v1.<Decimal>getProperty("weight").doubleValue(),0.0);
        assertEquals(222.2,tx2.getVertex(v2).<Decimal>getProperty("weight").doubleValue(),0.0);
        tx2.commit();
        close();

        Log txlog = openTxLog(ReadMarker.fromTime(startTime, TimeUnit.MILLISECONDS));
        Log triggerLog = openTriggerLog(triggerName, ReadMarker.fromTime(startTime, TimeUnit.MILLISECONDS));
        final AtomicInteger txMsgCounter = new AtomicInteger(0);
        final AtomicInteger triggerMeta = new AtomicInteger(0);
        txlog.registerReader(new MessageReader() {
            @Override
            public void read(Message message) {
                long msgTime = message.getTimestamp(TimeUnit.MILLISECONDS);
                assertTrue(msgTime>=startTime);
                assertNotNull(message.getSenderId());
                TransactionLogHeader.Entry txEntry = TransactionLogHeader.parse(message.getContent(),serializer, times);
                TransactionLogHeader header = txEntry.getHeader();
//                System.out.println(header.getTimestamp(TimeUnit.MILLISECONDS));
                assertTrue(header.getTimestamp(TimeUnit.MILLISECONDS) >= startTime);
                assertTrue(header.getTimestamp(TimeUnit.MILLISECONDS)<=msgTime);
                assertNotNull(txEntry.getMetadata());
                assertNull(txEntry.getMetadata().get(LogTxMeta.GROUPNAME));
                if (!txEntry.hasContent()) {
                    assertTrue(txEntry.getStatus().isSuccess());
                } else {
                    assertTrue(txEntry.getStatus().isPreCommit());
                    Object logid = txEntry.getMetadata().get(LogTxMeta.LOG_ID);
                    if (logid!=null) {
                        assertTrue(logid instanceof String);
                        assertEquals(triggerName,logid);
                        triggerMeta.incrementAndGet();
                    }
                    //TODO: Verify content parses correctly
                }
                txMsgCounter.incrementAndGet();
            }
        });
        final AtomicInteger triggerMsgCounter = new AtomicInteger(0);
        triggerLog.registerReader(new MessageReader() {
            @Override
            public void read(Message message) {
                long msgTime = message.getTimestamp(TimeUnit.MILLISECONDS);
                assertTrue(msgTime>=startTime);
                assertNotNull(message.getSenderId());
                StaticBuffer content = message.getContent();
                assertTrue(content!=null && content.length()>0);
                ReadBuffer read = content.asReadBuffer();
                long txTime = TimeUnit.MILLISECONDS.convert(read.getLong(),unit);
                assertTrue(txTime<=msgTime);
                assertTrue(txTime>=startTime);
                long txid = read.getLong();
                assertTrue(txid>0);
                for (String type : new String[]{"add","del"}) {
                    long num = VariableLong.readPositive(read);
                    assertTrue(num>=0 && num<Integer.MAX_VALUE);
                    if (type.equals("add")) {
                        assertEquals(2,num);
                    } else {
                        assertEquals(0,num);
                    }
                    for (int i=0; i<num;i++) {
                        Long vertexid = VariableLong.readPositive(read);
                        assertTrue(vertexid>0);
                        Entry entry = BufferUtil.readEntry(read,serializer);
                        assertNotNull(entry);
                        assertEquals(Direction.OUT,edgeSerializer.parseDirection(entry));
                    }
                }
                triggerMsgCounter.incrementAndGet();
            }
        });
        Thread.sleep(20000);
        assertEquals(8, txMsgCounter.get());
        assertEquals(2,triggerMsgCounter.get());
        assertEquals(2,triggerMeta.get());
    }


    @Test
    public void testTypes() {
        clopen(option(CUSTOM_ATTRIBUTE_CLASS,"attribute10"),SpecialInt.class.getCanonicalName(),
                option(CUSTOM_SERIALIZER_CLASS,"attribute10"),SpecialIntSerializer.class.getCanonicalName());

        EdgeLabel friend = mgmt.makeEdgeLabel("friend").directed().make();

        PropertyKey id = makeVertexIndexedUniqueKey("uid",String.class);

        PropertyKey name = makeKey("name",String.class);

        PropertyKey weight = makeKey("weight",Decimal.class);

        PropertyKey someid = makeVertexIndexedKey("someid",Object.class);

        PropertyKey boolval = makeKey("boolval",Boolean.class);

        PropertyKey sint = makeKey("int",SpecialInt.class);

        EdgeLabel link = mgmt.makeEdgeLabel("link").unidirected().make();

        EdgeLabel connect = mgmt.makeEdgeLabel("connect").signature(id, weight).multiplicity(Multiplicity.MANY2ONE).make();

        EdgeLabel parent = mgmt.makeEdgeLabel("parent").multiplicity(Multiplicity.MANY2ONE).make();
        assertTrue(parent.getMultiplicity().isUnique(OUT));
        EdgeLabel child = mgmt.makeEdgeLabel("child").multiplicity(Multiplicity.ONE2MANY).make();
        assertTrue(child.getMultiplicity().isUnique(IN));
        EdgeLabel spouse = mgmt.makeEdgeLabel("spouse").multiplicity(Multiplicity.ONE2ONE).make();
        assertTrue(spouse.getMultiplicity().isUnique(IN));
        assertTrue(spouse.getMultiplicity().isUnique(OUT));

        assertEquals("weight",weight.toString());


        try {
            mgmt.makePropertyKey("pint").dataType(int.class).make();
            fail();
        } catch (IllegalArgumentException e) {
        }

        try {
            mgmt.makePropertyKey("number").dataType(Number.class).make();
            fail();
        } catch (IllegalArgumentException e) {
        }

        PropertyKey arrType = mgmt.makePropertyKey("barr").dataType(byte[].class).make();

        finishSchema();
        clopen();

        assertNull(getVertex(id, "v1"));

        id = tx.getPropertyKey("uid");
        assertTrue(mgmt.getGraphIndex(id.getName()).isUnique());
        assertTrue(id.getCardinality()==Cardinality.SINGLE);
        assertEquals(String.class, id.getDataType());

        //Basic properties

        friend = tx.getEdgeLabel("friend");
        assertEquals("friend", friend.getName());
        assertTrue(friend.isDirected());
        assertFalse(friend.isUnidirected());
        assertTrue(friend.isEdgeLabel());
        assertFalse(friend.isPropertyKey());
        assertFalse(friend.getMultiplicity().isUnique(Direction.OUT));
        assertTrue(((InternalRelationType) friend).isHidden());
        assertFalse(((InternalRelationType) friend).isHiddenType());

        connect = tx.getEdgeLabel("connect");
        assertEquals("connect", connect.getName());
        assertFalse(connect.isUnidirected());
        assertTrue(connect.isEdgeLabel());
        assertFalse(connect.isPropertyKey());
        assertTrue(connect.getMultiplicity().isUnique(Direction.OUT));
        assertFalse(((InternalRelationType) connect).isHiddenType());

        link = tx.getEdgeLabel("link");
        assertTrue(link.isUnidirected());
        assertFalse(link.getMultiplicity().isUnique(Direction.OUT));

        weight = tx.getPropertyKey("weight");
        assertEquals(Decimal.class, weight.getDataType());

        boolval = tx.getPropertyKey("boolval");
        assertEquals(Boolean.class, boolval.getDataType());

        name = tx.getPropertyKey("name");
        //Failures
        try {
            tx.makePropertyKey("fid").make();
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            tx.makeEdgeLabel("link").unidirected().make();
            fail();
        } catch (IllegalArgumentException e) {
        }
        tx.makeEdgeLabel("test").make();
        try {
            ((StandardEdgeLabelMaker)tx.makeEdgeLabel("link2")).unidirected().
                    sortKey(name, weight).signature(name).make();
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            ((StandardEdgeLabelMaker)tx.makeEdgeLabel("link2")).unidirected().
                    sortKey(tx.getPropertyKey("int"), weight).make();
            fail();
        } catch (IllegalArgumentException e) {
        }
//        try {
//            tx.makeLabel("link2").unidirected().
//                    sortKey(id, weight).make();
//            fail();
//        } catch (IllegalArgumentException e) {
//        }
        EdgeLabel link2 = ((StandardEdgeLabelMaker)tx.makeEdgeLabel("link2")).unidirected().sortKey(name, weight).make();

        // Data types and serialization
        TitanVertex v = tx.addVertex();
        v.addProperty(id, "Hello");
        v.addProperty(weight, 0.5);
        v.addProperty(boolval, true);
        try {
            v.addProperty(weight, "x");
            fail();
        } catch (IllegalArgumentException e) {
        }

        // Verify vertex in new transaction and functional edges
        TitanVertex v1 = tx.addVertex();
        v1.setProperty("uid", "v1");
        v1.setProperty("someid", 100l);
        try {
            v1.addProperty("int", 10.5);
            fail();
        } catch (IllegalArgumentException e) {
        }
        v1.setProperty("int", new SpecialInt(77));
        TitanVertex v12 = tx.addVertex(), v13 = tx.addVertex();
        v12.setProperty("uid", "v12");
        v13.setProperty("uid", "v13");
        v12.addEdge("parent", v1).setProperty("weight", 3.5);
        v13.addEdge("parent", v1).setProperty("weight", 10.5);
        try {
            v12.addEdge("parent", v13);
            fail();
        } catch (IllegalArgumentException e) {
        }
        assertEquals(2, Iterables.size(v1.query().direction(IN).labels("parent").edges()));
        assertEquals(1, Iterables.size(v1.query().direction(IN).labels("parent").has("weight", Compare.GREATER_THAN, 10.0).edges()));
        assertEquals(1, Iterables.size(v12.query().direction(OUT).labels("parent").has("weight").edges()));
        assertEquals(1, Iterables.size(v12.query().direction(OUT).labels("parent").has("weight", Compare.GREATER_THAN, 3).edges()));
        assertEquals(1, Iterables.size(v13.query().direction(OUT).labels("parent").has("weight").edges()));

        v1.addEdge("child", v12);
        v1.addEdge("child", v13);
        try {
            v13.addEdge("child", v12);
            fail();
        } catch (IllegalArgumentException e) {
        }

        v1.addEdge("spouse", v12);
        try {
            v13.addEdge("spouse", v12);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            v1.addEdge("spouse", v13);
            fail();
        } catch (IllegalArgumentException e) {
        }

        v1.addEdge("connect", v12);
        Edge edge = Iterables.getOnlyElement(v1.getEdges(OUT, "connect"));
        assertEquals(0, edge.getPropertyKeys().size());

        clopen();

        id = tx.getPropertyKey("uid");
        v1 = getVertex(id, "v1");
        assertEquals(77, ((SpecialInt) v1.getProperty("int")).getValue());
        assertEquals(v1, getVertex("someid", 100l));
        assertEquals(v1, getVertex(id, "v1"));

        v12 = (TitanVertex) tx.getVertex(v12);
        v13 = (TitanVertex) tx.getVertex(v13);
        assertEquals(2, Iterables.size(v1.query().direction(IN).labels("parent").edges()));
        assertEquals(1, Iterables.size(v1.query().direction(IN).labels("parent").has("weight", Compare.GREATER_THAN, 10.0).edges()));
        assertEquals(1, Iterables.size(v12.query().direction(OUT).labels("parent").has("weight").edges()));
        assertEquals(1, Iterables.size(v12.query().direction(OUT).labels("parent").has("weight", Compare.GREATER_THAN, 3).edges()));
        assertEquals(1, Iterables.size(v13.query().direction(OUT).labels("parent").has("weight").edges()));

        edge = Iterables.getOnlyElement(v1.getEdges(OUT, "connect"));
        assertEquals(0, edge.getPropertyKeys().size());


        // Verify vertex in current transaction
        TitanVertex v2 = tx.addVertex();
        v2.setProperty("uid", "v2");
        v2.setProperty("someid", 200l);
        try {
            v2.addProperty("int", 10.5);
            fail();
        } catch (IllegalArgumentException e) {
        }
        v2.setProperty("int", new SpecialInt(154));

        v2 = getVertex(id, "v2");
        assertEquals(154, ((SpecialInt) v2.getProperty("int")).getValue());
        assertEquals(v2, getVertex("someid", 200l));
        assertEquals(v2, getVertex(id, "v2"));

        assertEquals(5, Iterables.size(tx.getVertices()));

        assertEquals(1, Iterables.size(tx.query().has("someid", Cmp.GREATER_THAN, 150).vertices()));
        assertEquals(2, Iterables.size(tx.query().has("someid", Cmp.GREATER_THAN, 50).vertices()));

        clopen();


        v = tx.addVertex();
        v.setProperty("uid", "unique1");
        assertEquals(1, Iterables.size(tx.getVertices("uid", "unique1")));
        clopen();

        try {
            v = tx.addVertex();
            v.setProperty("uid", "unique1");
            fail();
        } catch (IllegalArgumentException e) {

        } finally {
            tx.rollback();
            tx = null;
        }
        clopen();
        v = tx.addVertex();
        v.setProperty("uid", "unique2");
        try {
            v = tx.addVertex();
            v.setProperty("uid", "unique2");
            fail();
        } catch (IllegalArgumentException e) {

        } finally {
            tx.rollback();
            tx = null;
        }


        makeVertexIndexedUniqueKey("domain",String.class);
        finishSchema();

        v1 = tx.addVertex();
        try {
            v1.setProperty("domain", "unique1");
        } catch (IllegalArgumentException e) {

        } finally {
            tx.rollback();
            tx = null;
        }
        newTx();


        v1 = tx.addVertex();
        v1.addProperty("domain", "unique1");
        try {
            v2 = tx.addVertex();
            v2.addProperty("domain", "unique1");
            fail();
        } catch (IllegalArgumentException e) {

        } finally {
            tx.rollback();
            tx = null;
        }
        newTx();

        clopen();
        v1 = tx.addVertex();
        v1.addProperty("domain", "unique1");
        assertEquals(1, Iterables.size(tx.getVertices("domain", "unique1")));
        try {
            v2 = tx.addVertex();
            v2.addProperty("domain", "unique1");
            fail();
        } catch (IllegalArgumentException e) {

        } finally {
            tx.rollback();
            tx = null;
        }
        newTx();
    }

    @Test
    public void testVertexRemoval() {
        Vertex v1 = graph.addVertex(null);
        Vertex v2 = graph.addVertex(null);

        Edge e = graph.addEdge(null, v1, v2, "knows");
        clopen();

        v1 = graph.getVertex(v1);
        v2 = graph.getVertex(v2);
        assertEquals(1,Iterables.size(v1.getEdges(Direction.BOTH)));
        assertEquals(1,Iterables.size(v2.getEdges(Direction.BOTH)));
        v2.remove();
        assertEquals(0,Iterables.size(v1.getEdges(Direction.BOTH)));
        try {
            assertEquals(0,Iterables.size(v2.getEdges(Direction.BOTH)));
            fail();
        } catch (IllegalArgumentException ex) {}


        graph.commit();
        assertNull(graph.getVertex(v2));

    }

    @Test
    public void testVertexDeletion() throws Exception {

        PropertyKey name = makeVertexIndexedUniqueKey("name",String.class);
        finishSchema();

        TitanVertex v = tx.addVertex();
        TitanProperty p = v.addProperty("name", "oldName");
        newTx();

        TitanVertex v1 = tx.getVertex(v.getID());

        tx.removeVertex(v1);
        newTx();
        if (graph.getFeatures().supportsVertexIteration) {
            int count = 0;
            for (Vertex vertex : tx.getVertices()) count++;
            assertEquals(0, count);
        }
    }

    @Test
    public void testMultivaluedVertexProperty() {

        /*
         * Constant test data
         *
         * The values list below must have at least two elements. The string
         * literals were chosen arbitrarily and have no special significance.
         */
        final String foo = "foo", bar = "bar", weight = "weight";
        final List<String> values =
                ImmutableList.of("four", "score", "and", "seven");
        assertTrue("Values list must have multiple elements for this test to make sense",
                2 <= values.size());

        // Create property with name pname and a vertex
        PropertyKey w = makeKey(weight,Integer.class);
        PropertyKey f = ((StandardPropertyKeyMaker)mgmt.makePropertyKey(foo)).dataType(String.class).cardinality(Cardinality.LIST).sortKey(w).sortOrder(Order.DESC).make();
        mgmt.buildIndex(foo,Vertex.class).indexKey(f).buildInternalIndex();
        PropertyKey b = mgmt.makePropertyKey(bar).dataType(String.class).cardinality(Cardinality.LIST).make();
        mgmt.buildIndex(bar,Vertex.class).indexKey(b).buildInternalIndex();
        finishSchema();

        TitanVertex v = tx.addVertex();

        // Insert prop values
        int i=0;
        for (String s : values) {
            TitanProperty p = v.addProperty(foo, s);
            p.setProperty(weight,++i);
            p = v.addProperty(bar, s);
            p.setProperty(weight,i);
        }

        //Verify correct number of properties
        assertEquals(values.size(), Iterables.size(v.getProperties(foo)));
        assertEquals(values.size(), Iterables.size(v.getProperties(bar)));
        //Verify order
        for (String prop : new String[]{foo,bar}) {
            int sum = 0;
            int index = values.size();
            for (TitanProperty p : v.getProperties(foo)) {
                assertTrue(values.contains(p.getValue()));
                int wint = p.getProperty(weight);
                sum+=wint;
                if (prop==foo) assertEquals(index,wint);
                index--;
            }
            assertEquals(values.size()*(values.size()+1)/2,sum);
        }


        assertEquals(1, Iterables.size(tx.query().has(foo, values.get(1)).vertices()));
        assertEquals(1, Iterables.size(tx.query().has(foo, values.get(3)).vertices()));

        assertEquals(1, Iterables.size(tx.query().has(bar, values.get(1)).vertices()));
        assertEquals(1, Iterables.size(tx.query().has(bar, values.get(3)).vertices()));

        // Check that removeProperty(TitanKey) returns a valid value
        String lastValueRemoved = v.removeProperty(foo);
        assertNotNull(lastValueRemoved);
        assertTrue(values.contains(lastValueRemoved));
        // Check that the properties were actually deleted from v
        assertFalse(v.getProperties(foo).iterator().hasNext());

        // Reopen database
        clopen();

        assertEquals(0, Iterables.size(tx.query().has(foo, values.get(1)).vertices()));
        assertEquals(0, Iterables.size(tx.query().has(foo, values.get(3)).vertices()));

        assertEquals(1, Iterables.size(tx.query().has(bar, values.get(1)).vertices()));
        assertEquals(1, Iterables.size(tx.query().has(bar, values.get(3)).vertices()));

        // Retrieve and check our test vertex
        v = tx.getVertex(v.getID());
        Iterable<TitanProperty> iter = v.getProperties(foo);
        assertFalse("Failed to durably remove multivalued property",
                iter.iterator().hasNext());

        assertEquals(values.size(), Iterables.size(v.getProperties(bar)));
        // Reinsert prop values
        for (String s : values) {
            v.addProperty(foo, s);
        }
        assertEquals(values.size(), Iterables.size(v.getProperties(foo)));

        // Test removeProperty(String) method on the vertex
        lastValueRemoved = v.removeProperty(foo);
        assertNotNull(lastValueRemoved);
        assertTrue(values.contains(lastValueRemoved));
        assertFalse(v.getProperties(foo).iterator().hasNext());
    }

    @Test
    public void testDate() throws ParseException {
        makeKey("birthday",GregorianCalendar.class);
        finishSchema();

        Vertex v = tx.addVertex();
        Date date = new SimpleDateFormat("ddMMyyyy").parse("28101978");
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        v.setProperty("birthday", c);
//        System.out.println(c); // prints Sat Oct 28 00:00:00 CET 1978

        clopen();

        v = tx.getVertex(v);
//        System.out.println(v.getProperty("birthday")); // prints Wed Jan 16 18:49:44 CET 2013
        assertEquals(c, v.getProperty("birthday"));
    }

    @Test
    public void testConfiguration() {
        //default type maker
        //check GraphDatabaseConfiguration

    }

    @Test
    public void testTransaction() {
        TitanTransaction tx1 = graph.newTransaction();
        TitanTransaction tx2 = graph.newTransaction();

        TitanVertex v11 = tx1.addVertex();
        TitanVertex v12 = tx1.addVertex();
        tx1.addEdge(v11, v12, "knows");

        TitanVertex v21 = tx2.addVertex();
        try {
            v21.addEdge("knows", v11);
            fail();
        } catch (IllegalArgumentException e) {
        }
        TitanVertex v22 = tx2.addVertex();
        v21.addEdge("knows", v22);
        tx2.commit();
        try {
            v22.addEdge("knows", v21);
            fail();
        } catch (IllegalStateException e) {
        }
        tx1.rollback();
        try {
            v11.setProperty("test", 5);
            fail();
        } catch (IllegalStateException e) {
        }

        //Test internal vertex id verification
        newTx();
        v21 = tx.getVertex(v21.getID());
        tx.makeEdgeLabel("link").unidirected().make();
        TitanVertex v3 = tx.addVertex();
        v21.addEdge("link", v3);
        newTx();
        v21 = tx.getVertex(v21.getID());
        v3 = (TitanVertex) Iterables.getOnlyElement(v21.getVertices(OUT, "link"));
        assertFalse(v3.isRemoved());
        v3.remove();
        newTx();
        v21 = tx.getVertex(v21.getID());
        v3 = (TitanVertex) Iterables.getOnlyElement(v21.getVertices(OUT, "link"));
        assertFalse(v3.isRemoved());
        newTx();


        TitanTransaction tx3 = graph.buildTransaction().checkInternalVertexExistence().start();
        v21 = tx3.getVertex(v21.getID());
        v3 = (TitanVertex) Iterables.getOnlyElement(v21.getVertices(OUT, "link"));
        assertTrue(v3.isRemoved());
        tx3.commit();


    }

    //Add more removal operations, different transaction contexts
    @Test
    public void testCreateDelete() {
        makeKey("weight",Double.class);
        PropertyKey id = makeVertexIndexedUniqueKey("uid",Integer.class);
        ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("knows")).sortKey(id).sortOrder(Order.DESC).directed().make();
        mgmt.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
        finishSchema();

        id = tx.getPropertyKey("uid");
        TitanVertex n1 = graph.addVertex(null), n3 = graph.addVertex(null);
        TitanEdge e = n3.addEdge("knows", n1);
        Edge e2 = n1.addEdge("friend",n3);
        e.setProperty(id, 111);
        n3.addProperty(id, 445);
        assertEquals(111, e.getProperty(id));
        graph.commit();

        n3 = graph.getVertex(n3.getID());
        assertEquals(445, n3.getProperty("uid"));
        e = (TitanEdge) Iterables.getOnlyElement(n3.getEdges(OUT, "knows"));
        assertEquals(111, e.getProperty("uid"));
        assertEquals(e, graph.getEdge(e.getId()));
        assertEquals(e, graph.getEdge(e.getId().toString()));
        TitanProperty p = Iterables.getOnlyElement(n3.getProperties("uid"));
        p.remove();
        n3.addProperty("uid", 353);

        e = (TitanEdge)Iterables.getOnlyElement(n3.getEdges(Direction.OUT,"knows"));
        e.setProperty(id,222);

        e2 = Iterables.getOnlyElement(n1.getEdges(OUT,"friend"));
        e2.setProperty("uid", 1);
        e2.setProperty("weight", 2.0);

        assertEquals(1,e2.getProperty("uid"));
        assertEquals(2.0,e2.getProperty("weight"));


        clopen();

        n3 = graph.getVertex(n3.getID());
        assertEquals(353, n3.getProperty("uid"));

        e = (TitanEdge)Iterables.getOnlyElement(n3.getEdges(Direction.OUT,"knows"));
        assertEquals(222,e.getProperty(id));
    }

    @Test
    public void testSelfLoop() {
        Vertex v = tx.addVertex();
        tx.addEdge(null, v, v, "self");
        assertEquals(1, Iterables.size(v.getEdges(Direction.OUT, "self")));
        assertEquals(1, Iterables.size(v.getEdges(Direction.IN, "self")));
        clopen();
        v = tx.getVertex(v.getId());
        assertNotNull(v);
        assertEquals(1, Iterables.size(v.getEdges(Direction.IN, "self")));
        assertEquals(1, Iterables.size(v.getEdges(Direction.OUT, "self")));
        assertEquals(1, Iterables.size(v.getEdges(Direction.IN, "self")));
    }

    @Test
    public void testIteration() {
        int numV = 50;
        int deleteV = 5;
        if (graph.getFeatures().supportsVertexIteration) {

            TitanVertex previous = tx.addVertex();
            previous.setProperty("count", 0);
            for (int i = 1; i < numV; i++) {
                TitanVertex next = tx.addVertex();
                next.setProperty("count", i);
                previous.addEdge("next", next);
                previous = next;
            }
            int numE = numV - 1;
            Iterable<Vertex> vertices = tx.getVertices();
            assertEquals(numV, Iterables.size(vertices));
            assertEquals(numV, Iterables.size(vertices));
            assertEquals(numV, Iterables.size(tx.getVertices()));
            Iterable<Edge> edges = tx.getEdges();
            assertEquals(numE, Iterables.size(edges));
            assertEquals(numE, Iterables.size(edges));
            assertEquals(numE, Iterables.size(tx.getEdges()));

            clopen();

            vertices = tx.getVertices();
            assertEquals(numV, Iterables.size(vertices));
            assertEquals(numV, Iterables.size(vertices));
            assertEquals(numV, Iterables.size(tx.getVertices()));
            edges = tx.getEdges();
            assertEquals(numE, Iterables.size(edges));
            assertEquals(numE, Iterables.size(edges));
            assertEquals(numE, Iterables.size(tx.getEdges()));

            Iterator<Vertex> viter = tx.getVertices().iterator();
            for (int i = 0; i < deleteV; i++) {
                Vertex v = viter.next();
                tx.removeVertex(v);
            }
            assertEquals(numV - deleteV, Iterables.size(tx.getVertices()));
            clopen();
            assertEquals(numV - deleteV, Iterables.size(graph.getVertices()));
        }
    }

    @Test
    public void testRepeatingIterationOverAllVertices() {
        if (graph.getFeatures().supportsVertexIteration) {
            TitanVertex vertex = tx.addVertex();
            vertex.setProperty("key", "value");
            tx.commit();
            for (int i = 0; i < 100; i++) {
                tx = graph.newTransaction();
                Iterable<Vertex> vertices = tx.getVertices();
                assertEquals(1, Iterables.size(vertices));
                assertEquals("value", Iterables.getOnlyElement(vertices).getProperty("key"));
                tx.commit();
            }
        }
    }

    @Test
    public void testVertexDeletionWithIndex() {
        PropertyKey name = tx.makePropertyKey("name").dataType(String.class).make();
        Vertex v1 = tx.addVertex();
        v1.setProperty("name", "v1");
        Vertex v2 = tx.addVertex();
        v2.setProperty("name", "v2");

        Edge e = tx.addEdge(null, v1, v2, "some_edge");

        clopen();

        v1 = tx.getVertex(v1);
        tx.removeVertex(v1);
        v2 = tx.getVertices("name", "v2").iterator().next(); // generates IllegalArgumentException
        assertNotNull(v2);
        assertEquals(0, Iterables.size(tx.getVertices("name", "v1")));
    }

    @Test
    public void testPropertyIndexPersistence() {
        final String propName = "favorite_color";
        final String sharedValue = "blue";

        tx.makePropertyKey(propName).dataType(String.class).make();

        TitanVertex alice = tx.addVertex();
        TitanVertex bob = tx.addVertex();

        alice.addProperty(propName, sharedValue);

        clopen();

        alice = tx.getVertex(alice.getID());
        bob = tx.getVertex(bob.getID());

        assertEquals(sharedValue, alice.getProperty(propName));
        assertEquals(null, bob.getProperty(propName));

        alice.removeProperty(propName);
        bob.addProperty(propName, sharedValue);

        clopen();
    }

    @Test
    public void testUnidirectional() {
        EdgeLabel link = tx.makeEdgeLabel("link").unidirected().multiplicity(Multiplicity.MANY2ONE).make();
        EdgeLabel connect = ((StandardEdgeLabelMaker)tx.makeEdgeLabel("connect")).sortKey(link).make();

        TitanVertex v1 = tx.addVertex(), v2 = tx.addVertex(), v3 = tx.addVertex();
        TitanEdge e = v1.addEdge(link, v2);
        e.setProperty("time", 5);
        e = v1.addEdge(connect, v2);
        e.setProperty("time", 10);
        e.setProperty("link", v3);
        e = v2.addEdge(connect, v3);
        e.setProperty("time", 15);
        e.setProperty("link", v1);

        assertEquals(2, Iterables.size(v1.getEdges(Direction.OUT)));
        assertEquals(1, Iterables.size(v2.getEdges(Direction.OUT)));
        assertEquals(2, Iterables.size(v2.getEdges(Direction.BOTH)));

        assertEquals(1, Iterables.size(v3.getEdges(Direction.BOTH)));
        e = (TitanEdge) Iterables.getOnlyElement(v1.getEdges(Direction.OUT, "connect"));
        assertEquals(10, e.getProperty("time"));
        assertEquals(v3, e.getProperty("link"));

        clopen();

        v1 = (TitanVertex) tx.getVertex(v1);
        v2 = (TitanVertex) tx.getVertex(v2);
        v3 = (TitanVertex) tx.getVertex(v3);
        assertEquals(2, Iterables.size(v1.getEdges(Direction.OUT)));
        assertEquals(1, Iterables.size(v2.getEdges(Direction.OUT)));
        assertEquals(2, Iterables.size(v2.getEdges(Direction.BOTH)));

        assertEquals(1, Iterables.size(v3.getEdges(Direction.BOTH)));
        e = (TitanEdge) Iterables.getOnlyElement(v1.getEdges(Direction.OUT, "connect"));
        assertEquals(10, e.getProperty("time"));
        assertEquals(v3, e.getProperty("link"));

    }

    @Test
    public void testJointIndexRetrieval() {
        makeVertexIndexedKey("name",String.class);
        makeVertexIndexedKey("color",String.class);
        finishSchema();

        Vertex v = graph.addVertex(null);
        v.setProperty("name", "ilya");
        v.setProperty("color", "blue");
        graph.commit();

        assertEquals(1, Iterables.size(graph.query().has("name", "ilya").vertices()));
        assertEquals(1, Iterables.size(graph.query().has("name", "ilya").has("color", "blue").vertices()));
    }

    @Test
    public void testLargeJointIndexRetrieval() {
        makeVertexIndexedKey("sid",Integer.class);
        makeVertexIndexedKey("color",String.class);
        finishSchema();

        int sids = 17;
        String[] colors = {"blue", "red", "yellow", "brown", "green", "orange", "purple"};
        int multiplier = 200;
        int numV = sids * colors.length * multiplier;
        for (int i = 0; i < numV; i++) {
            Vertex v = graph.addVertex(null);
            v.setProperty("color", colors[i % colors.length]);
            v.setProperty("sid", i % sids);
        }
        graph.commit();
        clopen();

        assertEquals(numV / sids, Iterables.size(graph.query().has("sid", 8).vertices()));
        assertEquals(numV / colors.length, Iterables.size(graph.query().has("color", colors[2]).vertices()));

        assertEquals(multiplier, Iterables.size(graph.query().has("sid", 11).has("color", colors[3]).vertices()));
    }


    @Test
    public void testIndexRetrieval() {
        PropertyKey id = mgmt.makePropertyKey("uid").dataType(Integer.class).make();
        mgmt.buildIndex("vuid",Vertex.class).unique().indexKey(id).buildInternalIndex();
        mgmt.buildIndex("euid",Edge.class).indexKey(id).buildInternalIndex();

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.buildIndex("vname",Vertex.class).indexKey(name).buildInternalIndex();
        mgmt.buildIndex("ename",Edge.class).indexKey(name).buildInternalIndex();
        mgmt.makeEdgeLabel("connect").signature(id, name).make();
        finishSchema();


        int noNodes = 100;
        int div = 10;
        int mod = noNodes / div;
        for (int i = 0; i < noNodes; i++) {
            TitanVertex n = tx.addVertex();
            n.addProperty("uid", i);
            n.addProperty("name", "Name" + (i % mod));
            TitanVertex other = getVertex("uid", Math.max(0, i - 1));
            Preconditions.checkNotNull(other);
            TitanEdge e = n.addEdge("connect", other);
            e.setProperty("uid", i);
            e.setProperty("name", "Edge" + (i % mod));
        }
        clopen();
        for (int j = 0; j < mod; j++) {
            Iterable<Vertex> nodes = tx.getVertices("name", "Name" + j);
            assertEquals(div, Iterables.size(nodes));
            for (Vertex n : nodes) {
                int nid = ((Number) n.getProperty("uid")).intValue();
                assertEquals(j, nid % mod);
            }
            Iterable<Edge> edges = tx.getEdges("name", "Edge" + j);
            assertEquals(div, Iterables.size(edges));
            for (Edge e : edges) {
                int nid = ((Number) e.getProperty("uid")).intValue();
                assertEquals(j, nid % mod);
            }
        }
        clopen();
        for (int i = 0; i < noNodes; i++) {
            assertEquals(getVertex("uid", i).getProperty("name").toString().substring(4), String.valueOf(i % mod));
        }
        for (int i = 0; i < noNodes; i++) {
            assertEquals(Iterables.getOnlyElement(tx.getEdges("uid", i)).getProperty("name").toString().substring(4), String.valueOf(i % mod));
        }

    }

    @Test
    public void testThreadBoundTx() {
        PropertyKey t = mgmt.makePropertyKey("type").dataType(Integer.class).make();
        mgmt.buildIndex("etype",Edge.class).indexKey(t).buildInternalIndex();
        ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("friend")).sortKey(t).make();
        finishSchema();

        Vertex v1 = graph.addVertex(null);
        Vertex v2 = graph.addVertex(null);
        Vertex v3 = graph.addVertex(null);
        v1.setProperty("name", "Vertex1");
        v1.setProperty("age", 35);
        v2.setProperty("name", "Vertex2");
        v2.setProperty("age", 45);
        v3.setProperty("name", "Vertex3");
        v3.setProperty("age", 55);
        Edge e1 = v1.addEdge("knows", v2);
        e1.setProperty("time", 5);
        Edge e2 = v2.addEdge("knows", v3);
        e2.setProperty("time", 15);
        Edge e3 = v3.addEdge("knows", v1);
        e3.setProperty("time", 25);
        Edge e4 = v2.addEdge("friend", v2);
        e4.setProperty("type", 1);
        for (Vertex v : new Vertex[]{v1, v2, v3}) {
            assertEquals(2, v.query().direction(Direction.BOTH).labels("knows").count());
            assertEquals(1, v.query().direction(Direction.OUT).labels("knows").count());
            assertEquals(5, ((Number) Iterables.getOnlyElement(v.getEdges(Direction.OUT, "knows")).getProperty("time")).intValue() % 10);
        }
        e3.setProperty("time", 35);
        assertEquals(35, e3.getProperty("time"));

        v1.addEdge("friend", v2).setProperty("type", 0);
        graph.commit();
        e4.setProperty("type", 2);
        Edge ef = Iterables.getOnlyElement(v1.getEdges(OUT, "friend"));
        assertEquals(ef, Iterables.getOnlyElement(graph.getEdges("type", 0)));
        ef.setProperty("type", 1);
        graph.commit();

        assertEquals(35, e3.getProperty("time"));
        e3 = graph.getEdge(e3);
        e3.setProperty("time", 45);
        assertEquals(45, e3.getProperty("time"));

        assertEquals(15, e2.getProperty("time"));
        e2.setProperty("time", 25);
        assertEquals(25, e2.getProperty("time"));

        assertEquals(35, v1.getProperty("age"));
        assertEquals(55, v3.getProperty("age"));
        v3.setProperty("age", 65);
        assertEquals(65, v3.getProperty("age"));
        e1 = graph.getEdge(e1);

        for (Vertex v : new Vertex[]{v1, v2, v3}) {
            assertEquals(2, v.query().direction(Direction.BOTH).labels("knows").count());
            assertEquals(1, v.query().direction(Direction.OUT).labels("knows").count());
            assertEquals(5, ((Number) Iterables.getOnlyElement(v.getEdges(Direction.OUT, "knows")).getProperty("time")).intValue() % 10);
        }

        graph.commit();

        assertEquals(45, e3.getProperty("time"));
        assertEquals(5, e1.getProperty("time"));

//        try {
//            //TODO: how to address this? Only allow transactional passing for vertices?
//            assertEquals(25, e2.getProperty("time"));
//            fail();
//        } catch (InvalidElementException e) {
//        }

        assertEquals(35, v1.getProperty("age"));
        assertEquals(65, v3.getProperty("age"));

        for (Vertex v : new Vertex[]{v1, v2, v3}) {
            assertEquals(2, v.query().direction(Direction.BOTH).labels("knows").count());
            assertEquals(1, v.query().direction(Direction.OUT).labels("knows").count());
            assertEquals(5, ((Number) Iterables.getOnlyElement(v.getEdges(Direction.OUT, "knows")).getProperty("time")).intValue() % 10);
        }

        graph.commit();

        v1 = graph.addVertex(null);
        v2 = graph.addVertex(null);
        graph.addEdge(null, v1, v2, "knows");
        graph.commit();
        v3 = graph.addVertex(null);
        Edge e = graph.addEdge(null, v1, v3, "knows");
        assertNull(e.getProperty("age"));
    }


    //Test all element methods: vertex, edge, property, relation, element
    @Test
    public void testCreateAndRetrieveComprehensive() {
        makeLabel("connect");
        makeVertexIndexedUniqueKey("name",String.class);
        PropertyKey weight = makeKey("weight",Double.class);
        PropertyKey id = makeVertexIndexedUniqueKey("uid",Integer.class);
        ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("knows")).sortKey(id).signature(weight).make();
        finishSchema();

        PropertyKey name = tx.getPropertyKey("name");
        assertNotNull(name);
        EdgeLabel connect = tx.getEdgeLabel("connect");
        assertNotNull(connect);
        EdgeLabel knows = tx.getEdgeLabel("knows");
        assertNotNull(knows);
        assertTrue(knows.isEdgeLabel());

        TitanVertex n1 = tx.addVertex();
        TitanVertex n2 = tx.addVertex();
        TitanVertex n3 = tx.addVertex();
        assertNotNull(n1.toString());
        n1.addProperty(name, "Node1");
        n2.addProperty(name, "Node2");
        n3.addProperty("weight", 5.0);
        TitanEdge e = n1.addEdge(connect, n2);
        assertNotNull(e.toString());
        assertEquals(n1, e.getVertex(OUT));
        assertEquals(n2, e.getVertex(IN));
        e = n2.addEdge(knows, n3);
        e.setProperty("weight", 3.0);
        e.setProperty(name, "HasProperties TitanRelation");
        e = n3.addEdge(knows, n1);
        n3.addEdge(connect, n3);
        e.setProperty("uid", 111);
        assertEquals(4, Iterables.size(n3.getEdges()));
        assertEquals(2, Iterables.size(n3.getEdges(Direction.OUT)));
        assertEquals(2, Iterables.size(n3.getEdges(Direction.IN)));

        clopen();

        connect = tx.getEdgeLabel("connect");
        assertEquals(connect.getName(), "connect");
        assertTrue(connect.isDirected());
        name = tx.getPropertyKey("name");
        weight = tx.getPropertyKey("weight");
        id = tx.getPropertyKey("uid");
        knows = tx.getEdgeLabel("knows");
        log.debug("Loaded edge types");
        n2 = getVertex(name, "Node2");
        assertNotNull(n2.toString());
        assertEquals("Node2", n2.getProperty(name));
        e = Iterables.getOnlyElement(n2.getTitanEdges(BOTH, connect));
        assertNotNull(e.toString());
        n1 = e.getVertex(OUT);
        log.debug("Retrieved node!");
        assertEquals(n1, e.getVertex(OUT));
        assertEquals(n2, e.getVertex(IN));

        log.debug("First:");
        assertEquals(e, Iterables.getOnlyElement(n2.getEdges(IN)));
        log.debug("Second:");
        assertEquals(e, Iterables.getOnlyElement(n1.getEdges(OUT)));

        assertEquals(1, Iterables.size(n2.getTitanEdges(BOTH, tx.getEdgeLabel("knows"))));

        assertEquals(1, Iterables.size(n1.getTitanEdges(BOTH, tx.getEdgeLabel("knows"))));
        assertEquals(2, Iterables.size(n1.getEdges()));

        log.debug("Third:");
        assertEquals(e, Iterables.getOnlyElement(n2.getEdges(IN, "connect")));
        log.debug("Four:");
        assertEquals(e, Iterables.getOnlyElement(n1.getTitanEdges(OUT, connect)));

        log.debug("Fith:");
        assertEquals(e, Iterables.getOnlyElement(n2.getEdges(BOTH, "connect")));
        log.debug("Sixth:");
        assertEquals(e, Iterables.getOnlyElement(n1.getTitanEdges(BOTH, connect)));

        e = Iterables.getOnlyElement(n2.getTitanEdges(OUT, tx.getEdgeLabel("knows")));
        assertTrue(e.getProperty(weight).equals(3.0));
        assertEquals("HasProperties TitanRelation", e.getProperty(name));
        n3 = e.getVertex(IN);

        e = Iterables.getOnlyElement(n3.getTitanEdges(OUT, tx.getEdgeLabel("knows")));
        assertEquals(111, e.getProperty(id));

        assertEquals(4, Iterables.size(n3.getEdges()));
        assertEquals(2, Iterables.size(n3.getEdges(Direction.OUT)));
        assertEquals(2, Iterables.size(n3.getEdges(Direction.IN)));


        //Delete Edges, create new ones
        e = Iterables.getOnlyElement(n2.getTitanEdges(OUT, tx.getEdgeLabel("knows")));
        e.remove();
        assertEquals(0, Iterables.size(n2.getTitanEdges(BOTH, tx.getEdgeLabel("knows"))));
        assertEquals(1, Iterables.size(n3.getTitanEdges(BOTH, tx.getEdgeLabel("knows"))));

        e = n2.addEdge(knows, n1);
        e.setProperty(weight, 111.5);
        e.setProperty(name, "New TitanRelation");
        assertEquals(1, Iterables.size(n2.getEdges(BOTH, "knows")));
        assertEquals(2, Iterables.size(n1.getEdges(BOTH, "knows")));

        clopen();
        n2 = getVertex("name", "Node2");
        e = Iterables.getOnlyElement(n2.getTitanEdges(OUT, tx.getEdgeLabel("knows")));
        assertEquals("New TitanRelation", e.getProperty(tx.getPropertyKey("name")));
        assertTrue(e.getProperty("weight").equals(111.5));

    }

    @Test
    public void testQuery() {
        makeVertexIndexedUniqueKey("name",String.class);
        PropertyKey time = makeKey("time",Integer.class);
        PropertyKey weight = makeKey("weight",Precision.class);

        EdgeLabel author = mgmt.makeEdgeLabel("author").multiplicity(Multiplicity.MANY2ONE).unidirected().make();

        ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("connect")).sortKey(time).make();
        ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("connectDesc")).sortKey(time).sortOrder(Order.DESC).make();
        ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("friend")).sortKey(weight, time).sortOrder(Order.ASC).signature(author).make();
        ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("friendDesc")).sortKey(weight, time).sortOrder(Order.DESC).signature(author).make();
        ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("knows")).sortKey(author, weight).make();
        mgmt.makeEdgeLabel("follows").make();
        finishSchema();

        TitanVertex v = tx.addVertex();
        v.addProperty("name","v");
        TitanVertex u = tx.addVertex();
        u.addProperty("name","u");
        int noVertices = 10000;
        assertEquals(0,(noVertices-1)%3);
        TitanVertex[] vs = new TitanVertex[noVertices];
        for (int i = 1; i < noVertices; i++) {
            vs[i] = tx.addVertex();
            vs[i].addProperty("name", "v" + i);
        }
        EdgeLabel[] labelsV = {tx.getEdgeLabel("connect"),tx.getEdgeLabel("friend"),tx.getEdgeLabel("knows")};
        EdgeLabel[] labelsU = {tx.getEdgeLabel("connectDesc"),tx.getEdgeLabel("friendDesc"),tx.getEdgeLabel("knows")};
        for (int i = 1; i < noVertices; i++) {
            for (TitanVertex vertex : new TitanVertex[]{v,u}) {
                for (Direction d : new Direction[]{OUT,IN}) {
                    EdgeLabel label = vertex==v?labelsV[i%3]:labelsU[i%3];
                    TitanEdge e = d==OUT?vertex.addEdge(label,vs[i]):
                                         vs[i].addEdge(label,vertex);
                    e.setProperty("time", i);
                    e.setProperty("weight", i % 4 + 0.5);
                    e.setProperty("name", "e" + i);
                    e.setProperty("author", i%5==0?v:vs[i % 5]);
                }
            }
        }
        int edgesPerLabel = noVertices/3;



        VertexList vl;
        Map<TitanVertex, Iterable<TitanEdge>> results;
        Map<TitanVertex, Iterable<TitanProperty>> results2;
        TitanVertex[] qvs;
        int lastTime;
        Iterator<Edge> outer;

        clopen();

        long[] vidsubset = new long[31 - 3];
        for (int i = 0; i < vidsubset.length; i++) vidsubset[i] = vs[i + 3].getID();
        Arrays.sort(vidsubset);

        //##################################################
        //Queries from Cache
        //##################################################
        clopen();
        for (int i = 1; i < noVertices; i++) vs[i] = tx.getVertex(vs[i].getID());
        v = tx.getVertex(v.getID());
        u = tx.getVertex(u.getID());
        qvs = new TitanVertex[]{vs[6], vs[9], vs[12], vs[15], vs[60]};

        //To trigger queries from cache (don't copy!!!)
        assertEquals(2*(noVertices-1), Iterables.size(v.getEdges()));


        assertEquals(10, Iterables.size(v.query().labels("connect").limit(10).vertices()));
        assertEquals(10, Iterables.size(u.query().labels("connectDesc").limit(10).vertices()));
        assertEquals(10, Iterables.size(v.query().labels("connect").has("time", Compare.GREATER_THAN, 30).limit(10).vertices()));
        assertEquals(10, Iterables.size(u.query().labels("connectDesc").has("time", Compare.GREATER_THAN, 30).limit(10).vertices()));

        lastTime = 0;
        for (Edge e : v.query().labels("connect").direction(OUT).limit(20).edges()) {
            int nowTime = e.getProperty("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime <= nowTime);
            lastTime = nowTime;
        }
        lastTime = Integer.MAX_VALUE;
        for (Edge e : u.query().labels("connectDesc").direction(OUT).limit(20).edges()) {
            int nowTime = e.getProperty("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime >= nowTime);
            lastTime = nowTime;
        }
        assertEquals(10, Iterables.size(v.query().labels("connect").direction(OUT).has("time", Compare.GREATER_THAN, 60).limit(10).vertices()));
        assertEquals(10, Iterables.size(u.query().labels("connectDesc").direction(OUT).has("time", Compare.GREATER_THAN, 60).limit(10).vertices()));

        outer = v.query().labels("connect").direction(OUT).limit(20).edges().iterator();
        for (Edge e : v.query().labels("connect").direction(OUT).limit(10).edges()) {
            assertEquals(e, outer.next());
        }
        assertEquals(10, v.query().labels("connect").direction(OUT).interval("time", 3, 31).count());
        assertEquals(10, u.query().labels("connectDesc").direction(OUT).interval("time", 3, 31).count());
        assertEquals(10, v.query().labels("connect").direction(IN).interval("time", 3, 31).count());
        assertEquals(10, u.query().labels("connectDesc").direction(IN).interval("time", 3, 31).count());
        assertEquals(0, v.query().labels("connect").direction(OUT).has("time", null).count());
        assertEquals(10, v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertexIds().size());
        assertEquals(edgesPerLabel-10, v.query().labels("connect").direction(OUT).has("time", Compare.GREATER_THAN, 31).count());
        assertEquals(10, Iterables.size(v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertices()));
        assertEquals(3, v.query().labels("friend").direction(OUT).limit(3).count());
        assertEquals(3, v.query().labels("friend").direction(OUT).has("weight", 0.5).limit(3).count());
        assertEquals(3, v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5).count());
        assertEquals(3, u.query().labels("friendDesc").direction(OUT).interval("time", 3, 33).has("weight", 0.5).count());
        assertEquals(1, v.query().labels("friend").direction(OUT).has("weight", 0.5).interval("time", 4, 10).count());
        assertEquals(1, u.query().labels("friendDesc").direction(OUT).has("weight", 0.5).interval("time", 4, 10).count());
        assertEquals(3, v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5).count());
        assertEquals(4, v.query().labels("friend").direction(OUT).has("time", Compare.LESS_THAN_EQUAL, 10).count());
        assertEquals(edgesPerLabel-4, v.query().labels("friend").direction(OUT).has("time", Compare.GREATER_THAN, 10).count());
        assertEquals(20, v.query().labels("friend", "connect").direction(OUT).interval("time", 3, 33).count());

        assertEquals((int)Math.ceil(edgesPerLabel/5.0), v.query().labels("knows").direction(OUT).has("author", v).count());
        assertEquals((int)Math.ceil(edgesPerLabel/5.0), v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 0.0, 4.0).count());
        assertEquals((int)Math.ceil(edgesPerLabel/(5.0*2)), v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 0.0, 2.0).count());
        assertEquals((int)Math.floor(edgesPerLabel/(5.0*2)), v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 2.1, 4.0).count());
        assertEquals(20, Iterables.size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertices()));
        assertEquals(20, Iterables.size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertexIds()));
        assertEquals(30, v.query().labels("friend", "connect", "knows").direction(OUT).interval("time", 3, 33).count());
        assertEquals(noVertices-2, v.query().labels("friend", "connect", "knows").direction(OUT).has("time", Compare.NOT_EQUAL, 10).count());

        assertEquals(0, v.query().has("age", null).labels("undefined").direction(OUT).count());
        assertEquals(1, v.query().labels("connect").direction(OUT).adjacent(vs[6]).has("time", 6).count());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacent(vs[11]).count());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacent(vs[11]).has("weight", 3.5).count());
        assertEquals(2, v.query().labels("connect").adjacent(vs[6]).has("time", 6).count());
        assertEquals(0, v.query().labels("connect").adjacent(vs[8]).has("time", 8).count());

        assertEquals(edgesPerLabel, v.query().labels("connect").direction(OUT).count());
        assertEquals(edgesPerLabel, v.query().labels("connect").direction(IN).count());
        assertEquals(2*edgesPerLabel, v.query().labels("connect").direction(BOTH).count());

        assertEquals(edgesPerLabel, v.query().labels("connect").has("undefined", null).direction(OUT).count());
        assertEquals(2*(int)Math.ceil((noVertices-1)/4.0), Iterables.size(v.query().labels("connect", "friend", "knows").has("weight", 1.5).vertexIds()));
        assertEquals(1, v.query().direction(IN).has("time", 1).count());
        assertEquals(10, v.query().direction(OUT).interval("time", 4, 14).count());
        assertEquals(9, v.query().direction(IN).interval("time", 4, 14).has("time", Compare.NOT_EQUAL, 10).count());
        assertEquals(9, v.query().direction(OUT).interval("time", 4, 14).has("time", Compare.NOT_EQUAL, 10).count());
        assertEquals(noVertices-1, Iterables.size(v.query().direction(OUT).vertices()));
        assertEquals(noVertices-1, Iterables.size(v.query().direction(IN).vertices()));
        for (Direction dir : new Direction[]{IN,OUT}) {
            vl = v.query().labels().direction(dir).interval("time", 3, 31).vertexIds();
            vl.sort();
            for (int i = 0; i < vl.size(); i++) assertEquals(vidsubset[i], vl.getID(i));
        }
        assertEquals(2*(noVertices-1), Iterables.size(v.getEdges()));


        //Property queries
        assertEquals(1, Iterables.size(v.query().properties()));
        assertEquals(1, Iterables.size(v.query().keys("name").properties()));

        //MultiQueries
        results = tx.multiQuery(qvs).direction(IN).labels("connect").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(1, Iterables.size(result));
        results = tx.multiQuery(Sets.newHashSet(qvs)).labels("connect").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(2, Iterables.size(result));
        results = tx.multiQuery(qvs).labels("knows").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(0, Iterables.size(result));
        results = tx.multiQuery(qvs).titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(4, Iterables.size(result));
        results2 = tx.multiQuery(qvs).properties();
        for (Iterable<TitanProperty> result : results2.values()) assertEquals(1, Iterables.size(result));
        results2 = tx.multiQuery(qvs).keys("name").properties();
        for (Iterable<TitanProperty> result : results2.values()) assertEquals(1, Iterables.size(result));

        //##################################################
        //Same queries as above but without memory loading (i.e. omitting the first query)
        //##################################################
        clopen();
        for (int i = 1; i < noVertices; i++) vs[i] = tx.getVertex(vs[i].getID());
        v = tx.getVertex(v.getID());
        u = tx.getVertex(u.getID());
        qvs = new TitanVertex[]{vs[6], vs[9], vs[12], vs[15], vs[60]};

        assertEquals(10, Iterables.size(v.query().labels("connect").limit(10).vertices()));
        assertEquals(10, Iterables.size(u.query().labels("connectDesc").limit(10).vertices()));
        assertEquals(10, Iterables.size(v.query().labels("connect").has("time", Compare.GREATER_THAN, 30).limit(10).vertices()));
        assertEquals(10, Iterables.size(u.query().labels("connectDesc").has("time", Compare.GREATER_THAN, 30).limit(10).vertices()));

        lastTime = 0;
        for (Edge e : v.query().labels("connect").direction(OUT).limit(20).edges()) {
            int nowTime = e.getProperty("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime <= nowTime);
            lastTime = nowTime;
        }
        lastTime = Integer.MAX_VALUE;
        for (Edge e : u.query().labels("connectDesc").direction(OUT).limit(20).edges()) {
            int nowTime = e.getProperty("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime >= nowTime);
            lastTime = nowTime;
        }
        assertEquals(10, Iterables.size(v.query().labels("connect").direction(OUT).has("time", Compare.GREATER_THAN, 60).limit(10).vertices()));
        assertEquals(10, Iterables.size(u.query().labels("connectDesc").direction(OUT).has("time", Compare.GREATER_THAN, 60).limit(10).vertices()));

        outer = v.query().labels("connect").direction(OUT).limit(20).edges().iterator();
        for (Edge e : v.query().labels("connect").direction(OUT).limit(10).edges()) {
            assertEquals(e, outer.next());
        }
        assertEquals(10, v.query().labels("connect").direction(OUT).interval("time", 3, 31).count());
        assertEquals(10, u.query().labels("connectDesc").direction(OUT).interval("time", 3, 31).count());
        assertEquals(10, v.query().labels("connect").direction(IN).interval("time", 3, 31).count());
        assertEquals(10, u.query().labels("connectDesc").direction(IN).interval("time", 3, 31).count());
        assertEquals(0, v.query().labels("connect").direction(OUT).has("time", null).count());
        assertEquals(10, v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertexIds().size());
        assertEquals(edgesPerLabel-10, v.query().labels("connect").direction(OUT).has("time", Compare.GREATER_THAN, 31).count());
        assertEquals(10, Iterables.size(v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertices()));
        assertEquals(3, v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5).count());
        assertEquals(3, u.query().labels("friendDesc").direction(OUT).interval("time", 3, 33).has("weight", 0.5).count());
        assertEquals(1, v.query().labels("friend").direction(OUT).has("weight", 0.5).interval("time", 4, 10).count());
        assertEquals(1, u.query().labels("friendDesc").direction(OUT).has("weight", 0.5).interval("time", 4, 10).count());
        assertEquals(3, v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5).count());
        assertEquals(4, v.query().labels("friend").direction(OUT).has("time", Compare.LESS_THAN_EQUAL, 10).count());
        assertEquals(edgesPerLabel-4, v.query().labels("friend").direction(OUT).has("time", Compare.GREATER_THAN, 10).count());
        assertEquals(20, v.query().labels("friend", "connect").direction(OUT).interval("time", 3, 33).count());

        assertEquals((int)Math.ceil(edgesPerLabel/5.0), v.query().labels("knows").direction(OUT).has("author", v).count());
        assertEquals((int)Math.ceil(edgesPerLabel/5.0), v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 0.0, 4.0).count());
        assertEquals((int)Math.ceil(edgesPerLabel/(5.0*2)), v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 0.0, 2.0).count());
        assertEquals((int)Math.floor(edgesPerLabel/(5.0*2)), v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 2.1, 4.0).count());
        assertEquals(20, Iterables.size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertices()));
        assertEquals(20, Iterables.size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertexIds()));
        assertEquals(30, v.query().labels("friend", "connect", "knows").direction(OUT).interval("time", 3, 33).count());
        assertEquals(noVertices-2, v.query().labels("friend", "connect", "knows").direction(OUT).has("time", Compare.NOT_EQUAL, 10).count());

        assertEquals(0, v.query().has("age", null).labels("undefined").direction(OUT).count());
        assertEquals(1, v.query().labels("connect").direction(OUT).adjacent(vs[6]).has("time", 6).count());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacent(vs[11]).count());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacent(vs[11]).has("weight", 3.5).count());
        assertEquals(2, v.query().labels("connect").adjacent(vs[6]).has("time", 6).count());
        assertEquals(0, v.query().labels("connect").adjacent(vs[8]).has("time", 8).count());

        assertEquals(edgesPerLabel, v.query().labels("connect").direction(OUT).count());
        assertEquals(edgesPerLabel, v.query().labels("connect").direction(IN).count());
        assertEquals(2*edgesPerLabel, v.query().labels("connect").direction(BOTH).count());

        assertEquals(edgesPerLabel, v.query().labels("connect").has("undefined", null).direction(OUT).count());
        assertEquals(2*(int)Math.ceil((noVertices-1)/4.0), Iterables.size(v.query().labels("connect", "friend", "knows").has("weight", 1.5).vertexIds()));
        assertEquals(1, v.query().direction(IN).has("time", 1).count());
        assertEquals(10, v.query().direction(OUT).interval("time", 4, 14).count());
        assertEquals(9, v.query().direction(IN).interval("time", 4, 14).has("time", Compare.NOT_EQUAL, 10).count());
        assertEquals(9, v.query().direction(OUT).interval("time", 4, 14).has("time", Compare.NOT_EQUAL, 10).count());
        assertEquals(noVertices-1, Iterables.size(v.query().direction(OUT).vertices()));
        assertEquals(noVertices-1, Iterables.size(v.query().direction(IN).vertices()));
        for (Direction dir : new Direction[]{IN,OUT}) {
            vl = v.query().labels().direction(dir).interval("time", 3, 31).vertexIds();
            vl.sort();
            for (int i = 0; i < vl.size(); i++) assertEquals(vidsubset[i], vl.getID(i));
        }
        assertEquals(2*(noVertices-1), Iterables.size(v.getEdges()));


        //Property queries
        assertEquals(1, Iterables.size(v.query().properties()));
        assertEquals(1, Iterables.size(v.query().keys("name").properties()));

        //MultiQueries
        results = tx.multiQuery(qvs).direction(IN).labels("connect").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(1, Iterables.size(result));
        results = tx.multiQuery(Sets.newHashSet(qvs)).labels("connect").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(2, Iterables.size(result));
        results = tx.multiQuery(qvs).labels("knows").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(0, Iterables.size(result));
        results = tx.multiQuery(qvs).titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(4, Iterables.size(result));
        results2 = tx.multiQuery(qvs).properties();
        for (Iterable<TitanProperty> result : results2.values()) assertEquals(1, Iterables.size(result));
        results2 = tx.multiQuery(qvs).keys("name").properties();
        for (Iterable<TitanProperty> result : results2.values()) assertEquals(1, Iterables.size(result));

        //##################################################
        //End copied queries
        //##################################################

        newTx();

        v = (TitanVertex) tx.getVertices("name", "v").iterator().next();
        assertNotNull(v);
        assertEquals(2, v.query().has("weight", 1.5).interval("time", 10, 30).limit(2).vertexIds().size());
        assertEquals(10, v.query().has("weight", 1.5).interval("time", 10, 30).vertexIds().size());

        newTx();

        v = (TitanVertex) tx.getVertices("name", "v").iterator().next();
        assertNotNull(v);
        assertEquals(2, v.query().has("weight", 1.5).interval("time", 10, 30).limit(2).count());
        assertEquals(10, v.query().has("weight", 1.5).interval("time", 10, 30).count());


        newTx();
        //Test partially new vertex queries
        TitanVertex[] qvs2 = new TitanVertex[qvs.length+2];
        qvs2[0]=tx.addVertex();
        for (int i=0;i<qvs.length;i++) qvs2[i+1]=tx.getVertex(qvs[i].getID());
        qvs2[qvs2.length-1]=tx.addVertex();
        qvs2[0].addEdge("connect",qvs2[qvs2.length-1]);
        qvs2[qvs2.length-1].addEdge("connect", qvs2[0]);
        results = tx.multiQuery(qvs2).direction(IN).labels("connect").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(1, Iterables.size(result));

    }

    //Merge above
    public void neighborhoodTest() {
        testCreateAndRetrieveComprehensive();
        log.debug("Neighborhood:");
        TitanVertex n1 = getVertex("name", "Node1");
        TitanVertexQuery q = n1.query().direction(OUT).types(tx.getEdgeLabel("connect"));
        VertexList res = q.vertexIds();
        assertEquals(1, res.size());
        TitanVertex n2 = getVertex("name", "Node2");
        assertEquals(n2.getID(), res.getID(0));
    }

    /**
     * Testing using hundreds of objects
     */
    @Test
    public void createAndRetrieveMedium() {
        //Create Graph
        makeLabel("connect");
        makeVertexIndexedUniqueKey("name",String.class);
        PropertyKey weight = makeKey("weight",Double.class);
        PropertyKey id = makeVertexIndexedUniqueKey("uid",Integer.class);
        ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("knows")).sortKey(id).signature(weight).make();
        finishSchema();

        //Create Nodes
        id = tx.getPropertyKey("uid");
        int noNodes = 500;
        String[] names = new String[noNodes];
        int[] ids = new int[noNodes];
        TitanVertex[] nodes = new TitanVertex[noNodes];
        long[] nodeIds = new long[noNodes];
        List[] nodeEdges = new List[noNodes];
        for (int i = 0; i < noNodes; i++) {
            names[i] = "vertex" + i;//RandomGenerator.randomString();
            ids[i] = i;//RandomGenerator.randomInt(1, Integer.MAX_VALUE / 4);
            nodes[i] = tx.addVertex();
            nodes[i].addProperty("name", names[i]);
            nodes[i].addProperty("uid", ids[i]);
            if ((i + 1) % 100 == 0) log.debug("Added 100 nodes");
        }
        log.debug("Nodes created");
        int[] connectOff = {-100, -34, -4, 10, 20};
        int[] knowsOff = {-400, -18, 8, 232, 334};
        for (int i = 0; i < noNodes; i++) {
            TitanVertex n = nodes[i];
            nodeEdges[i] = new ArrayList(10);
            for (int c : connectOff) {
                TitanEdge r = n.addEdge("connect", nodes[wrapAround(i + c, noNodes)]);
                nodeEdges[i].add(r);
            }
            for (int k : knowsOff) {
                TitanVertex n2 = nodes[wrapAround(i + k, noNodes)];
                TitanEdge r = n.addEdge("knows", n2);
                r.setProperty(id, ((Number) n.getProperty(id)).intValue() + ((Number) n2.getProperty(id)).intValue());
                r.setProperty("weight", k * 1.5);
                r.setProperty("name", i + "-" + k);
                nodeEdges[i].add(r);
            }
            if (i % 100 == 99) log.debug(".");
        }

        tx.commit();
        tx = null;
        Set[] nodeEdgeIds = new Set[noNodes];
        for (int i = 0; i < noNodes; i++) {
            nodeIds[i] = nodes[i].getID();
            nodeEdgeIds[i] = new HashSet(10);
            for (Object r : nodeEdges[i]) {
                nodeEdgeIds[i].add(((TitanEdge) r).getID());
            }
        }
        clopen();

        nodes = new TitanVertex[noNodes];
        PropertyKey name = tx.getPropertyKey("name");
        weight = tx.getPropertyKey("weight");
        assertEquals("name", name.getName());
        id = tx.getPropertyKey("uid");
        assertTrue(id.getCardinality()==Cardinality.SINGLE);
        for (int i = 0; i < noNodes; i++) {
            TitanVertex n = getVertex(id, ids[i]);
            assertEquals(n, getVertex(name, names[i]));
            assertEquals(names[i], n.getProperty(name));
            nodes[i] = n;
            assertEquals(nodeIds[i], n.getID());
        }
        EdgeLabel knows = tx.getEdgeLabel("knows");
        for (int i = 0; i < noNodes; i++) {
            TitanVertex n = nodes[i];
            assertEquals(connectOff.length + knowsOff.length, Iterables.size(n.getEdges(OUT)));
            assertEquals(connectOff.length, Iterables.size(n.getEdges(OUT, "connect")));
            assertEquals(connectOff.length * 2, Iterables.size(n.getTitanEdges(BOTH, tx.getEdgeLabel("connect"))));
            assertEquals(knowsOff.length * 2, Iterables.size(n.getTitanEdges(BOTH, knows)), i);

            assertEquals(connectOff.length + knowsOff.length + 2, Iterables.size(n.query().direction(OUT).relations()));
            for (TitanEdge r : n.getTitanEdges(OUT, knows)) {
                TitanVertex n2 = r.getOtherVertex(n);
                int idsum = ((Number) n.getProperty(id)).intValue() + ((Number) n2.getProperty(id)).intValue();
                assertEquals(idsum, r.getProperty(id));
                double k = ((Number) r.getProperty(weight)).doubleValue() / 1.5;
                int ki = (int) k;
                assertEquals(i + "-" + ki, r.getProperty(name));
            }

            Set edgeIds = new HashSet(10);
            for (TitanEdge r : n.getTitanEdges(OUT)) {
                edgeIds.add(r.getID());
            }
            assertTrue(edgeIds.equals(nodeEdgeIds[i]));
        }
    }

    @Test
    public void testLimitWithMixedIndexCoverage() {
        final String vt = "vt";
        final String fn = "firstname";
        final String user = "user";
        final String alice = "alice";
        final String bob = "bob";

        PropertyKey vtk = makeVertexIndexedKey(vt,String.class);
        PropertyKey fnk = makeKey(fn,String.class);

        finishSchema();

        vtk = tx.getPropertyKey(vt);
        fnk = tx.getPropertyKey(fn);

        TitanVertex a = tx.addVertex();
        a.setProperty(vtk, user);
        a.setProperty(fnk, "alice");

        TitanVertex b = tx.addVertex();
        b.setProperty(vtk, user);
        b.setProperty(fnk, "bob");

        Iterable<Vertex> i;
        i = tx.query().has(vt, user).has(fn, bob).limit(1).vertices();
        assertEquals(bob, Iterators.getOnlyElement(i.iterator()).getProperty(fn));
        assertEquals(user, Iterators.getOnlyElement(i.iterator()).getProperty(vt));
        assertEquals(1, Iterators.size(i.iterator()));

        i = tx.query().has(vt, user).has(fn, alice).limit(1).vertices();
        assertEquals(alice, Iterators.getOnlyElement(i.iterator()).getProperty(fn));
        assertEquals(user, Iterators.getOnlyElement(i.iterator()).getProperty(vt));
        assertEquals(1, Iterators.size(i.iterator()));

        tx.commit();
        tx = graph.newTransaction();

        i = tx.query().has(vt, user).has(fn, bob).limit(1).vertices();
        assertEquals(bob, Iterators.getOnlyElement(i.iterator()).getProperty(fn));
        assertEquals(user, Iterators.getOnlyElement(i.iterator()).getProperty(vt));
        assertEquals(1, Iterators.size(i.iterator()));

        i = tx.query().has(vt, user).has(fn, alice).limit(1).vertices();
        assertEquals(alice, Iterators.getOnlyElement(i.iterator()).getProperty(fn));
        assertEquals(user, Iterators.getOnlyElement(i.iterator()).getProperty(vt));
        assertEquals(1, Iterators.size(i.iterator()));
    }

    @Test
    public void testWithoutIndex() {
        PropertyKey kid = graph.makePropertyKey("kid").dataType(Long.class).make();
        graph.makePropertyKey("name").dataType(String.class).make();
        graph.makeEdgeLabel("knows").signature(kid).make();
        Random random = new Random();
        int numV = 1000;
        TitanVertex previous = null;
        for (int i=0;i<numV;i++) {
            TitanVertex v = graph.addVertex(null);
            v.setProperty("kid",random.nextInt(numV));
            v.setProperty("name","v"+i);
            if (previous!=null) {
                TitanEdge e = v.addEdge("knows",previous);
                e.setProperty("kid",random.nextInt(numV/2));
            }
            previous=v;
        }

        TestUtil.verifyElementOrder(graph.query().orderBy("kid",Order.ASC).limit(500).vertices(),"kid",Order.ASC,500);
        TestUtil.verifyElementOrder(graph.query().orderBy("kid",Order.ASC).limit(300).edges(),"kid",Order.ASC,300);
        TestUtil.verifyElementOrder(graph.query().orderBy("kid",Order.DESC).limit(400).vertices(),"kid",Order.DESC,400);
        TestUtil.verifyElementOrder(graph.query().orderBy("kid",Order.DESC).limit(200).edges(),"kid",Order.DESC,200);

        clopen();

        //Copied from above
        TestUtil.verifyElementOrder(graph.query().orderBy("kid",Order.ASC).limit(500).vertices(),"kid",Order.ASC,500);
        TestUtil.verifyElementOrder(graph.query().orderBy("kid",Order.ASC).limit(300).edges(),"kid",Order.ASC,300);
        TestUtil.verifyElementOrder(graph.query().orderBy("kid",Order.DESC).limit(400).vertices(),"kid",Order.DESC,400);
        TestUtil.verifyElementOrder(graph.query().orderBy("kid",Order.DESC).limit(200).edges(),"kid",Order.DESC,200);
    }

    @Test
    public void testSimpleGlobalVertexCount() {
        final int n = 3;
        for (int i = 0; i < n; i++) {
            tx.addVertex();
        }
        assertEquals(n, Iterables.size(tx.getVertices()));
        tx.commit();
        tx = graph.newTransaction();
        assertEquals(n, Iterables.size(tx.getVertices()));
    }
}
