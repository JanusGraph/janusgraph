package com.thinkaurelius.titan.graphdb;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.serializer.SpecialInt;
import com.thinkaurelius.titan.graphdb.serializer.SpecialIntSerializer;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.tinkerpop.blueprints.Direction.*;
import static org.junit.Assert.*;

public abstract class TitanGraphTest extends TitanGraphTestCommon {

    private Logger log = LoggerFactory.getLogger(TitanGraphTest.class);

    public TitanGraphTest(Configuration config) {
        super(config);
    }

    @Test
    public void testOpenClose() {
    }

    @Test
    public void testBasic() {
        TitanKey weight = makeWeightPropertyKey("weight");
        TitanVertex n1 = tx.addVertex();
        n1.addProperty(weight, 10.5);
        clopen();
        long nid = n1.getID();
        assertTrue(tx.containsVertex(nid));
        assertTrue(tx.containsVertex(weight.getID()));
        assertFalse(tx.containsVertex(nid + 64));
        assertFalse(tx.containsVertex(weight.getID() + 64));
        assertTrue(tx.containsType("weight"));
        weight = tx.getPropertyKey("weight");
        assertEquals(weight.getDataType(), Double.class);
        assertEquals(weight.getName(), "weight");
        n1 = tx.getVertex(nid);

        for (TitanProperty prop : n1.getProperties()) {
            Object o = prop.getValue();
        }
        n1.query().relations();
        assertEquals(10.5, n1.getProperty(weight));
    }

    @Test
    public void testTypes() {

        config.subset(GraphDatabaseConfiguration.ATTRIBUTE_NAMESPACE)
                .setProperty("attribute10", SpecialInt.class.getCanonicalName());
        config.subset(GraphDatabaseConfiguration.ATTRIBUTE_NAMESPACE)
                .setProperty("serializer10", SpecialIntSerializer.class.getCanonicalName());

        clopen();

        TitanLabel friend = tx.makeLabel("friend").directed().manyToMany().make();

        TitanKey id = tx.makeKey("uid").single().unique().indexed(Vertex.class).dataType(String.class).make();

        TitanKey weight = tx.makeKey("weight").single().dataType(Double.class).make();

        TitanKey someid = tx.makeKey("someid").single().dataType(Object.class).indexed(Vertex.class).make();


        TitanKey boolval = tx.makeKey("boolval").dataType(Boolean.class).single().make();

        TitanKey sint = tx.makeKey("int").dataType(SpecialInt.class).single().make();

        TitanLabel link = tx.makeLabel("link").unidirected().make();

        TitanLabel connect = tx.makeLabel("connect").signature(id, weight).manyToOne(TypeMaker.UniquenessConsistency.NO_LOCK).make();

        TitanLabel parent = tx.makeLabel("parent").manyToOne().sortKey(weight).make();
        assertTrue(parent.isUnique(OUT));
        TitanLabel child = tx.makeLabel("child").oneToMany().make();
        assertTrue(child.isUnique(IN));
        TitanLabel spouse = tx.makeLabel("spouse").oneToOne().make();
        assertTrue(spouse.isUnique(IN));
        assertTrue(spouse.isUnique(OUT));


        try {
            tx.makeKey("pint").dataType(int.class).make();
            fail();
        } catch (IllegalArgumentException e) {
        }

        try {
            tx.makeKey("number").dataType(Number.class).single().make();
            fail();
        } catch (IllegalArgumentException e) {
        }

        TitanKey arrType = tx.makeKey("barr").dataType(byte[].class).single().make();

        clopen();

        assertNull(tx.getVertex(id, "v1"));

        id = tx.getPropertyKey("uid");
        assertTrue(id.isUnique(Direction.IN));
        assertTrue(id.hasIndex(Titan.Token.STANDARD_INDEX, Vertex.class));
        assertTrue(id.isUnique(Direction.OUT));
        assertEquals(String.class, id.getDataType());

        //Basic properties

        friend = tx.getEdgeLabel("friend");
        assertEquals("friend", friend.getName());
        assertTrue(friend.isDirected());
        assertFalse(friend.isUnidirected());
        assertTrue(friend.isEdgeLabel());
        assertFalse(friend.isPropertyKey());
        assertFalse(friend.isUnique(Direction.OUT));
        assertFalse(((InternalType) friend).uniqueLock(Direction.OUT));
        assertFalse(((InternalType) friend).isHidden());

        connect = tx.getEdgeLabel("connect");
        assertEquals("connect", connect.getName());
        assertFalse(connect.isUnidirected());
        assertTrue(connect.isEdgeLabel());
        assertFalse(connect.isPropertyKey());
        assertTrue(connect.isUnique(Direction.OUT));
        assertFalse(((InternalType) connect).uniqueLock(Direction.OUT));
        assertFalse(((InternalType) connect).isHidden());

        link = tx.getEdgeLabel("link");
        assertTrue(link.isUnidirected());
        assertFalse(link.isUnique(Direction.OUT));

        weight = tx.getPropertyKey("weight");
        assertEquals(Double.class, weight.getDataType());

        boolval = tx.getPropertyKey("boolval");
        assertEquals(Boolean.class, boolval.getDataType());


        //Failures
        try {
            tx.makeKey("fid").make();
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            tx.makeLabel("link").unidirected().make();
            fail();
        } catch (IllegalArgumentException e) {
        }
        tx.makeLabel("test").make();
        try {
            tx.makeLabel("link2").unidirected().
                    sortKey(id, weight).signature(id).make();
            fail();
        } catch (IllegalArgumentException e) {
        }
//        try {
//            tx.makeLabel("link2").unidirected().
//                    sortKey(id, weight).make();
//            fail();
//        } catch (IllegalArgumentException e) {
//        }
        tx.makeLabel("link2").unidirected().
                sortKey(id, weight).make();

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
        v1 = tx.getVertex(id, "v1");
        assertEquals(77, ((SpecialInt) v1.getProperty("int")).getValue());
        assertEquals(v1, Iterables.getOnlyElement(tx.getVertices("someid", 100l)));
        assertEquals(v1, Iterables.getOnlyElement(tx.getVertices(id, "v1")));

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

        v2 = tx.getVertex(id, "v2");
        assertEquals(154, ((SpecialInt) v2.getProperty("int")).getValue());
        assertEquals(v2, Iterables.getOnlyElement(tx.getVertices("someid", 200l)));
        assertEquals(v2, Iterables.getOnlyElement(tx.getVertices(id, "v2")));

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

        newTx();
        tx.makeKey("domain").unique().indexed(Vertex.class).dataType(String.class).make();
        v1 = tx.addVertex();
        try {
            v1.setProperty("domain", "unique1");
        } catch (IllegalArgumentException e) {

        } finally {
            tx.rollback();
            tx = null;
        }
        newTx();


        tx.makeKey("domain").unique().indexed(Vertex.class).dataType(String.class).make();
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

        tx.makeKey("domain").unique().indexed(Vertex.class).dataType(String.class).make();
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
        graph.removeVertex(v1);
        graph.removeVertex(v2);

        graph.commit();
    }

    @Test
    public void testVertexDeletion() throws Exception {

        TitanKey name = makeUniqueStringPropertyKey("name");
        newTx();

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
        final String foo = "foo", bar = "bar";
        final List<String> values =
                ImmutableList.of("four", "score", "and", "seven");
        assertTrue("Values list must have multiple elements for this test to make sense",
                2 <= values.size());

        // Create property with name pname and a vertex
        makeNonUniqueStringPropertyKey(foo);
        makeNonUniqueStringPropertyKey(bar);
        newTx();
        TitanVertex v = tx.addVertex();

        // Insert prop values
        for (String s : values) {
            v.addProperty(foo, s);
            v.addProperty(bar, s);
        }

        //Verify correct number of properties
        assertEquals(values.size(), Iterables.size(v.getProperties(foo)));

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
        tx.makeKey("birthday").single().dataType(GregorianCalendar.class).make();

        Vertex v = tx.addVertex(null);
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
        tx.makeLabel("link").unidirected().make();
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
        TitanKey weight = makeWeightPropertyKey("weight");
        TitanKey id = makeIntegerUIDPropertyKey("uid");
        TitanLabel knows = makeKeyedEdgeLabel("knows", id, weight);

        TitanVertex n1 = tx.addVertex(), n3 = tx.addVertex();
        TitanEdge e = n3.addEdge(knows, n1);
        e.setProperty(id, 111);
        n3.addProperty(id, 445);
        assertEquals(111, e.getProperty(id));
        clopen();
        Object eid = e.getId();
        long nid = n3.getID();

        n3 = tx.getVertex(nid);
        assertEquals(445, n3.getProperty("uid"));
        e = Iterables.getOnlyElement(n3.getTitanEdges(OUT, tx.getEdgeLabel("knows")));
        assertEquals(111, e.getProperty("uid"));
        assertEquals(e, tx.getEdge(eid));
        assertEquals(e, tx.getEdge(eid.toString()));
        TitanProperty p = Iterables.getOnlyElement(n3.getProperties("uid"));
        p.remove();
        n3.addProperty("uid", 353);


        clopen();

        n3 = tx.getVertex(nid);
        assertEquals(353, n3.getProperty("uid"));
        TitanEdge e2 = n3.addEdge("knows", tx.addVertex());
    }

    @Test
    public void testSelfLoop() {
        Vertex v = tx.addVertex(null);
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
    public void testVertexDeletionWithIndex() {
        TitanKey name = makeStringPropertyKey("name");
        Vertex v1 = tx.addVertex(null);
        v1.setProperty("name", "v1");
        Vertex v2 = tx.addVertex(null);
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

        makeStringPropertyKey(propName);

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
        TitanLabel link = tx.makeLabel("link").unidirected().manyToOne().make();
        TitanLabel connect = tx.makeLabel("connect").sortKey(link).make();

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
        graph.makeKey("name").dataType(String.class).indexed(Vertex.class).make();
        graph.makeKey("color").dataType(String.class).indexed(Vertex.class).make();
        Vertex v = graph.addVertex(null);
        v.setProperty("name", "ilya");
        v.setProperty("color", "blue");
        graph.commit();

        assertEquals(1, Iterables.size(graph.query().has("name", "ilya").vertices()));
        assertEquals(1, Iterables.size(graph.query().has("name", "ilya").has("color", "blue").vertices()));
    }

    @Test
    public void testLargeJointIndexRetrieval() {
        graph.makeKey("sid").dataType(Integer.class).indexed(Vertex.class).make();
        graph.makeKey("color").dataType(String.class).indexed(Vertex.class).make();
        graph.commit();

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
        TitanKey id = tx.makeKey("uid").
                single().
                unique().indexed(Vertex.class).indexed(Edge.class).
                dataType(Integer.class).
                make();
        TitanKey name = tx.makeKey("name").single().
                indexed(Vertex.class).indexed(Edge.class).
                dataType(String.class).
                make();
        TitanLabel connect = tx.makeLabel("connect").signature(id, name).make();
        int noNodes = 100;
        int div = 10;
        int mod = noNodes / div;
        for (int i = 0; i < noNodes; i++) {
            TitanVertex n = tx.addVertex();
            n.addProperty(id, i);
            n.addProperty(name, "Name" + (i % mod));
            TitanVertex other = tx.getVertex(id, Math.max(0, i - 1));
            Preconditions.checkNotNull(other);
            TitanEdge e = n.addEdge(connect, other);
            e.setProperty(id, i);
            e.setProperty(name, "Edge" + (i % mod));
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
            assertEquals(Iterables.getOnlyElement(tx.getVertices("uid", i)).getProperty("name").toString().substring(4), String.valueOf(i % mod));
        }
        for (int i = 0; i < noNodes; i++) {
            assertEquals(Iterables.getOnlyElement(tx.getEdges("uid", i)).getProperty("name").toString().substring(4), String.valueOf(i % mod));
        }

    }

    @Test
    public void testThreadBoundTx() {
        TitanKey t = graph.makeKey("type").dataType(Integer.class).single().indexed(Edge.class).make();
        graph.makeLabel("friend").sortKey(t).make();
        graph.commit();

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

        try {
            //TODO: how to address this? Only allow transactional passing for vertices?
            assertEquals(25, e2.getProperty("time"));
            fail();
        } catch (InvalidElementException e) {
        }

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
        String[] etNames = {"connect", "name", "weight", "knows"};
        TitanLabel connect = makeSimpleEdgeLabel(etNames[0]);
        TitanKey name = makeUniqueStringPropertyKey(etNames[1]);
        TitanKey weight = makeWeightPropertyKey(etNames[2]);
        TitanKey id = makeIntegerUIDPropertyKey("uid");
        TitanLabel knows = makeKeyedEdgeLabel(etNames[3], id, weight);


        assertEquals(connect, tx.getEdgeLabel(etNames[0]));
        assertEquals(name, tx.getPropertyKey(etNames[1]));
        assertEquals(knows, tx.getEdgeLabel(etNames[3]));
        assertTrue(knows.isEdgeLabel());

        TitanVertex n1 = tx.addVertex();
        TitanVertex n2 = tx.addVertex();
        TitanVertex n3 = tx.addVertex();
        assertNotNull(n1.toString());
        n1.addProperty(name, "Node1");
        n2.addProperty(name, "Node2");
        n3.addProperty(weight, 5.0);
        TitanEdge e = n1.addEdge(connect, n2);
        assertNotNull(e.toString());
        assertEquals(n1, e.getVertex(OUT));
        assertEquals(n2, e.getVertex(IN));
        e = n2.addEdge(knows, n3);
        e.setProperty(weight, 3.0);
        e.setProperty(name, "HasProperties TitanRelation");
        e = n3.addEdge(knows, n1);
        n3.addEdge(connect, n3);
        e.setProperty(id, 111);
        assertEquals(4, Iterables.size(n3.getEdges()));
        assertEquals(2, Iterables.size(n3.getEdges(Direction.OUT)));
        assertEquals(2, Iterables.size(n3.getEdges(Direction.IN)));

        clopen();

        connect = tx.getEdgeLabel(etNames[0]);
        assertEquals(connect.getName(), etNames[0]);
        assertTrue(connect.isDirected());
        name = tx.getPropertyKey(etNames[1]);
        assertTrue(name.isUnique(Direction.IN));
        assertTrue(name.hasIndex(Titan.Token.STANDARD_INDEX, Vertex.class));
        weight = tx.getPropertyKey(etNames[2]);
        id = tx.getPropertyKey("uid");
        knows = tx.getEdgeLabel(etNames[3]);
        log.debug("Loaded edge types");
        n2 = tx.getVertex(name, "Node2");
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
        assertEquals(3.0, e.getProperty(weight));
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
        n2 = tx.getVertex("name", "Node2");
        e = Iterables.getOnlyElement(n2.getTitanEdges(OUT, tx.getEdgeLabel("knows")));
        assertEquals("New TitanRelation", e.getProperty(tx.getPropertyKey("name")));
        assertEquals(111.5, ((Double) e.getProperty("weight")).doubleValue(), 0.01);

    }

    @Test
    public void testQuery() {
        TitanKey name = tx.makeKey("name").dataType(String.class).single().indexed(Vertex.class).unique().make();
        TitanKey time = tx.makeKey("time").dataType(Integer.class).single().make();
        TitanKey weight = tx.makeKey("weight").dataType(Double.class).single().make();

        TitanLabel author = tx.makeLabel("author").manyToOne().unidirected().make();

        TitanLabel connect = tx.makeLabel("connect").sortKey(time).make();
        TitanLabel friend = tx.makeLabel("friend").sortKey(weight, time).signature(author).make();
        TitanLabel knows = tx.makeLabel("knows").sortKey(author, weight).make();
        TitanLabel follows = tx.makeLabel("follows").make();

        int noVertices = 100;
        TitanVertex[] vs = new TitanVertex[noVertices];
        for (int i = 0; i < noVertices; i++) {
            vs[i] = tx.addVertex();
            vs[i].addProperty(name, "v" + i);
        }
        TitanVertex v = vs[0];
        for (int i = 1; i < noVertices; i++) {
            TitanEdge e;
            if (i % 3 == 0) {
                e = v.addEdge(connect, vs[i]);
            } else if (i % 3 == 1) {
                e = v.addEdge(friend, vs[i]);
            } else {
                e = v.addEdge(knows, vs[i]);
            }
            e.setProperty("time", i);
            e.setProperty("weight", i % 4 + 0.5);
            e.setProperty("name", "e" + i);
            e.setProperty(author, vs[i % 5]);
        }

        VertexList vl;
        clopen();
        for (int i = 0; i < noVertices; i++) vs[i] = tx.getVertex(vs[i].getID());
        v = vs[0];


        //Queries from cache
        assertEquals(noVertices - 1, Iterables.size(v.getEdges()));
        assertEquals(10, Iterables.size(v.query().labels("connect").limit(10).vertices()));
        assertEquals(10, Iterables.size(v.query().labels("connect").has("time", Compare.GREATER_THAN, 30).limit(10).vertices()));

        int lastTime = 0;
        for (Edge e : v.query().labels("connect").direction(OUT).limit(20).edges()) {
            int nowTime = e.getProperty("time");
            //System.out.println(nowTime);
            assertTrue(lastTime + " vs. " + nowTime, lastTime <= nowTime);
            lastTime = nowTime;
        }
        assertEquals(10, Iterables.size(v.query().labels("connect").direction(OUT).has("time", Compare.GREATER_THAN, 60).limit(10).vertices()));

        Iterator<Edge> outer = v.query().labels("connect").direction(OUT).limit(20).edges().iterator();
        for (Edge e : v.query().labels("connect").direction(OUT).limit(10).edges()) {
            assertEquals(e, outer.next());
        }
        assertEquals(10, v.query().labels("connect").direction(OUT).interval("time", 3, 31).count());
        assertEquals(33, v.query().labels("connect").direction(OUT).count());
        assertEquals(33, v.query().labels("connect").has("undefined", null).direction(OUT).count());
        assertEquals(0, v.query().labels("connect").direction(OUT).has("time", null).count());
        assertEquals(10, v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertexIds().size());
        assertEquals(23, v.query().labels("connect").direction(OUT).has("time", Compare.GREATER_THAN, 31).count());
        assertEquals(10, Iterables.size(v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertices()));
        assertEquals(1, v.query().has("time", 1).count());
        assertEquals(10, v.query().interval("time", 4, 14).count());
        assertEquals(9, v.query().interval("time", 4, 14).has("time", Compare.NOT_EQUAL, 10).count());
        assertEquals(20, v.query().labels("friend", "connect").direction(OUT).interval("time", 3, 33).count());
        assertEquals(30, v.query().labels("friend", "connect", "knows").direction(OUT).interval("time", 3, 33).count());
//        for (TitanRelation r : v.query().labels("friend", "connect", "knows").direction(OUT).has("time", 10, Query.Compare.NOT_EQUAL).relations()) System.out.println(r);
        assertEquals(98, v.query().labels("friend", "connect", "knows").direction(OUT).has("time", Compare.NOT_EQUAL, 10).count());
        assertEquals(3, v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5).count());
        assertEquals(1, v.query().labels("friend").direction(OUT).has("weight", 0.5).interval("time", 4, 10).count());
        assertEquals(4, v.query().labels("friend").direction(OUT).has("time", Compare.LESS_THAN_EQUAL, 10).count());
        assertEquals(29, v.query().labels("friend").direction(OUT).has("time", Compare.GREATER_THAN, 10).count());
        vl = v.query().labels().direction(OUT).interval("time", 3, 31).vertexIds();
        vl.sort();
        long[] vidsubset = new long[31 - 3];
        for (int i = 0; i < vidsubset.length; i++) vidsubset[i] = vs[i + 3].getID();
        Arrays.sort(vidsubset);
        for (int i = 0; i < vl.size(); i++) assertEquals(vidsubset[i], vl.getID(i));
        assertEquals(7, v.query().labels("knows").direction(OUT).has("author", v).count());
        assertEquals(7, v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 0.0, 4.0).count());
        assertEquals(4, v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 0.0, 2.0).count());
        assertEquals(3, v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 2.1, 4.0).count());
        assertEquals(20, Iterables.size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertices()));
        assertEquals(25, Iterables.size(v.query().labels("connect", "friend", "knows").has("weight", 1.5).vertexIds()));
        assertEquals(0, v.query().has("age", null).labels("undefined").direction(OUT).count());
        //Adjacent queries
        assertEquals(1, v.query().labels("connect").direction(OUT).adjacentVertex(vs[6]).has("time", 6).count());
        assertEquals(1, v.query().labels("connect").adjacentVertex(vs[6]).has("time", 6).count());
        assertEquals(0, v.query().labels("connect").adjacentVertex(vs[8]).has("time", 8).count());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacentVertex(vs[11]).count());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacentVertex(vs[11]).has("weight", 3.5).count());

        //MultiQueries
        TitanVertex[] qvs = {vs[6], vs[9], vs[12], vs[15], vs[60]};
        Map<TitanVertex, Iterable<TitanEdge>> results;
        results = tx.multiQuery(qvs).direction(IN).labels("connect").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(1, Iterables.size(result));
        results = tx.multiQuery(qvs).labels("connect").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(1, Iterables.size(result));
        results = tx.multiQuery(qvs).labels("knows").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(0, Iterables.size(result));

        clopen();
        for (int i = 0; i < noVertices; i++) vs[i] = tx.getVertex(vs[i].getID());
        v = vs[0];

        //Same queries as above but without memory loading
        assertEquals(10, Iterables.size(v.query().labels("connect").limit(10).vertices()));
        assertEquals(10, Iterables.size(v.query().labels("connect").has("time", Compare.GREATER_THAN, 30).limit(10).vertices()));

        assertEquals(0, v.query().labels("follows").has("time", Compare.LESS_THAN, 10).count());

        //Adjacent queries
        assertEquals(1, v.query().labels("connect").direction(OUT).adjacentVertex(vs[6]).has("time", 6).count());
        assertEquals(1, v.query().labels("connect").adjacentVertex(vs[6]).has("time", 6).count());
        assertEquals(0, v.query().labels("connect").adjacentVertex(vs[8]).has("time", 8).count());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacentVertex(vs[11]).count());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacentVertex(vs[11]).has("weight", 3.5).count());

        assertEquals(0, v.query().labels("connect").direction(OUT).has("time", null).count());
        assertEquals(10, v.query().labels("connect").direction(OUT).interval("time", 3, 31).count());
        assertEquals(10, v.query().labels("connect").direction(OUT).interval("time", 3, 31).count());
        assertEquals(23, v.query().labels("connect").direction(OUT).has("time", Compare.GREATER_THAN, 31).count());


        assertEquals(10, v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertexIds().size());
        assertEquals(10, Iterables.size(v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertices()));
        assertEquals(1, v.query().has("time", 1).count());
        assertEquals(10, v.query().interval("time", 4, 14).count());
        assertEquals(30, v.query().labels("friend", "connect", "knows").direction(OUT).interval("time", 3, 33).count());
        assertEquals(3, v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5).count());
        assertEquals(1, v.query().labels("friend").direction(OUT).has("weight", 0.5).interval("time", 4, 10).count());
        assertEquals(4, v.query().labels("friend").direction(OUT).has("time", Compare.LESS_THAN_EQUAL, 10).count());
        assertEquals(29, v.query().labels("friend").direction(OUT).has("time", Compare.GREATER_THAN, 10).count());
        vl = v.query().labels().direction(OUT).interval("time", 3, 31).vertexIds();
        vl.sort();
        for (int i = 0; i < vl.size(); i++) assertEquals(vidsubset[i], vl.getID(i));
        assertEquals(7, v.query().labels("knows").direction(OUT).has("author", v).count());
        assertEquals(7, v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 0.0, 4.0).count());
        assertEquals(4, v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 0.0, 2.0).count());
        assertEquals(3, v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 2.1, 4.0).count());
        assertEquals(20, Iterables.size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertices()));
        assertEquals(20, Iterables.size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertexIds()));
        assertEquals(25, Iterables.size(v.query().labels("connect", "friend", "knows").has("weight", 1.5).vertexIds()));
        assertEquals(33, v.query().labels("connect").direction(OUT).count());
        assertEquals(33, v.query().labels("connect").has("undefined", null).direction(OUT).count());
        assertEquals(98, v.query().labels("friend", "connect", "knows").direction(OUT).has("time", Compare.NOT_EQUAL, 10).count());
        assertEquals(99, Iterables.size(v.query().direction(OUT).vertices()));

        clopen();

        //MultiQueries
        TitanVertex[] qvs2 = new TitanVertex[qvs.length];
        for (int i = 0; i < qvs.length; i++) qvs2[i] = tx.getVertex(qvs[i].getID());
        qvs = qvs2;
        results = tx.multiQuery(qvs).direction(IN).labels("connect").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(1, Iterables.size(result));
        results = tx.multiQuery(qvs).labels("connect").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(1, Iterables.size(result));
        results = tx.multiQuery(qvs).labels("knows").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(0, Iterables.size(result));

        newTx();

        v = (TitanVertex) tx.getVertices("name", "v0").iterator().next();
        assertNotNull(v);
        assertEquals(2, v.query().has("weight", 1.5).interval("time", 10, 30).limit(2).vertexIds().size());
        assertEquals(5, v.query().has("weight", 1.5).interval("time", 10, 30).vertexIds().size());

        newTx();

        v = (TitanVertex) tx.getVertices("name", "v0").iterator().next();
        assertNotNull(v);
        assertEquals(2, v.query().has("weight", 1.5).interval("time", 10, 30).limit(2).count());
        assertEquals(5, v.query().has("weight", 1.5).interval("time", 10, 30).count());
    }

    //Merge above
    public void neighborhoodTest() {
        testCreateAndRetrieveComprehensive();
        log.debug("Neighborhood:");
        TitanVertex n1 = tx.getVertex("name", "Node1");
        TitanVertexQuery q = n1.query().direction(OUT).types(tx.getEdgeLabel("connect"));
        VertexList res = q.vertexIds();
        assertEquals(1, res.size());
        TitanVertex n2 = tx.getVertex("name", "Node2");
        assertEquals(n2.getID(), res.getID(0));
    }

    /**
     * Testing using hundreds of objects
     */
    @Test
    public void createAndRetrieveMedium() {
        //Create Graph
        TitanLabel connect = makeSimpleEdgeLabel("connect");
        TitanKey name = makeUniqueStringPropertyKey("name");
        TitanKey weight = makeWeightPropertyKey("weight");
        TitanKey id = makeIntegerUIDPropertyKey("uid");
        TitanLabel knows = makeKeyedEdgeLabel("knows", id, weight);

        //Create Nodes
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
            nodes[i].addProperty(name, names[i]);
            nodes[i].addProperty(id, ids[i]);
            if ((i + 1) % 100 == 0) log.debug("Added 100 nodes");
        }
        log.debug("Nodes created");
        int[] connectOff = {-100, -34, -4, 10, 20};
        int[] knowsOff = {-400, -18, 8, 232, 334};
        for (int i = 0; i < noNodes; i++) {
            TitanVertex n = nodes[i];
            nodeEdges[i] = new ArrayList(10);
            for (int c : connectOff) {
                TitanEdge r = n.addEdge(connect, nodes[wrapAround(i + c, noNodes)]);
                nodeEdges[i].add(r);
            }
            for (int k : knowsOff) {
                TitanVertex n2 = nodes[wrapAround(i + k, noNodes)];
                TitanEdge r = n.addEdge(knows, n2);
                r.setProperty(id, ((Number) n.getProperty(id)).intValue() + ((Number) n2.getProperty(id)).intValue());
                r.setProperty(weight, k * 1.5);
                r.setProperty(name, i + "-" + k);
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
        name = tx.getPropertyKey("name");
        weight = tx.getPropertyKey("weight");
        assertEquals("name", name.getName());
        id = tx.getPropertyKey("uid");
        assertTrue(id.isUnique(Direction.OUT));
        for (int i = 0; i < noNodes; i++) {
            TitanVertex n = tx.getVertex(id, ids[i]);
            assertEquals(n, tx.getVertex(name, names[i]));
            assertEquals(names[i], n.getProperty(name));
            nodes[i] = n;
            assertEquals(nodeIds[i], n.getID());
        }
        knows = tx.getEdgeLabel("knows");
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

        TitanKey vtk = makeStringPropertyKey(vt);
        TitanKey fnk = makeUnindexedStringPropertyKey(fn);

        tx.commit();
        tx = graph.newTransaction();

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
