package com.thinkaurelius.titan.graphdb;


import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Decimal;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.attribute.Precision;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.schema.*;
import com.thinkaurelius.titan.core.log.*;
import com.thinkaurelius.titan.core.schema.TitanSchemaType;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLog;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.diskstorage.util.time.Timepoint;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.log.Log;
import com.thinkaurelius.titan.diskstorage.log.Message;
import com.thinkaurelius.titan.diskstorage.log.MessageReader;
import com.thinkaurelius.titan.diskstorage.log.ReadMarker;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.log.LogTxMeta;
import com.thinkaurelius.titan.graphdb.database.log.LogTxStatus;
import com.thinkaurelius.titan.graphdb.database.log.TransactionLogHeader;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.OrderList;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;

import static com.thinkaurelius.titan.graphdb.internal.RelationCategory.*;

import com.thinkaurelius.titan.graphdb.log.StandardTransactionLogProcessor;
import com.thinkaurelius.titan.graphdb.query.StandardQueryDescription;
import com.thinkaurelius.titan.graphdb.query.vertex.BasicVertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.thinkaurelius.titan.graphdb.schema.EdgeLabelDefinition;
import com.thinkaurelius.titan.graphdb.schema.PropertyKeyDefinition;
import com.thinkaurelius.titan.graphdb.schema.SchemaContainer;
import com.thinkaurelius.titan.graphdb.schema.VertexLabelDefinition;
import com.thinkaurelius.titan.graphdb.serializer.SpecialInt;
import com.thinkaurelius.titan.graphdb.serializer.SpecialIntSerializer;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.StandardEdgeLabelMaker;
import com.thinkaurelius.titan.graphdb.types.StandardPropertyKeyMaker;
import com.thinkaurelius.titan.graphdb.types.system.BaseVertexLabel;
import com.thinkaurelius.titan.graphdb.types.system.ImplicitKey;
import com.thinkaurelius.titan.testutil.TestUtil;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Vertex;

import com.tinkerpop.blueprints.util.ElementHelper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.tinkerpop.blueprints.Direction.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class TitanGraphTest extends TitanGraphBaseTest {

    private Logger log = LoggerFactory.getLogger(TitanGraphTest.class);

    protected abstract boolean isLockingOptimistic();

  /* ==================================================================================
                            INDEXING
     ==================================================================================*/

    /**
     * Just opens and closes the graph
     */
    @Test
    public void testOpenClose() {
    }

    /**
     * Very simple graph operation to ensure minimal functionality and cleanup
     */
    @Test
    public void testBasic() {
        PropertyKey uid = makeVertexIndexedUniqueKey("name",String.class);
        finishSchema();

        TitanVertex n1 = tx.addVertex();
        uid = tx.getPropertyKey("name");
        n1.addProperty(uid.getName(), "abcd");
        clopen();
        long nid = n1.getLongId();
        uid = tx.getPropertyKey("name");
        assertTrue(tx.containsVertex(nid));
        assertTrue(tx.containsVertex(uid.getLongId()));
        assertFalse(tx.containsVertex(nid + 64));
        uid = tx.getPropertyKey(uid.getName());
        n1 = tx.getVertex(nid);
        assertEquals(n1,Iterables.getOnlyElement(tx.getVertices(uid, "abcd")));
        assertEquals(1, Iterables.size(n1.query().relations()));
        assertTrue(n1.getProperty(uid).equals("abcd"));
        assertEquals(1,Iterables.size(tx.getVertices()));
        close();
        TitanCleanup.clear(graph);
        open(config);
        assertTrue(Iterables.isEmpty(tx.getVertices()));
    }

    /**
     * Adding a removing a vertex with index
     */
    @Test
    public void testVertexRemoval() {
        String namen = "name";
        PropertyKey name = makeVertexIndexedUniqueKey(namen,String.class);
        finishSchema();


        Vertex v1 = graph.addVertex(null);
        v1.setProperty(namen,"v1");
        Vertex v2 = graph.addVertex(null);
        v2.setProperty(namen,"v2");
        Edge e = graph.addEdge(null, v1, v2, "knows");
        assertEquals(2,Iterables.size(graph.getVertices()));
        assertEquals(1,Iterables.size(graph.query().has(namen,"v2").vertices()));

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
        assertEquals(1,Iterables.size(graph.getVertices()));
        assertEquals(1,Iterables.size(graph.query().has(namen,"v1").vertices()));
        assertEquals(0,Iterables.size(graph.query().has(namen,"v2").vertices()));

        graph.commit();

        assertNull(graph.getVertex(v2));
        assertEquals(1,Iterables.size(graph.getVertices()));
        assertEquals(1,Iterables.size(graph.query().has(namen,"v1").vertices()));
        assertEquals(0,Iterables.size(graph.query().has(namen,"v2").vertices()));

    }

    /**
     * Iterating over all vertices and edges in a graph
     */
    @Test
    public void testGlobalIteration() {
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
            for (int i=0;i<10;i++) { //Repeated vertex counts
                assertEquals(numV - deleteV, Iterables.size(tx.getVertices()));
                for (Vertex v : tx.getVertices()) assertTrue(v.<Integer>getProperty("count")>=0);
            }

            clopen();
            assertEquals(numV - deleteV, Iterables.size(graph.getVertices()));
            for (int i=0;i<10;i++) {
                assertEquals(numV - deleteV, Iterables.size(tx.getVertices()));
                for (Vertex v : tx.getVertices()) assertTrue(v.<Integer>getProperty("count")>=0);
            }
        }
    }

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


    /**
     * In purpose similar to {@link #testCreateAndRetrieveComprehensive()} but less focused on comprehensive feature
     * coverage and more on using larger transaction sizes
     */
    @Test
    public void testCreateAndRetrieveMedium() {
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
            nodeIds[i] = nodes[i].getLongId();
            nodeEdgeIds[i] = new HashSet(10);
            for (Object r : nodeEdges[i]) {
                nodeEdgeIds[i].add(((TitanEdge) r).getLongId());
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
            assertEquals(nodeIds[i], n.getLongId());
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
                edgeIds.add(r.getLongId());
            }
            assertTrue(edgeIds.equals(nodeEdgeIds[i]));
        }
    }


    /* ==================================================================================
                            SCHEMA TESTS
     ==================================================================================*/

    /**
     * Test the definition and inspection of various schema types and ensure their correct interpretation
     * within the graph
     */
    @Test
    public void testSchemaTypes() {
        // ---------- PROPERTY KEYS ----------------
        //Normal single-valued property key
        PropertyKey weight = makeKey("weight",Decimal.class);
        //Indexed unique property key
        PropertyKey id = makeVertexIndexedUniqueKey("uid",String.class);
        //Indexed but not unique
        PropertyKey someid = makeVertexIndexedKey("someid", Object.class);
        //Set-valued property key
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SET).make();
        //List-valued property key with signature
        PropertyKey value = mgmt.makePropertyKey("value").dataType(Precision.class).signature(weight).cardinality(Cardinality.LIST).make();

        // ---------- EDGE LABELS ----------------
        //Standard edge label
        EdgeLabel friend = mgmt.makeEdgeLabel("friend").make();
        //Unidirected
        EdgeLabel link = mgmt.makeEdgeLabel("link").unidirected().multiplicity(Multiplicity.MANY2ONE).make();
        //Signature label
        EdgeLabel connect = mgmt.makeEdgeLabel("connect").signature(id, link).multiplicity(Multiplicity.SIMPLE).make();
        //Edge labels with different cardinalities
        EdgeLabel parent = mgmt.makeEdgeLabel("parent").multiplicity(Multiplicity.MANY2ONE).make();
        EdgeLabel child = mgmt.makeEdgeLabel("child").multiplicity(Multiplicity.ONE2MANY).make();
        EdgeLabel spouse = mgmt.makeEdgeLabel("spouse").multiplicity(Multiplicity.ONE2ONE).make();

        // ---------- VERTEX LABELS ----------------

        VertexLabel person = mgmt.makeVertexLabel("person").make();
        VertexLabel tag = mgmt.makeVertexLabel("tag").partition().make();
        VertexLabel tweet = mgmt.makeVertexLabel("tweet").setStatic().make();

        long[] sig;

        // ######### INSPECTION & FAILURE ############

        assertTrue(mgmt.isOpen());
        assertEquals("weight",weight.toString());
        assertTrue(mgmt.containsRelationType("weight"));
        assertFalse(mgmt.containsRelationType("bla"));
        assertTrue(weight.isPropertyKey());
        assertFalse(weight.isEdgeLabel());
        assertEquals(Cardinality.SINGLE,weight.getCardinality());
        assertEquals(Cardinality.SINGLE,someid.getCardinality());
        assertEquals(Cardinality.SET,name.getCardinality());
        assertEquals(Cardinality.LIST,value.getCardinality());
        assertEquals(Object.class,someid.getDataType());
        assertEquals(Decimal.class,weight.getDataType());
        sig = ((InternalRelationType)value).getSignature();
        assertEquals(1,sig.length);
        assertEquals(weight.getLongId(),sig[0]);
        assertTrue(mgmt.getGraphIndex(id.getName()).isUnique());
        assertFalse(mgmt.getGraphIndex(someid.getName()).isUnique());

        assertEquals("friend",friend.getName());
        assertTrue(friend.isEdgeLabel());
        assertFalse(friend.isPropertyKey());
        assertEquals(Multiplicity.ONE2ONE,spouse.getMultiplicity());
        assertEquals(Multiplicity.ONE2MANY,child.getMultiplicity());
        assertEquals(Multiplicity.MANY2ONE,parent.getMultiplicity());
        assertEquals(Multiplicity.MULTI,friend.getMultiplicity());
        assertEquals(Multiplicity.SIMPLE, connect.getMultiplicity());
        assertTrue(link.isUnidirected());
        assertFalse(link.isDirected());
        assertFalse(child.isUnidirected());
        assertTrue(spouse.isDirected());
        assertFalse(((InternalRelationType) friend).isHiddenType());
        assertTrue(((InternalRelationType) friend).isHidden());
        assertEquals(0,((InternalRelationType) friend).getSignature().length);
        sig = ((InternalRelationType)connect).getSignature();
        assertEquals(2,sig.length);
        assertEquals(id.getLongId(),sig[0]);
        assertEquals(link.getLongId(),sig[1]);
        assertEquals(0,((InternalRelationType) friend).getSortKey().length);
        assertEquals(Order.DEFAULT,((InternalRelationType) friend).getSortOrder());
        assertEquals(SchemaStatus.ENABLED,((InternalRelationType)friend).getStatus());

        assertEquals(5,Iterables.size(mgmt.getRelationTypes(PropertyKey.class)));
        assertEquals(6,Iterables.size(mgmt.getRelationTypes(EdgeLabel.class)));
        assertEquals(11,Iterables.size(mgmt.getRelationTypes(RelationType.class)));
        assertEquals(3,Iterables.size(mgmt.getVertexLabels()));

        assertEquals("tweet",tweet.getName());
        assertTrue(mgmt.containsVertexLabel("person"));
        assertFalse(mgmt.containsVertexLabel("bla"));
        assertFalse(person.isPartitioned());
        assertFalse(person.isStatic());
        assertTrue(tag.isPartitioned());
        assertTrue(tweet.isStatic());

        //------ TRY INVALID STUFF --------

        //Failures
        try {
            //No datatype
            mgmt.makePropertyKey("fid").make();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //Already exists
            mgmt.makeEdgeLabel("link").unidirected().make();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //signature and sort-key collide
            ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("other")).
                    sortKey(someid, weight).signature(someid).make();
            fail();
        } catch (IllegalArgumentException e) {}
//        try {
//            //keys must be single-valued
//            ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("other")).
//                    sortKey(name, weight).make();
//            fail();
//        } catch (IllegalArgumentException e) {}
        try {
            //sort key requires the label to be non-constrained
            ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("other")).multiplicity(Multiplicity.SIMPLE).
                    sortKey(weight).make();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //sort key requires the label to be non-constrained
            ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("other")).multiplicity(Multiplicity.MANY2ONE).
                    sortKey(weight).make();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //Already exists
            mgmt.makeVertexLabel("tweet").make();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //only unidrected, Many2One labels are allowed in signatures
            mgmt.makeEdgeLabel("test").signature(friend).make();
            fail();
        } catch (IllegalArgumentException e) {}

        // ######### END INSPECTION ############


        finishSchema();
        clopen();

        //Load schema types into current transaction
        weight = mgmt.getPropertyKey("weight");
        id = mgmt.getPropertyKey("uid");
        someid = mgmt.getPropertyKey("someid");
        name = mgmt.getPropertyKey("name");
        value = mgmt.getPropertyKey("value");
        friend = mgmt.getEdgeLabel("friend");
        link = mgmt.getEdgeLabel("link");
        connect = mgmt.getEdgeLabel("connect");
        parent = mgmt.getEdgeLabel("parent");
        child = mgmt.getEdgeLabel("child");
        spouse = mgmt.getEdgeLabel("spouse");
        person = mgmt.getVertexLabel("person");
        tag = mgmt.getVertexLabel("tag");
        tweet = mgmt.getVertexLabel("tweet");


        // ######### INSPECTION & FAILURE (COPIED FROM ABOVE) ############

        assertTrue(mgmt.isOpen());
        assertEquals("weight",weight.toString());
        assertTrue(mgmt.containsRelationType("weight"));
        assertFalse(mgmt.containsRelationType("bla"));
        assertTrue(weight.isPropertyKey());
        assertFalse(weight.isEdgeLabel());
        assertEquals(Cardinality.SINGLE,weight.getCardinality());
        assertEquals(Cardinality.SINGLE,someid.getCardinality());
        assertEquals(Cardinality.SET,name.getCardinality());
        assertEquals(Cardinality.LIST,value.getCardinality());
        assertEquals(Object.class,someid.getDataType());
        assertEquals(Decimal.class,weight.getDataType());
        sig = ((InternalRelationType)value).getSignature();
        assertEquals(1,sig.length);
        assertEquals(weight.getLongId(),sig[0]);
        assertTrue(mgmt.getGraphIndex(id.getName()).isUnique());
        assertFalse(mgmt.getGraphIndex(someid.getName()).isUnique());

        assertEquals("friend",friend.getName());
        assertTrue(friend.isEdgeLabel());
        assertFalse(friend.isPropertyKey());
        assertEquals(Multiplicity.ONE2ONE,spouse.getMultiplicity());
        assertEquals(Multiplicity.ONE2MANY,child.getMultiplicity());
        assertEquals(Multiplicity.MANY2ONE,parent.getMultiplicity());
        assertEquals(Multiplicity.MULTI,friend.getMultiplicity());
        assertEquals(Multiplicity.SIMPLE, connect.getMultiplicity());
        assertTrue(link.isUnidirected());
        assertFalse(link.isDirected());
        assertFalse(child.isUnidirected());
        assertTrue(spouse.isDirected());
        assertFalse(((InternalRelationType) friend).isHiddenType());
        assertTrue(((InternalRelationType) friend).isHidden());
        assertEquals(0,((InternalRelationType) friend).getSignature().length);
        sig = ((InternalRelationType)connect).getSignature();
        assertEquals(2,sig.length);
        assertEquals(id.getLongId(),sig[0]);
        assertEquals(link.getLongId(),sig[1]);
        assertEquals(0,((InternalRelationType) friend).getSortKey().length);
        assertEquals(Order.DEFAULT,((InternalRelationType) friend).getSortOrder());
        assertEquals(SchemaStatus.ENABLED,((InternalRelationType)friend).getStatus());

        assertEquals(5,Iterables.size(mgmt.getRelationTypes(PropertyKey.class)));
        assertEquals(6,Iterables.size(mgmt.getRelationTypes(EdgeLabel.class)));
        assertEquals(11,Iterables.size(mgmt.getRelationTypes(RelationType.class)));
        assertEquals(3,Iterables.size(mgmt.getVertexLabels()));

        assertEquals("tweet",tweet.getName());
        assertTrue(mgmt.containsVertexLabel("person"));
        assertFalse(mgmt.containsVertexLabel("bla"));
        assertFalse(person.isPartitioned());
        assertFalse(person.isStatic());
        assertTrue(tag.isPartitioned());
        assertTrue(tweet.isStatic());

        //------ TRY INVALID STUFF --------

        //Failures
        try {
            //No datatype
            mgmt.makePropertyKey("fid").make();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //Already exists
            mgmt.makeEdgeLabel("link").unidirected().make();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //signature and sort-key collide
            ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("other")).
                    sortKey(someid, weight).signature(someid).make();
            fail();
        } catch (IllegalArgumentException e) {}
//        try {
//            //keys must be single-valued
//            ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("other")).
//                    sortKey(name, weight).make();
//            fail();
//        } catch (IllegalArgumentException e) {}
        try {
            //sort key requires the label to be non-constrained
            ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("other")).multiplicity(Multiplicity.SIMPLE).
                    sortKey(weight).make();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //sort key requires the label to be non-constrained
            ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("other")).multiplicity(Multiplicity.MANY2ONE).
                    sortKey(weight).make();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //Already exists
            mgmt.makeVertexLabel("tweet").make();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //only unidrected, Many2One labels are allowed in signatures
            mgmt.makeEdgeLabel("test").signature(friend).make();
            fail();
        } catch (IllegalArgumentException e) {}

        // ######### END INSPECTION ############

        /*
          ####### Make sure schema semantics are honored in transactions ######
        */

        clopen();

        weight = tx.getPropertyKey("weight");
        id = tx.getPropertyKey("uid");
        someid = tx.getPropertyKey("someid");
        name = tx.getPropertyKey("name");
        value = tx.getPropertyKey("value");
        friend = tx.getEdgeLabel("friend");
        link = tx.getEdgeLabel("link");
        connect = tx.getEdgeLabel("connect");
        parent = tx.getEdgeLabel("parent");
        child = tx.getEdgeLabel("child");
        spouse = tx.getEdgeLabel("spouse");
        person = tx.getVertexLabel("person");
        tag = tx.getVertexLabel("tag");
        tweet = tx.getVertexLabel("tweet");

        TitanTransaction tx2;
        assertNull(getVertex(id, "v1")); //shouldn't exist

        TitanVertex v = tx.addVertex();
        //test property keys
        v.addProperty(id, "v1");
        v.addProperty(weight, 1.5);
        v.setProperty(someid,"Hello");
        v.addProperty(name,"Bob");
        v.addProperty(name,"John");
        TitanProperty p = v.addProperty(value,11);
        p.setProperty(weight,22);
        p = v.addProperty(value,33.3);
        p.setProperty(weight,66.6);
        p = v.addProperty(value,11); //same values are supported for list-properties
        p.setProperty(weight,22);
        //test edges
        TitanVertex v12 = tx.addVertex(person), v13 = tx.addVertex(person);
        v12.setProperty(id, "v12");
        v13.setProperty(id, "v13");
        v12.addEdge(parent, v).setProperty(weight, 4.5);
        v13.addEdge(parent, v).setProperty(weight, 4.5);
        v.addEdge(child, v12);
        v.addEdge(child, v13);
        v.addEdge(spouse, v12);
        v.addEdge(friend,v12);
        v.addEdge(friend,v12); //supports multi edges
        TitanEdge edge = v.addEdge(connect, v12);
        edge.setProperty(id,"e1");
        edge.setProperty(link,v);
        v.addEdge(link,v13);
        TitanVertex v2 = tx.addVertex(tweet);
        v2.addEdge(link,v13);
        v12.addEdge(connect,v2);

        // ######### INSPECTION & FAILURE ############
        assertEquals(v, Iterables.getOnlyElement(tx.query().has(id, Cmp.EQUAL, "v1").vertices()));
        v = (TitanVertex)Iterables.getOnlyElement(tx.query().has(id,Cmp.EQUAL,"v1").vertices());
        v12 = (TitanVertex)Iterables.getOnlyElement(tx.query().has(id,Cmp.EQUAL,"v12").vertices());
        v13 = (TitanVertex)Iterables.getOnlyElement(tx.query().has(id,Cmp.EQUAL,"v13").vertices());
        try {
            //Invalid data type
            v.setProperty(weight, "x");
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //Only one "Bob" should be allowed
            v.addProperty(name,"John");
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //setProperty not supported for multi-properties
            v.setProperty(name,"Don");
            fail();
        } catch (IllegalArgumentException e) {}

        //Only one property for weight allowed
        v.addProperty(weight, 1.0);
        assertEquals(1,Iterables.size(v.getProperties(weight)));
        v.setProperty(weight,0.5);
        assertEquals(0.5,v.<Decimal>getProperty(weight).doubleValue(),0.00001);
        assertEquals("v1",v.getProperty(id));
        assertEquals(2,v.<List>getProperty(name).size());
        for (TitanProperty prop : v.getProperties(name)) {
            String nstr = prop.getValue();
            assertTrue(nstr.equals("Bob") || nstr.equals("John"));
        }
        assertTrue(v.<List>getProperty(value).size()>=3);
        for (TitanProperty prop : v.getProperties(value)) {
            assertEquals(prop.<Number>getValue().doubleValue()*2,prop.<Number>getProperty(weight).doubleValue(),0.00001);
        }
        //Ensure we can add additional values
        p = v.addProperty(value,44.4);
        p.setProperty(weight,88.8);
        assertEquals(v,Iterables.getOnlyElement(tx.query().has(someid,Cmp.EQUAL,"Hello").vertices()));

        //------- EDGES -------
        try {
            //multiplicity violation
            v12.addEdge(parent, v13);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //multiplicity violation
            v13.addEdge(child, v12);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //multiplicity violation
            v13.addEdge(spouse, v12);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //multiplicity violation
            v.addEdge(spouse, v13);
            fail();
        } catch (IllegalArgumentException e) {
        }
        assertEquals(2, Iterables.size(v.query().direction(IN).types(parent).edges()));
        assertEquals(1, Iterables.size(v12.query().direction(OUT).types(parent).has(weight,Cmp.EQUAL,4.5).edges()));
        assertEquals(1, Iterables.size(v13.query().direction(OUT).types(parent).has(weight,Cmp.EQUAL,4.5).edges()));
        assertEquals(v12,Iterables.getOnlyElement(v.getVertices(OUT,spouse.getName())));
        edge = (TitanEdge)Iterables.getOnlyElement(v.query().types(connect).direction(BOTH).edges());
        assertEquals(2,edge.getPropertyKeys().size());
        assertEquals("e1",edge.getProperty(id));
        assertEquals(v,edge.getProperty(link));
        try {
            //connect is simple
            v.addEdge(connect, v12);
            fail();
        } catch (IllegalArgumentException e) {
        }
        //Make sure "link" is unidirected
        assertEquals(1, v.query().types(link).direction(BOTH).count());
        assertEquals(0,Iterables.size(v13.query().types(link).direction(BOTH).edges()));
        //Assert we can add more friendships
        v.addEdge(friend,v12);
        v2 = (TitanVertex)Iterables.getOnlyElement(v12.getVertices(Direction.OUT,connect.getName()));
        assertEquals(v13,Iterables.getOnlyElement(v2.getEdges(OUT,link.getName())).getVertex(Direction.IN));

        assertEquals(BaseVertexLabel.DEFAULT_VERTEXLABEL,v.getVertexLabel());
        assertEquals(person,v12.getVertexLabel());
        assertEquals(person,v13.getVertexLabel());

        assertEquals(4, Iterables.size(tx.getVertices()));

        // ######### END INSPECTION & FAILURE ############

        clopen();

        weight = tx.getPropertyKey("weight");
        id = tx.getPropertyKey("uid");
        someid = tx.getPropertyKey("someid");
        name = tx.getPropertyKey("name");
        value = tx.getPropertyKey("value");
        friend = tx.getEdgeLabel("friend");
        link = tx.getEdgeLabel("link");
        connect = tx.getEdgeLabel("connect");
        parent = tx.getEdgeLabel("parent");
        child = tx.getEdgeLabel("child");
        spouse = tx.getEdgeLabel("spouse");
        person = tx.getVertexLabel("person");
        tag = tx.getVertexLabel("tag");
        tweet = tx.getVertexLabel("tweet");

        // ######### INSPECTION & FAILURE (copied from above) ############
        assertEquals(v,Iterables.getOnlyElement(tx.query().has(id,Cmp.EQUAL,"v1").vertices()));
        v = (TitanVertex)Iterables.getOnlyElement(tx.query().has(id,Cmp.EQUAL,"v1").vertices());
        v12 = (TitanVertex)Iterables.getOnlyElement(tx.query().has(id,Cmp.EQUAL,"v12").vertices());
        v13 = (TitanVertex)Iterables.getOnlyElement(tx.query().has(id,Cmp.EQUAL,"v13").vertices());
        try {
            //Invalid data type
            v.setProperty(weight, "x");
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //Only one "Bob" should be allowed
            v.addProperty(name,"John");
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //setProperty not supported for multi-properties
            v.setProperty(name,"Don");
            fail();
        } catch (IllegalArgumentException e) {}

        //Only one property for weight allowed
        v.addProperty(weight, 1.0);
        assertEquals(1,Iterables.size(v.getProperties(weight)));
        v.setProperty(weight,0.5);
        assertEquals(0.5,v.<Decimal>getProperty(weight).doubleValue(),0.00001);
        assertEquals("v1",v.getProperty(id));
        assertEquals(2,v.<List>getProperty(name).size());
        for (TitanProperty prop : v.getProperties(name)) {
            String nstr = prop.getValue();
            assertTrue(nstr.equals("Bob") || nstr.equals("John"));
        }
        assertTrue(v.<List>getProperty(value).size()>=3);
        for (TitanProperty prop : v.getProperties(value)) {
            assertEquals(prop.<Number>getValue().doubleValue()*2,prop.<Number>getProperty(weight).doubleValue(),0.00001);
        }
        //Ensure we can add additional values
        p = v.addProperty(value,44.4);
        p.setProperty(weight,88.8);
        assertEquals(v,Iterables.getOnlyElement(tx.query().has(someid,Cmp.EQUAL,"Hello").vertices()));

        //------- EDGES -------
        try {
            //multiplicity violation
            v12.addEdge(parent, v13);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //multiplicity violation
            v13.addEdge(child, v12);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //multiplicity violation
            v13.addEdge(spouse, v12);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //multiplicity violation
            v.addEdge(spouse, v13);
            fail();
        } catch (IllegalArgumentException e) {
        }
        assertEquals(2, Iterables.size(v.query().direction(IN).types(parent).edges()));
        assertEquals(1, Iterables.size(v12.query().direction(OUT).types(parent).has(weight,Cmp.EQUAL,4.5).edges()));
        assertEquals(1, Iterables.size(v13.query().direction(OUT).types(parent).has(weight,Cmp.EQUAL,4.5).edges()));
        assertEquals(v12,Iterables.getOnlyElement(v.getVertices(OUT,spouse.getName())));
        edge = (TitanEdge)Iterables.getOnlyElement(v.query().types(connect).direction(BOTH).edges());
        assertEquals(2,edge.getPropertyKeys().size());
        assertEquals("e1",edge.getProperty(id));
        assertEquals(v,edge.getProperty(link));
        try {
            //connect is simple
            v.addEdge(connect, v12);
            fail();
        } catch (IllegalArgumentException e) {
        }
        //Make sure "link" is unidirected
        assertEquals(1,v.query().types(link).direction(BOTH).count());
        assertEquals(0,Iterables.size(v13.query().types(link).direction(BOTH).edges()));
        //Assert we can add more friendships
        v.addEdge(friend,v12);
        v2 = (TitanVertex)Iterables.getOnlyElement(v12.getVertices(Direction.OUT,connect.getName()));
        assertEquals(v13,Iterables.getOnlyElement(v2.getEdges(OUT,link.getName())).getVertex(Direction.IN));

        assertEquals(4, Iterables.size(tx.getVertices()));

        // ######### END INSPECTION & FAILURE ############

        //Ensure index uniqueness enforcement
        tx2 = graph.newTransaction();
        try {
            TitanVertex vx = tx2.addVertex();
            try {
                //property is unique
                vx.setProperty(id,"v1");
                fail();
            } catch (IllegalArgumentException e) {}
            vx.setProperty(id,"unique");
            TitanVertex vx2 = tx2.addVertex();
            try {
                //property unique
                vx2.setProperty(id,"unique");
                fail();
            } catch (IllegalArgumentException e) {}
        } finally {
            tx2.rollback();
        }


        //Ensure that v2 is really static
        v2 = (TitanVertex)tx.getVertex(v2);
        assertEquals(tweet,v2.getVertexLabel());
        try {
            v2.setProperty(weight,11);
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            v2.addEdge(friend,v12);
            fail();
        } catch (IllegalArgumentException e) {}

        //Ensure that unidirected edges keep pointing to deleted vertices
        tx.getVertex(v13).remove();
        assertEquals(1,Iterables.size(v.query().types(link).direction(BOTH).edges()));

        //Finally, test the schema container
        SchemaContainer schemaContainer = new SchemaContainer(graph);
        assertTrue(schemaContainer.containsRelationType("weight"));
        assertTrue(schemaContainer.containsRelationType("friend"));
        assertTrue(schemaContainer.containsVertexLabel("person"));
        VertexLabelDefinition vld = schemaContainer.getVertexLabel("tag");
        assertTrue(vld.isPartitioned());
        assertFalse(vld.isStatic());
        PropertyKeyDefinition pkd = schemaContainer.getPropertyKey("name");
        assertEquals(Cardinality.SET,pkd.getCardinality());
        assertEquals(String.class,pkd.getDataType());
        EdgeLabelDefinition eld = schemaContainer.getEdgeLabel("child");
        assertEquals("child",eld.getName());
        assertEquals(child.getLongId(),eld.getLongId());
        assertEquals(Multiplicity.ONE2MANY,eld.getMultiplicity());
        assertFalse(eld.isUnidirected());
    }

    /**
     * Test the different data types that Titan supports natively and ensure that invalid data types aren't allowed
     */
    @Test
    public void testDataTypes() throws Exception {
        clopen(option(CUSTOM_ATTRIBUTE_CLASS,"attribute10"),SpecialInt.class.getCanonicalName(),
                option(CUSTOM_SERIALIZER_CLASS,"attribute10"),SpecialIntSerializer.class.getCanonicalName());

        PropertyKey sint = makeKey("int",SpecialInt.class);

        PropertyKey arrayKey = makeKey("barr",byte[].class);

        PropertyKey boolKey = makeKey("boolval",Boolean.class);

        PropertyKey birthday = makeKey("birthday",GregorianCalendar.class);

        PropertyKey geo = makeKey("geo", Geoshape.class);

        PropertyKey precision = makeKey("precise",Precision.class);

        PropertyKey any = mgmt.makePropertyKey("any").cardinality(Cardinality.LIST).dataType(Object.class).make();

        try {
            //Not a valid data type - primitive
            makeKey("pint",int.class);
            fail();
        } catch (IllegalArgumentException e) {
        }

        try {
            //Not a valid data type - interface
            makeKey("number", Number.class);
            fail();
        } catch (IllegalArgumentException e) {
        }

        finishSchema();
        clopen();

        boolKey = tx.getPropertyKey("boolval");
        sint = tx.getPropertyKey("int");
        arrayKey = tx.getPropertyKey("barr");
        birthday = tx.getPropertyKey("birthday");
        geo = tx.getPropertyKey("geo");
        precision = tx.getPropertyKey("precise");
        any = tx.getPropertyKey("any");

        assertEquals(Boolean.class, boolKey.getDataType());
        assertEquals(byte[].class, arrayKey.getDataType());
        assertEquals(Object.class, any.getDataType());

        final Calendar c = Calendar.getInstance();
        c.setTime(new SimpleDateFormat("ddMMyyyy").parse("28101978"));
        final Geoshape shape = Geoshape.box(10.0,10.0,20.0,20.0);

        TitanVertex v = tx.addVertex();
        v.addProperty(boolKey, true);
        v.setProperty(birthday, c);
        v.setProperty(sint,new SpecialInt(10));
        v.setProperty(arrayKey,new byte[]{1,2,3,4});
        v.setProperty(geo,shape);
        v.setProperty(precision,10.12345);
        v.addProperty(any, "Hello");
        v.addProperty(any,10l);
        HashMap<String,Integer> testmap = new HashMap<String, Integer>(1);
        testmap.put("test", 10);
        v.addProperty(any, testmap);

        // ######## VERIFICATION ##########
        assertTrue(v.<Boolean>getProperty(boolKey));
        assertEquals(10,v.<SpecialInt>getProperty(sint).getValue());
        assertEquals(c, v.getProperty(birthday));
        assertEquals(4, v.<byte[]>getProperty(arrayKey).length);
        assertEquals(shape, v.<Geoshape>getProperty(geo));
        assertEquals(10.12345,v.<Precision>getProperty(precision).doubleValue(),0.000001);
        assertEquals(3,Iterables.size(v.getProperties(any)));
        for (TitanProperty prop : v.getProperties(any)) {
            Object value = prop.getValue();
            if (value instanceof String) assertEquals("Hello",value);
            else if (value instanceof Long) assertEquals(10l,value);
            else if (value instanceof Map) {
                HashMap<String,Integer> map = (HashMap<String,Integer>)value;
                assertEquals(1,map.size());
            } else fail();
        }

        clopen();

        v = (TitanVertex)tx.getVertex(v);

        // ######## VERIFICATION (copied from above) ##########
        boolKey = tx.getPropertyKey("boolval");
        sint = tx.getPropertyKey("int");
        arrayKey = tx.getPropertyKey("barr");
        birthday = tx.getPropertyKey("birthday");
        geo = tx.getPropertyKey("geo");
        precision = tx.getPropertyKey("precise");
        any = tx.getPropertyKey("any");

        assertTrue(v.<Boolean>getProperty(boolKey));
        assertEquals(10,v.<SpecialInt>getProperty(sint).getValue());
        assertEquals(c, v.getProperty(birthday));
        assertEquals(4, v.<byte[]>getProperty(arrayKey).length);
        assertEquals(shape, v.<Geoshape>getProperty(geo));
        assertEquals(10.12345,v.<Precision>getProperty(precision).doubleValue(),0.000001);
        assertEquals(3,Iterables.size(v.getProperties(any)));
        for (TitanProperty prop : v.getProperties(any)) {
            Object value = prop.getValue();
            if (value instanceof String) assertEquals("Hello",value);
            else if (value instanceof Long) assertEquals(10l,value);
            else if (value instanceof Map) {
                HashMap<String,Integer> map = (HashMap<String,Integer>)value;
                assertEquals(1,map.size());
            } else fail();
        }
    }

    /**
     * This tests a special scenario under which a schema type is defined in a (management) transaction
     * and then accessed in a concurrent transaction.
     * Also ensures that unique property values are enforced within and across transactions
     */
    @Test
    public void testTransactionalScopeOfSchemaTypes() {
        makeVertexIndexedUniqueKey("domain",String.class);
        finishSchema();

        TitanVertex v1,v2;
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

    /**
     * Tests the automatic creation of types
     */
    @Test
    public void testAutomaticTypeCreation() {

        assertFalse(tx.containsVertexLabel("person"));
        assertFalse(tx.containsVertexLabel("person"));
        assertFalse(tx.containsRelationType("value"));
        PropertyKey value = tx.getPropertyKey("value");
        assertTrue(tx.containsRelationType("value"));
        TitanVertex v = tx.addVertex("person");
        assertTrue(tx.containsVertexLabel("person"));
        assertEquals("person",v.getLabel());
        assertFalse(tx.containsRelationType("knows"));
        Edge e = v.addEdge("knows",v);
        assertTrue(tx.containsRelationType("knows"));
        assertNotNull(tx.getEdgeLabel(e.getLabel()));

        clopen(option(AUTO_TYPE),"none");

        assertTrue(tx.containsRelationType("value"));
        assertTrue(tx.containsVertexLabel("person"));
        assertTrue(tx.containsRelationType("knows"));
        v = tx.getVertex(v.getLongId());

        //Cannot create new labels
        try {
            tx.addVertex("org");
            fail();
        } catch (IllegalArgumentException ex) {}
        try {
            v.setProperty("bla",5);
            fail();
        } catch (IllegalArgumentException ex) {}
        try {
            v.addEdge("blub",v);
            fail();
        } catch (IllegalArgumentException ex) {}
    }

    @Test
    public void testSchemaNameChange() {
        PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        EdgeLabel knows = mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.MULTI).make();
        mgmt.buildEdgeIndex(knows, "byTime", Direction.BOTH, time);
        mgmt.buildIndex("timeIndex",Vertex.class).addKey(time).buildCompositeIndex();
        mgmt.makeVertexLabel("people").make();
        finishSchema();

        //CREATE SMALL GRAPH
        TitanVertex v = tx.addVertex("people");
        v.setProperty("time",5);
        v.addEdge("knows",v).setProperty("time",11);

        newTx();
        v = (TitanVertex)Iterables.getOnlyElement(tx.query().has("time",5).vertices());
        assertNotNull(v);
        assertEquals("people",v.getLabel());
        assertEquals(5,v.getProperty("time"));
        assertEquals(1,Iterables.size(v.getEdges(IN,"knows")));
        assertEquals(1,v.query().labels("knows").direction(OUT).has("time",11).count());
        newTx();

        //UPDATE SCHEMA NAMES

        assertTrue(mgmt.containsRelationType("knows"));
        knows = mgmt.getEdgeLabel("knows");
        mgmt.changeName(knows,"know");
        assertEquals("know",knows.getName());

        assertTrue(mgmt.containsRelationIndex(knows,"byTime"));
        RelationTypeIndex rindex = mgmt.getRelationIndex(knows,"byTime");
        assertEquals("byTime",rindex.getName());
        mgmt.changeName(rindex,"overTime");
        assertEquals("overTime",rindex.getName());

        assertTrue(mgmt.containsVertexLabel("people"));
        VertexLabel vl = mgmt.getVertexLabel("people");
        mgmt.changeName(vl,"person");
        assertEquals("person",vl.getName());

        assertTrue(mgmt.containsGraphIndex("timeIndex"));
        TitanGraphIndex gindex = mgmt.getGraphIndex("timeIndex");
        mgmt.changeName(gindex,"byTime");
        assertEquals("byTime",gindex.getName());

        finishSchema();

        //VERIFY UPDATES IN MGMT SYSTEM

        assertTrue(mgmt.containsRelationType("know"));
        assertFalse(mgmt.containsRelationType("knows"));
        knows = mgmt.getEdgeLabel("know");

        assertTrue(mgmt.containsRelationIndex(knows,"overTime"));
        assertFalse(mgmt.containsRelationIndex(knows,"byTime"));

        assertTrue(mgmt.containsVertexLabel("person"));
        assertFalse(mgmt.containsVertexLabel("people"));

        assertTrue(mgmt.containsGraphIndex("byTime"));
        assertFalse(mgmt.containsGraphIndex("timeIndex"));

        //VERIFY UPDATES IN TRANSACTION
        newTx();
        v = (TitanVertex)Iterables.getOnlyElement(tx.query().has("time",5).vertices());
        assertNotNull(v);
        assertEquals("person",v.getLabel());
        assertEquals(5,v.getProperty("time"));
        assertEquals(1,Iterables.size(v.getEdges(Direction.IN,"know")));
        assertEquals(0,Iterables.size(v.getEdges(Direction.IN,"knows")));
        assertEquals(1,v.query().labels("know").direction(OUT).has("time",11).count());

    }

    @Test
    public void testIndexUpdatesWithoutReindex() throws InterruptedException {
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

    }

    @Test
    public void testIndexUpdateSyncWithMultipleInstances() throws InterruptedException {
        clopen( option(LOG_SEND_DELAY,MANAGEMENT_LOG),new StandardDuration(0,TimeUnit.MILLISECONDS),
                option(KCVSLog.LOG_READ_LAG_TIME,MANAGEMENT_LOG),new StandardDuration(50,TimeUnit.MILLISECONDS),
                option(LOG_READ_INTERVAL,MANAGEMENT_LOG),new StandardDuration(250,TimeUnit.MILLISECONDS)
        );

        StandardTitanGraph graph2 = (StandardTitanGraph) TitanFactory.open(config);
        TitanTransaction tx2;

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        finishSchema();

        ElementHelper.setProperties(tx.addVertex(),"name","v1");
        newTx();
        evaluateQuery(tx.query().has("name","v1"),ElementCategory.VERTEX,1,new boolean[]{false,true});
        tx2 = graph2.newTransaction();
        evaluateQuery(tx2.query().has("name","v1"),ElementCategory.VERTEX,1,new boolean[]{false,true});
        //Leave tx2 open to delay acknowledgement

        mgmt.buildIndex("theIndex",Vertex.class).addKey(mgmt.getPropertyKey("name")).buildCompositeIndex();
        mgmt.commit();

        TitanTransaction tx3 = graph2.newTransaction();
        ElementHelper.setProperties(tx3.addVertex(),"name","v2");
        tx3.commit();
        newTx();
        ElementHelper.setProperties(tx.addVertex(),"name","v3");
        tx.commit();

        Thread.sleep(2000); //Wait for the index to register in graph2
        finishSchema();
        try {
            mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.ENABLE_INDEX);
            fail(); //Open tx2 should not make this possible
        } catch (IllegalArgumentException e) {}
        finishSchema();
        tx2.commit(); //Release transaction and wait a little for registration which should make enabling possible
        Thread.sleep(500);
        mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.ENABLE_INDEX);
        finishSchema();

        tx2 = graph2.newTransaction();
        ElementHelper.setProperties(tx2.addVertex(),"name","v4"); //Should be added to index but index not yet enabled
        tx2.commit();

        newTx();
        evaluateQuery(tx.query().has("name","v1"),ElementCategory.VERTEX,0,new boolean[]{true,true},"theIndex");
        evaluateQuery(tx.query().has("name","v2"),ElementCategory.VERTEX,1,new boolean[]{true,true},"theIndex");
        evaluateQuery(tx.query().has("name","v3"),ElementCategory.VERTEX,1,new boolean[]{true,true},"theIndex");
        evaluateQuery(tx.query().has("name","v4"),ElementCategory.VERTEX,1,new boolean[]{true,true},"theIndex");

        Thread.sleep(2000);
        tx2 = graph2.newTransaction();
        evaluateQuery(tx2.query().has("name","v1"),ElementCategory.VERTEX,0,new boolean[]{true,true},"theIndex");
        evaluateQuery(tx2.query().has("name","v2"),ElementCategory.VERTEX,1,new boolean[]{true,true},"theIndex");
        evaluateQuery(tx2.query().has("name","v3"),ElementCategory.VERTEX,1,new boolean[]{true,true},"theIndex");
        evaluateQuery(tx2.query().has("name","v4"),ElementCategory.VERTEX,1,new boolean[]{true,true},"theIndex");
        tx2.commit();

        //Finally test retrieving and closing open instances

        Set<String> openInstances = mgmt.getOpenInstances();
        assertEquals(2,openInstances.size());
        assertTrue(openInstances.contains(graph.getConfiguration().getUniqueGraphId()));
        assertTrue(openInstances.contains(graph2.getConfiguration().getUniqueGraphId()));
        mgmt.forceCloseInstance(graph2.getConfiguration().getUniqueGraphId());

        graph2.shutdown();

    }

   /* ==================================================================================
                            ADVANCED
     ==================================================================================*/

    /**
     * Test the correct application of {@link com.thinkaurelius.titan.graphdb.types.system.ImplicitKey}
     * to vertices, edges, and properties.
     *
     * Additionally tests RelationIdentifier since this is closely related to ADJACENT and TITANID implicit keys.
     */
    @Test
    public void testImplicitKey() {
        TitanVertex v = graph.addVertex(null), u = graph.addVertex(null);
        v.setProperty("name","Dan");
        Edge e = v.addEdge("knows",u);
        graph.commit();
        RelationIdentifier eid = (RelationIdentifier)e.getId();

        assertEquals(v.getId(),v.getProperty("id"));
        assertEquals(eid,e.getProperty("id"));
        assertEquals("knows",e.getProperty("label"));
        assertEquals(BaseVertexLabel.DEFAULT_VERTEXLABEL.getName(),v.getProperty("label"));
        assertEquals(1,v.query().labels("knows").direction(BOTH).has("id",eid).count());
        assertEquals(0,v.query().labels("knows").direction(BOTH).has("id",RelationIdentifier.get(new long[]{4,5,6,7})).count());
        assertEquals(1,v.query().labels("knows").direction(BOTH).has("$titanid",eid.getRelationId()).count());
        assertEquals(0,v.query().labels("knows").direction(BOTH).has("$titanid",110111).count());
        assertEquals(1,v.query().has("$adjacent",u.getLongId()).count());
        assertEquals(1,v.query().has("$adjacent",(int)u.getLongId()).count());
        try {
            //Not a valid vertex
             assertEquals(0,v.query().has("$adjacent",110111).count());
            fail();
        } catch (IllegalArgumentException ex) {}
        assertNotNull(graph.getEdge(eid));
        assertEquals(eid,graph.getEdge(eid).getId());

        //Test edge retrieval
    }

    /**
     * Tests that self-loop edges are handled and counted correctly
     */
    @Test
    public void testSelfLoop() {
        Vertex v = tx.addVertex();
        tx.addEdge(null, v, v, "self");
        assertEquals(1, Iterables.size(v.getEdges(Direction.OUT, "self")));
        assertEquals(1, Iterables.size(v.getEdges(Direction.IN, "self")));
        assertEquals(2, Iterables.size(v.getEdges(BOTH,"self")));
        clopen();
        v = tx.getVertex(v.getId());
        assertNotNull(v);
        assertEquals(1, Iterables.size(v.getEdges(Direction.IN, "self")));
        assertEquals(1, Iterables.size(v.getEdges(Direction.OUT, "self")));
        assertEquals(1, Iterables.size(v.getEdges(Direction.IN, "self")));
        assertEquals(2, Iterables.size(v.getEdges(BOTH,"self")));
    }

    /**
     * Tests that elements can be accessed beyond their transactional boundaries if they
     * are bound to single-threaded graph transactions
     */
    @Test
    public void testThreadBoundTx() {
        PropertyKey t = mgmt.makePropertyKey("type").dataType(Integer.class).make();
        mgmt.buildIndex("etype",Edge.class).addKey(t).buildCompositeIndex();
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

    /**
     * Verifies transactional isolation and internal vertex existence checking
     */
    @Test
    public void testTransactionIsolation() {
        // Create edge label before attempting to write it from concurrent transactions
        makeLabel("knows");
        finishSchema();

        TitanTransaction tx1 = graph.newTransaction();
        TitanTransaction tx2 = graph.newTransaction();

        //Verify that using vertices across transactions is prohibited
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

        //Test unidirected edge with and without internal existence check
        newTx();
        v21 = tx.getVertex(v21.getLongId());
        tx.makeEdgeLabel("link").unidirected().make();
        TitanVertex v3 = tx.addVertex();
        v21.addEdge("link", v3);
        newTx();
        v21 = tx.getVertex(v21.getLongId());
        v3 = (TitanVertex) Iterables.getOnlyElement(v21.getVertices(OUT, "link"));
        assertFalse(v3.isRemoved());
        v3.remove();
        newTx();
        v21 = tx.getVertex(v21.getLongId());
        v3 = (TitanVertex) Iterables.getOnlyElement(v21.getVertices(OUT, "link"));
        assertFalse(v3.isRemoved());
        newTx();

        TitanTransaction tx3 = graph.buildTransaction().checkInternalVertexExistence().start();
        v21 = tx3.getVertex(v21.getLongId());
        v3 = (TitanVertex) Iterables.getOnlyElement(v21.getVertices(OUT, "link"));
        assertTrue(v3.isRemoved());
        tx3.commit();
    }


    /**
     * Tests multi-valued properties with special focus on indexing and incident unidirectional edges
     * which is not tested in {@link #testSchemaTypes()}
     *
     * -->TODO: split and move this into other test cases: ordering to query, indexing to index
     */
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
        mgmt.buildIndex(foo,Vertex.class).addKey(f).buildCompositeIndex();
        PropertyKey b = mgmt.makePropertyKey(bar).dataType(String.class).cardinality(Cardinality.LIST).make();
        mgmt.buildIndex(bar,Vertex.class).addKey(b).buildCompositeIndex();
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
        v = tx.getVertex(v.getLongId());
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
    public void testLocalGraphConfiguration() {
        setIllegalGraphOption(STORAGE_READONLY, ConfigOption.Type.LOCAL, true);
    }

    @Test
    public void testMaskableGraphConfig() {
        setAndCheckGraphOption(DB_CACHE, ConfigOption.Type.MASKABLE, true, false);
    }

    @Test
    public void testGlobalGraphConfig() {
        setAndCheckGraphOption(SYSTEM_LOG_TRANSACTIONS, ConfigOption.Type.GLOBAL, true, false);
    }

    @Test
    public void testGlobalOfflineGraphConfig() {
        setAndCheckGraphOption(DB_CACHE_TIME, ConfigOption.Type.GLOBAL_OFFLINE, 500L, 777L);
    }

    @Test
    public void testFixedGraphConfig() {
        setIllegalGraphOption(INITIAL_TITAN_VERSION, ConfigOption.Type.FIXED, "foo");
    }

    @Test
    public void testTransactionConfiguration() {
        // Superficial tests for a few transaction builder methods

        // Test read-only transaction
        TitanTransaction readOnlyTx = graph.buildTransaction().readOnly().start();
        try {
            readOnlyTx.addVertex();
            readOnlyTx.commit();
            fail("Read-only transactions should not be able to add a vertex and commit");
        } catch (Throwable t) {
            if (readOnlyTx.isOpen())
                readOnlyTx.rollback();
        }

        // Test custom log identifier
        String logID = "spam";
        StandardTitanTx customLogIDTx = (StandardTitanTx)graph.buildTransaction().setLogIdentifier(logID).start();
        assertEquals(logID, customLogIDTx.getConfiguration().getLogIdentifier());
        customLogIDTx.rollback();

        // Test timestamp
        long customTimestamp = -42L;
        StandardTitanTx customTimeTx = (StandardTitanTx)graph.buildTransaction().setCommitTime(customTimestamp, TimeUnit.MILLISECONDS).start();
        assertTrue(customTimeTx.getConfiguration().hasCommitTime());
        assertEquals(customTimestamp, customTimeTx.getConfiguration().getCommitTime().getTimestamp(TimeUnit.MILLISECONDS));
        customTimeTx.rollback();
    }

    private <T> void setAndCheckGraphOption(ConfigOption<T> opt, ConfigOption.Type requiredType, T firstValue, T secondValue) {
        // Sanity check: make sure the Type of the configoption is what we expect
        Preconditions.checkState(opt.getType().equals(requiredType));
        final EnumSet<ConfigOption.Type> allowedTypes =
                EnumSet.of(ConfigOption.Type.GLOBAL,
                           ConfigOption.Type.GLOBAL_OFFLINE,
                           ConfigOption.Type.MASKABLE);
        Preconditions.checkState(allowedTypes.contains(opt.getType()));

        // Sanity check: it's kind of pointless for the first and second values to be identical
        Preconditions.checkArgument(!firstValue.equals(secondValue));

        // Get full string path of config option
        final String path = ConfigElement.getPath(opt);

        // Set and check initial value before and after database restart
        mgmt.set(path, firstValue);
        assertEquals(firstValue.toString(), mgmt.get(path));
        // Close open tx first.  This is specific to BDB + GLOBAL_OFFLINE.
        // Basically: the BDB store manager throws a fit if shutdown is called
        // with one or more transactions still open, and GLOBAL_OFFLINE calls
        // shutdown on our behalf when we commit this change.
        tx.rollback();
        mgmt.commit();
        clopen();
        // Close tx again following clopen
        tx.rollback();
        assertEquals(firstValue.toString(), mgmt.get(path));

        // Set and check updated value before and after database restart
        mgmt.set(path, secondValue);
        assertEquals(secondValue.toString(), mgmt.get(path));
        mgmt.commit();
        clopen();
        tx.rollback();
        assertEquals(secondValue.toString(), mgmt.get(path));

        // Open a separate graph "g2"
        TitanGraph g2 = TitanFactory.open(config);
        TitanManagement m2 = g2.getManagementSystem();
        assertEquals(secondValue.toString(), m2.get(path));

        // GLOBAL_OFFLINE options should be unmodifiable with g2 open
        if (opt.getType().equals(ConfigOption.Type.GLOBAL_OFFLINE)) {
            try {
                mgmt.set(path, firstValue);
                mgmt.commit();
                fail("Option " + path + " with type "+ ConfigOption.Type.GLOBAL_OFFLINE + " should not be modifiable with concurrent instances");
            } catch (RuntimeException e) {
                log.debug("Caught expected exception", e);
            }
            assertEquals(secondValue.toString(), mgmt.get(path));
        // GLOBAL and MASKABLE should be modifiable even with g2 open
        } else {
            mgmt.set(path, firstValue);
            assertEquals(firstValue.toString(), mgmt.get(path));
            mgmt.commit();
            clopen();
            assertEquals(firstValue.toString(), mgmt.get(path));
        }

        m2.rollback();
        g2.shutdown();
    }

    private <T> void setIllegalGraphOption(ConfigOption<T> opt, ConfigOption.Type requiredType, T attemptedValue) {
        // Sanity check: make sure the Type of the configoption is what we expect
        final ConfigOption.Type type = opt.getType();
        Preconditions.checkState(type.equals(requiredType));
        Preconditions.checkArgument(requiredType.equals(ConfigOption.Type.LOCAL) ||
                                    requiredType.equals(ConfigOption.Type.FIXED));

        // Get full string path of config option
        final String path = ConfigElement.getPath(opt);


        // Try to read the option
        try {
            mgmt.get(path);
        } catch (Throwable t) {
            log.debug("Caught expected exception", t);
        }

        // Try to modify the option
        try {
            mgmt.set(path, attemptedValue);
            mgmt.commit();
            fail("Option " +  path + " with type " + type + " should not be modifiable in the persistent graph config");
        } catch (Throwable t) {
            log.debug("Caught expected exception", t);
        }
    }


   /* ==================================================================================
                            CONSISTENCY
     ==================================================================================*/

    /**
     * Tests the correct application of ConsistencyModifiers across transactional boundaries
     */
    @Test
    public void testConsistencyEnforcement() {
        PropertyKey id = makeVertexIndexedUniqueKey("uid",Integer.class);
        PropertyKey name = makeKey("name",String.class);
        mgmt.setConsistency(id, ConsistencyModifier.LOCK);
        mgmt.setConsistency(name, ConsistencyModifier.LOCK);
        mgmt.setConsistency(mgmt.getGraphIndex("uid"),ConsistencyModifier.LOCK);
        EdgeLabel knows = mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.SIMPLE).make();
        EdgeLabel spouse = mgmt.makeEdgeLabel("spouse").multiplicity(Multiplicity.ONE2ONE).make();
        EdgeLabel connect = mgmt.makeEdgeLabel("connect").multiplicity(Multiplicity.MULTI).make();
        EdgeLabel related = mgmt.makeEdgeLabel("related").multiplicity(Multiplicity.MULTI).make();
        mgmt.setConsistency(knows, ConsistencyModifier.LOCK);
        mgmt.setConsistency(spouse, ConsistencyModifier.LOCK);
        mgmt.setConsistency(related,ConsistencyModifier.FORK);
        finishSchema();

        name = tx.getPropertyKey("name");
        connect = tx.getEdgeLabel("connect");
        related = tx.getEdgeLabel("related");

        TitanVertex v1 = tx.addVertex();
        v1.setProperty("uid",1);
        TitanVertex v2 = tx.addVertex();
        v2.setProperty("uid",2);
        TitanVertex v3 = tx.addVertex();
        v3.setProperty("uid",3);

        TitanEdge e1 = v1.addEdge(connect.getName(),v2);
        e1.setProperty(name.getName(),"e1");
        TitanEdge e2 = v1.addEdge(related.getName(),v2);
        e2.setProperty(name.getName(),"e2");

        newTx();
        name = tx.getPropertyKey("name");
        connect = tx.getEdgeLabel("connect");
        related = tx.getEdgeLabel("related");
        v1 = tx.getVertex(v1.getLongId());
        /*
         ==== check fork, no fork behavior
         */
        long e1id = e1.getLongId();
        long e2id = e2.getLongId();
        e1 = (TitanEdge)Iterables.getOnlyElement(v1.getEdges(OUT,connect.getName()));
        assertEquals("e1",e1.getProperty(name.getName()));
        assertEquals(e1id,e1.getLongId());
        e2 = (TitanEdge)Iterables.getOnlyElement(v1.getEdges(OUT,related.getName()));
        assertEquals("e2",e2.getProperty(name.getName()));
        assertEquals(e2id,e2.getLongId());
        //Update edges - one should simply update, the other fork
        e1.setProperty(name.getName(),"e1.2");
        e2.setProperty(name.getName(),"e2.2");

        newTx();
        name = tx.getPropertyKey("name");
        connect = tx.getEdgeLabel("connect");
        related = tx.getEdgeLabel("related");
        v1 = tx.getVertex(v1.getLongId());

        e1 = (TitanEdge)Iterables.getOnlyElement(v1.getEdges(OUT,connect.getName()));
        assertEquals("e1.2",e1.getProperty(name.getName()));
        assertEquals(e1id,e1.getLongId()); //should have same id
        e2 = (TitanEdge)Iterables.getOnlyElement(v1.getEdges(OUT,related.getName()));
        assertEquals("e2.2",e2.getProperty(name.getName()));
        assertNotEquals(e2id,e2.getLongId()); //should have different id since forked

        clopen();

        /*
         === check cross transaction
         */
        final Random random = new Random();
        final long vids[] = {v1.getLongId(),v2.getLongId(),v3.getLongId()};
        //1) Index uniqueness
        executeLockConflictingTransactionJobs(graph,new TransactionJob() {
            private int pos = 0;
            @Override
            public void run(TitanTransaction tx) {
                Vertex u = tx.getVertex(vids[pos++]);
                u.setProperty("uid",5);
            }
        });
        //2) Property out-uniqueness
        executeLockConflictingTransactionJobs(graph,new TransactionJob() {
            @Override
            public void run(TitanTransaction tx) {
                Vertex u = tx.getVertex(vids[0]);
                u.setProperty("name","v"+random.nextInt(10));
            }
        });
        //3) knows simpleness
        executeLockConflictingTransactionJobs(graph,new TransactionJob() {
            @Override
            public void run(TitanTransaction tx) {
                Vertex u1 = tx.getVertex(vids[0]), u2 = tx.getVertex(vids[1]);
                u1.addEdge("knows",u2);
            }
        });
        //4) knows one2one (in 2 separate configurations)
        executeLockConflictingTransactionJobs(graph,new TransactionJob() {
            private int pos = 1;
            @Override
            public void run(TitanTransaction tx) {
                Vertex u1 = tx.getVertex(vids[0]),
                        u2 = tx.getVertex(vids[pos++]);
                u1.addEdge("spouse",u2);
            }
        });
        executeLockConflictingTransactionJobs(graph,new TransactionJob() {
            private int pos = 1;
            @Override
            public void run(TitanTransaction tx) {
                Vertex u1 = tx.getVertex(vids[pos++]),
                        u2 = tx.getVertex(vids[0]);
                u1.addEdge("spouse",u2);
            }
        });

//        TitanTransaction tx1 = graph.newTransaction();
//        TitanTransaction tx2 = graph.newTransaction();
//        Vertex u1 = tx1.getVertex(vids[0]);
//        Vertex u2 = tx2.getVertex(vids[0]);
//        assertEquals(u1.getId(),u2.getId());
//        u1.setProperty("name","u1");
//        u2.setProperty("name","u2");
//        tx1.commit();
//        tx2.commit();
//        tx1 = graph.newTransaction();
//        System.out.println(tx1.getVertex(vids[0]).getProperty("name"));
//        tx1.rollback();

        //######### TRY INVALID CONSISTENCY
        try {
            //Fork does not apply to constrained types
            mgmt.setConsistency(mgmt.getPropertyKey("name"),ConsistencyModifier.FORK);
            fail();
        } catch (IllegalArgumentException e) {}
    }

    /**
     * A piece of logic to be executed in a transactional context
     */
    private static interface TransactionJob {
        public void run(TitanTransaction tx);
    }

    /**
     * Executes a transaction job in two parallel transactions under the assumptions that the two transactions
     * should conflict and the one committed later should throw a locking exception.
     *
     * @param graph
     * @param job
     */
    private void executeLockConflictingTransactionJobs(TitanGraph graph, TransactionJob job) {
        TitanTransaction tx1 = graph.newTransaction();
        TitanTransaction tx2 = graph.newTransaction();
        job.run(tx1);
        job.run(tx2);
        /*
         * Under pessimistic locking, tx1 should abort and tx2 should commit.
         * Under optimistic locking, tx1 may commit and tx2 may abort.
         */
        if (isLockingOptimistic()) {
            tx1.commit();
            try {
                tx2.commit();
                fail("Storage backend does not abort conflicting transactions");
            } catch (TitanException e) {
            }
        } else {
            try {
                tx1.commit();
                fail("Storage backend does not abort conflicting transactions");
            } catch (TitanException e) {
            }
            tx2.commit();
        }
    }

    @Test
    public void testConcurrentConsistencyEnforcement() throws Exception {
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        TitanGraphIndex nameIndex = mgmt.buildIndex("name", Vertex.class)
                .addKey(name).unique().buildCompositeIndex();
        mgmt.setConsistency(nameIndex, ConsistencyModifier.LOCK);
        EdgeLabel married = mgmt.makeEdgeLabel("married").multiplicity(Multiplicity.ONE2ONE).make();
        mgmt.setConsistency(married,ConsistencyModifier.LOCK);
        EdgeLabel friend = mgmt.makeEdgeLabel("friend").multiplicity(Multiplicity.MULTI).make();
        finishSchema();

        TitanVertex baseV = tx.addVertex();
        baseV.setProperty("name","base");
        newTx();
        final long baseVid = baseV.getLongId();
        final String nameA = "a", nameB = "b";
        final int parallelThreads = 4;
        final AtomicInteger totalExe = new AtomicInteger();

        int numSuccess = executeParallelTransactions(new TransactionJob() {
            @Override
            public void run(TitanTransaction tx) {
                TitanVertex a = tx.addVertex();
                TitanVertex base = tx.getVertex(baseVid);
                base.addEdge("married",a);
            }
        },parallelThreads);

        assertTrue("At most 1 tx should succeed: " + numSuccess,numSuccess<=1);

        numSuccess = executeParallelTransactions(new TransactionJob() {
            @Override
            public void run(TitanTransaction tx) {
                Vertex a = tx.addVertex();
                a.setProperty("name",nameA);
                Vertex b = tx.addVertex();
                b.setProperty("name",nameB);
                b.addEdge("friend",b);
            }
        },parallelThreads);

        newTx();
        int numA = Iterables.size(tx.query().has("name",nameA).vertices());
        int numB = Iterables.size(tx.query().has("name",nameB).vertices());
//        System.out.println(numA + " - " + numB);
        assertTrue("At most 1 tx should succeed: " + numSuccess,numSuccess<=1);
        assertTrue(numA<=1);
        assertTrue(numB<=1);
    }

    private int executeSerialTransaction(final TransactionJob job, int number) {
        final AtomicInteger txSuccess = new AtomicInteger(0);
        for (int i = 0; i < number; i++) {
            TitanTransaction tx = graph.newTransaction();
            try {
                job.run(tx);
                tx.commit();
                txSuccess.incrementAndGet();
            } catch (Exception ex) {
                tx.rollback();
                ex.printStackTrace();
            }
        }
        return txSuccess.get();
    }

    private int executeParallelTransactions(final TransactionJob job, int number) {
        final CountDownLatch startLatch = new CountDownLatch(number);
        final CountDownLatch finishLatch = new CountDownLatch(number);
        final AtomicInteger txSuccess = new AtomicInteger(0);
        for (int i = 0; i < number; i++) {
            new Thread() {
                public void run() {
                    awaitAllThreadsReady();
                    TitanTransaction tx = graph.newTransaction();
                    try {
                        job.run(tx);
                        tx.commit();
                        txSuccess.incrementAndGet();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        if (tx.isOpen()) tx.rollback();
                    } finally {
                        finishLatch.countDown();
                    }
                }

                private void awaitAllThreadsReady() {
                    startLatch.countDown();
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }.start();
        }

        try {
            finishLatch.await(10000,TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return txSuccess.get();
    }

   /* ==================================================================================
                            VERTEX CENTRIC QUERIES
     ==================================================================================*/

   @Test
   public void testVertexCentricQuery() {
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
       for (int i = 0; i < vidsubset.length; i++) vidsubset[i] = vs[i + 3].getLongId();
       Arrays.sort(vidsubset);

       //##################################################
       //Queries from Cache
       //##################################################
       clopen();
       for (int i = 1; i < noVertices; i++) vs[i] = tx.getVertex(vs[i].getLongId());
       v = tx.getVertex(v.getLongId());
       u = tx.getVertex(u.getLongId());
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

       evaluateQuery(v.query().labels("connect").direction(OUT).interval("time", 3, 31),EDGE,10,1,new boolean[]{true,true});
       evaluateQuery(v.query().labels("connect").direction(OUT).has("time",15).has("weight",3.5),EDGE,1,1,new boolean[]{false,true});
       evaluateQuery(u.query().labels("connectDesc").direction(OUT).interval("time", 3, 31),EDGE,10,1,new boolean[]{true,true});
       assertEquals(10, v.query().labels("connect").direction(IN).interval("time", 3, 31).count());
       assertEquals(10, u.query().labels("connectDesc").direction(IN).interval("time", 3, 31).count());
       assertEquals(0, v.query().labels("connect").direction(OUT).has("time", null).count());
       assertEquals(10, v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertexIds().size());
       assertEquals(edgesPerLabel-10, v.query().labels("connect").direction(OUT).has("time", Compare.GREATER_THAN, 31).count());
       assertEquals(10, Iterables.size(v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertices()));
       assertEquals(3, v.query().labels("friend").direction(OUT).limit(3).count());
       evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", 0.5).limit(3),EDGE,3,1,new boolean[]{true,true});
       evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5),EDGE,3,1,new boolean[]{true,true});
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
       for (int i = 1; i < noVertices; i++) vs[i] = tx.getVertex(vs[i].getLongId());
       v = tx.getVertex(v.getLongId());
       u = tx.getVertex(u.getLongId());
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

       evaluateQuery(v.query().labels("connect").direction(OUT).interval("time", 3, 31),EDGE,10,1,new boolean[]{true,true});
       evaluateQuery(v.query().labels("connect").direction(OUT).has("time",15).has("weight",3.5),EDGE,1,1,new boolean[]{false,true});
       evaluateQuery(u.query().labels("connectDesc").direction(OUT).interval("time", 3, 31),EDGE,10,1,new boolean[]{true,true});
       assertEquals(10, v.query().labels("connect").direction(IN).interval("time", 3, 31).count());
       assertEquals(10, u.query().labels("connectDesc").direction(IN).interval("time", 3, 31).count());
       assertEquals(0, v.query().labels("connect").direction(OUT).has("time", null).count());
       assertEquals(10, v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertexIds().size());
       assertEquals(edgesPerLabel-10, v.query().labels("connect").direction(OUT).has("time", Compare.GREATER_THAN, 31).count());
       assertEquals(10, Iterables.size(v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertices()));
       assertEquals(3, v.query().labels("friend").direction(OUT).limit(3).count());
       evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", 0.5).limit(3),EDGE,3,1,new boolean[]{true,true});
       evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5),EDGE,3,1,new boolean[]{true,true});
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
       for (int i=0;i<qvs.length;i++) qvs2[i+1]=tx.getVertex(qvs[i].getLongId());
       qvs2[qvs2.length-1]=tx.addVertex();
       qvs2[0].addEdge("connect",qvs2[qvs2.length-1]);
       qvs2[qvs2.length-1].addEdge("connect", qvs2[0]);
       results = tx.multiQuery(qvs2).direction(IN).labels("connect").titanEdges();
       for (Iterable<TitanEdge> result : results.values()) assertEquals(1, Iterables.size(result));

   }

    @Test
    public void testRelationTypeIndexes() {
        PropertyKey weight = makeKey("weight",Decimal.class);
        PropertyKey time = makeKey("time",Long.class);

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.LIST).make();
        EdgeLabel connect = mgmt.makeEdgeLabel("connect").signature(time).make();
        EdgeLabel child = mgmt.makeEdgeLabel("child").multiplicity(Multiplicity.ONE2MANY).make();
        EdgeLabel link = mgmt.makeEdgeLabel("link").unidirected().make();

        RelationTypeIndex name1 = mgmt.buildPropertyIndex(name, "weightDesc", weight);

        RelationTypeIndex connect1 = mgmt.buildEdgeIndex(connect, "weightAsc", Direction.BOTH, Order.ASC, weight);
        RelationTypeIndex connect2 = mgmt.buildEdgeIndex(connect, "weightDesc", Direction.OUT, Order.DESC, weight);
        RelationTypeIndex connect3 = mgmt.buildEdgeIndex(connect, "time+weight", Direction.OUT, Order.DESC, time, weight);

        RelationTypeIndex child1 = mgmt.buildEdgeIndex(child, "time", Direction.OUT, time);

        RelationTypeIndex link1 = mgmt.buildEdgeIndex(link, "time", Direction.OUT, time);

        final String name1n = name1.getName(), connect1n = connect1.getName(), connect2n = connect2.getName(),
                connect3n = connect3.getName(), child1n = child1.getName(), link1n = link1.getName();

        // ########### INSPECTION & FAILURE ##############

        assertTrue(mgmt.containsRelationIndex(name,"weightDesc"));
        assertTrue(mgmt.containsRelationIndex(connect,"weightDesc"));
        assertFalse(mgmt.containsRelationIndex(child,"weightDesc"));
        assertEquals("time+weight",mgmt.getRelationIndex(connect,"time+weight").getName());
        assertNotNull(mgmt.getRelationIndex(link,"time"));
        assertNull(mgmt.getRelationIndex(name,"time"));
        assertEquals(1,Iterables.size(mgmt.getRelationIndexes(child)));
        assertEquals(3,Iterables.size(mgmt.getRelationIndexes(connect)));
        assertEquals(0,Iterables.size(mgmt.getRelationIndexes(weight)));
        try {
           //Name already exists
           mgmt.buildEdgeIndex(connect, "weightAsc", Direction.OUT, time);
           fail();
        } catch (IllegalArgumentException e) {}
//        try {
//           //Invalid key - must be single valued
//           mgmt.createEdgeIndex(connect,"blablub",Direction.OUT,name);
//           fail();
//        } catch (IllegalArgumentException e) {}
        try {
           //Not valid in this direction due to multiplicity constraint
           mgmt.buildEdgeIndex(child, "blablub", Direction.IN, time);
           fail();
        } catch (IllegalArgumentException e) {}
        try {
           //Not valid in this direction due to unidirectionality
           mgmt.buildEdgeIndex(link, "blablub", Direction.BOTH, time);
           fail();
        } catch (IllegalArgumentException e) {}

        // ########## END INSPECTION ###########

        finishSchema();

        weight = mgmt.getPropertyKey("weight");
        time = mgmt.getPropertyKey("time");

        name = mgmt.getPropertyKey("name");
        connect = mgmt.getEdgeLabel("connect");
        child = mgmt.getEdgeLabel("child");
        link = mgmt.getEdgeLabel("link");

        // ########### INSPECTION & FAILURE (copied from above) ##############

        assertTrue(mgmt.containsRelationIndex(name,"weightDesc"));
        assertTrue(mgmt.containsRelationIndex(connect,"weightDesc"));
        assertFalse(mgmt.containsRelationIndex(child, "weightDesc"));
        assertEquals("time+weight",mgmt.getRelationIndex(connect,"time+weight").getName());
        assertNotNull(mgmt.getRelationIndex(link, "time"));
        assertNull(mgmt.getRelationIndex(name, "time"));
        assertEquals(1, Iterables.size(mgmt.getRelationIndexes(child)));
        assertEquals(3,Iterables.size(mgmt.getRelationIndexes(connect)));
        assertEquals(0,Iterables.size(mgmt.getRelationIndexes(weight)));
        try {
           //Name already exists
           mgmt.buildEdgeIndex(connect, "weightAsc", Direction.OUT, time);
           fail();
        } catch (IllegalArgumentException e) {}
//        try {
//           //Invalid key - must be single valued
//           mgmt.createEdgeIndex(connect,"blablub",Direction.OUT,name);
//           fail();
//        } catch (IllegalArgumentException e) {}
        try {
           //Not valid in this direction due to multiplicity constraint
           mgmt.buildEdgeIndex(child, "blablub", Direction.IN, time);
           fail();
        } catch (IllegalArgumentException e) {}
        try {
           //Not valid in this direction due to unidirectionality
           mgmt.buildEdgeIndex(link, "blablub", Direction.BOTH, time);
           fail();
        } catch (IllegalArgumentException e) {}

        // ########## END INSPECTION ###########

        mgmt.rollback();

        /*
        ########## TEST WITHIN TRANSACTION ##################
        */

        weight = tx.getPropertyKey("weight");
        time = tx.getPropertyKey("time");

        name = tx.getPropertyKey("name");
        connect = tx.getEdgeLabel("connect");
        child = tx.getEdgeLabel("child");
        link = tx.getEdgeLabel("link");

        final int numV = 100;
        TitanVertex v = tx.addVertex();
        TitanVertex ns[] = new TitanVertex[numV];

        for (int i=0;i<numV;i++) {
            double w = (i*0.5)%5;
            long t = (i+77)%numV;
            TitanProperty p = v.addProperty(name,"v"+i);
            p.setProperty(weight,w);
            p.setProperty(time,t);

            ns[i]=tx.addVertex();
            for (EdgeLabel label : new EdgeLabel[]{connect,child,link}) {
                TitanEdge e = v.addEdge(label,ns[i]);
                e.setProperty(weight,w);
                e.setProperty(time,t);
            }
        }
        TitanVertex u = ns[0];
        VertexList vl;

        //######### QUERIES ##########
        v = (TitanVertex)tx.getVertex(v);
        u = (TitanVertex)tx.getVertex(u);

        evaluateQuery(v.query().keys(name.getName()).has(weight,Cmp.GREATER_THAN,3.6),
                PROPERTY, 2*numV/10, 1, new boolean[]{true,true});
        evaluateQuery(v.query().keys(name.getName()).has(weight,Cmp.LESS_THAN,0.9).orderBy(weight,Order.ASC),
                PROPERTY, 2*numV/10, 1, new boolean[]{true,true},weight,Order.ASC);
        evaluateQuery(v.query().keys(name.getName()).interval(weight, 1.1, 2.2).orderBy(weight,Order.DESC).limit(numV/10),
                PROPERTY, numV/10, 1, new boolean[]{true,false},weight,Order.DESC);
        evaluateQuery(v.query().keys(name.getName()).has(time,Cmp.EQUAL,5).orderBy(weight,Order.DESC),
                PROPERTY, 1, 1, new boolean[]{false,false},weight,Order.DESC);
        evaluateQuery(v.query().keys(name.getName()),
                PROPERTY, numV, 1, new boolean[]{true,true});

        evaluateQuery(v.query().labels(child.getName()).direction(OUT).has(time,Cmp.EQUAL,5),
                EDGE, 1, 1 , new boolean[]{true,true});
        evaluateQuery(v.query().labels(child.getName()).direction(BOTH).has(time,Cmp.EQUAL,5),
                EDGE, 1, 2 , new boolean[0]);
        evaluateQuery(v.query().labels(child.getName()).direction(OUT).interval(time,10,20).orderBy(weight,Order.DESC).limit(5),
                EDGE, 5, 1 , new boolean[]{true,false}, weight, Order.DESC);
        evaluateQuery(v.query().labels(child.getName()).direction(BOTH).interval(weight, 0.0, 1.0).orderBy(weight, Order.DESC),
                EDGE, 2*numV/10, 2 , new boolean[]{false,false}, weight, Order.DESC);
        evaluateQuery(v.query().labels(child.getName()).direction(OUT).interval(weight, 0.0, 1.0),
                EDGE, 2*numV/10, 1 , new boolean[]{false,true});
        evaluateQuery(v.query().labels(child.getName()).direction(BOTH),
                EDGE, numV, 1 , new boolean[]{true,true});
        vl = v.query().labels(child.getName()).direction(BOTH).vertexIds();
        assertEquals(numV,vl.size());
        assertTrue(vl.isSorted());
        assertTrue(isSortedByID(vl));
        evaluateQuery(v.query().labels(child.getName()).interval(weight, 0.0, 1.0).direction(OUT),
                EDGE, 2*numV/10, 1 , new boolean[]{false,true});
        vl = v.query().labels(child.getName()).interval(weight, 0.0, 1.0).direction(OUT).vertexIds();
        assertEquals(2*numV/10,vl.size());
        assertTrue(vl.isSorted());
        assertTrue(isSortedByID(vl));
        evaluateQuery(v.query().labels(child.getName()).interval(time, 70, 80).direction(OUT).orderBy(time,Order.ASC),
                EDGE, 10, 1 , new boolean[]{true,true},time,Order.ASC);
        vl = v.query().labels(child.getName()).interval(time, 70, 80).direction(OUT).orderBy(time,Order.ASC).vertexIds();
        assertEquals(10,vl.size());
        assertFalse(vl.isSorted());
        assertFalse(isSortedByID(vl));
        vl.sort();
        assertTrue(vl.isSorted());
        assertTrue(isSortedByID(vl));

        evaluateQuery(v.query().labels(connect.getName()).has(time,Cmp.EQUAL,5).interval(weight,0.0,5.0).direction(OUT),
                EDGE, 1, 1 , new boolean[]{true,true});
        evaluateQuery(v.query().labels(connect.getName()).has(time,Cmp.EQUAL,5).interval(weight,0.0,5.0).direction(BOTH),
                EDGE, 1, 2 , new boolean[0]);
        evaluateQuery(v.query().labels(connect.getName()).interval(time,10,20).interval(weight,0.0,5.0).direction(OUT),
                EDGE, 10, 1 , new boolean[]{false,true});
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).orderBy(weight,Order.ASC).limit(10),
                EDGE, 10, 1 , new boolean[]{true,true},weight,Order.ASC);
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).orderBy(weight,Order.DESC).limit(10),
                EDGE, 10, 1 , new boolean[]{true,true},weight,Order.DESC);
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).interval(weight,1.4,2.75).orderBy(weight,Order.DESC),
                EDGE, 3*numV/10, 1 , new boolean[]{true,true},weight,Order.DESC);
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).has(time,Cmp.EQUAL,22).orderBy(weight,Order.DESC),
                EDGE, 1, 1 , new boolean[]{true,true},weight,Order.DESC);
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).has(time,Cmp.EQUAL,22).orderBy(weight,Order.ASC),
                EDGE, 1, 1 , new boolean[]{true,false},weight,Order.ASC);
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).adjacent(u),
                EDGE, 1, 1 , new boolean[]{true,true});
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).has(weight, Cmp.EQUAL, 0.0).adjacent(u),
                EDGE, 1, 1 , new boolean[]{true,true});
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).interval(weight, 0.0, 1.0).adjacent(u),
                EDGE, 1, 1 , new boolean[]{false,true});
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).interval(time,50,100).adjacent(u),
                EDGE, 1, 1 , new boolean[]{false,true});

        evaluateQuery(v.query(),
                RELATION, numV*4, 1 , new boolean[]{true,true});
        evaluateQuery(v.query().direction(OUT),
                RELATION, numV*4, 1 , new boolean[]{false,true});

        //--------------

        clopen();

        weight = tx.getPropertyKey("weight");
        time = tx.getPropertyKey("time");

        name = tx.getPropertyKey("name");
        connect = tx.getEdgeLabel("connect");
        child = tx.getEdgeLabel("child");
        link = tx.getEdgeLabel("link");

        //######### QUERIES (copied from above) ##########
        v = (TitanVertex)tx.getVertex(v);
        u = (TitanVertex)tx.getVertex(u);

        evaluateQuery(v.query().keys(name.getName()).has(weight,Cmp.GREATER_THAN,3.6),
                PROPERTY, 2*numV/10, 1, new boolean[]{true,true});
        evaluateQuery(v.query().keys(name.getName()).has(weight,Cmp.LESS_THAN,0.9).orderBy(weight,Order.ASC),
                PROPERTY, 2*numV/10, 1, new boolean[]{true,true},weight,Order.ASC);
        evaluateQuery(v.query().keys(name.getName()).interval(weight, 1.1, 2.2).orderBy(weight,Order.DESC).limit(numV/10),
                PROPERTY, numV/10, 1, new boolean[]{true,false},weight,Order.DESC);
        evaluateQuery(v.query().keys(name.getName()).has(time,Cmp.EQUAL,5).orderBy(weight,Order.DESC),
                PROPERTY, 1, 1, new boolean[]{false,false},weight,Order.DESC);
        evaluateQuery(v.query().keys(name.getName()),
                PROPERTY, numV, 1, new boolean[]{true,true});

        evaluateQuery(v.query().labels(child.getName()).direction(OUT).has(time,Cmp.EQUAL,5),
                EDGE, 1, 1 , new boolean[]{true,true});
        evaluateQuery(v.query().labels(child.getName()).direction(BOTH).has(time,Cmp.EQUAL,5),
                EDGE, 1, 2 , new boolean[0]);
        evaluateQuery(v.query().labels(child.getName()).direction(OUT).interval(time,10,20).orderBy(weight,Order.DESC).limit(5),
                EDGE, 5, 1 , new boolean[]{true,false}, weight, Order.DESC);
        evaluateQuery(v.query().labels(child.getName()).direction(BOTH).interval(weight, 0.0, 1.0).orderBy(weight, Order.DESC),
                EDGE, 2*numV/10, 2 , new boolean[]{false,false}, weight, Order.DESC);
        evaluateQuery(v.query().labels(child.getName()).direction(OUT).interval(weight, 0.0, 1.0),
                EDGE, 2*numV/10, 1 , new boolean[]{false,true});
        evaluateQuery(v.query().labels(child.getName()).direction(BOTH),
                EDGE, numV, 1 , new boolean[]{true,true});
        vl = v.query().labels(child.getName()).direction(BOTH).vertexIds();
        assertEquals(numV,vl.size());
        assertTrue(vl.isSorted());
        assertTrue(isSortedByID(vl));
        evaluateQuery(v.query().labels(child.getName()).interval(weight, 0.0, 1.0).direction(OUT),
                EDGE, 2*numV/10, 1 , new boolean[]{false,true});
        vl = v.query().labels(child.getName()).interval(weight, 0.0, 1.0).direction(OUT).vertexIds();
        assertEquals(2*numV/10,vl.size());
        assertTrue(vl.isSorted());
        assertTrue(isSortedByID(vl));
        evaluateQuery(v.query().labels(child.getName()).interval(time, 70, 80).direction(OUT).orderBy(time,Order.ASC),
                EDGE, 10, 1 , new boolean[]{true,true},time,Order.ASC);
        vl = v.query().labels(child.getName()).interval(time, 70, 80).direction(OUT).orderBy(time,Order.ASC).vertexIds();
        assertEquals(10,vl.size());
        assertFalse(vl.isSorted());
        assertFalse(isSortedByID(vl));
        vl.sort();
        assertTrue(vl.isSorted());
        assertTrue(isSortedByID(vl));

        evaluateQuery(v.query().labels(connect.getName()).has(time,Cmp.EQUAL,5).interval(weight,0.0,5.0).direction(OUT),
                EDGE, 1, 1 , new boolean[]{true,true});
        evaluateQuery(v.query().labels(connect.getName()).has(time,Cmp.EQUAL,5).interval(weight,0.0,5.0).direction(BOTH),
                EDGE, 1, 2 , new boolean[0]);
        evaluateQuery(v.query().labels(connect.getName()).interval(time,10,20).interval(weight,0.0,5.0).direction(OUT),
                EDGE, 10, 1 , new boolean[]{false,true});
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).orderBy(weight,Order.ASC).limit(10),
                EDGE, 10, 1 , new boolean[]{true,true},weight,Order.ASC);
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).orderBy(weight,Order.DESC).limit(10),
                EDGE, 10, 1 , new boolean[]{true,true},weight,Order.DESC);
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).interval(weight,1.4,2.75).orderBy(weight,Order.DESC),
                EDGE, 3*numV/10, 1 , new boolean[]{true,true},weight,Order.DESC);
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).has(time,Cmp.EQUAL,22).orderBy(weight,Order.DESC),
                EDGE, 1, 1 , new boolean[]{true,true},weight,Order.DESC);
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).has(time,Cmp.EQUAL,22).orderBy(weight,Order.ASC),
                EDGE, 1, 1 , new boolean[]{true,false},weight,Order.ASC);
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).adjacent(u),
                EDGE, 1, 1 , new boolean[]{true,true});
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).has(weight, Cmp.EQUAL, 0.0).adjacent(u),
                EDGE, 1, 1 , new boolean[]{true,true});
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).interval(weight, 0.0, 1.0).adjacent(u),
                EDGE, 1, 1 , new boolean[]{false,true});
        evaluateQuery(v.query().labels(connect.getName()).direction(OUT).interval(time,50,100).adjacent(u),
                EDGE, 1, 1 , new boolean[]{false,true});

        evaluateQuery(v.query(),
                RELATION, numV*4, 1 , new boolean[]{true,true});
        evaluateQuery(v.query().direction(OUT),
                RELATION, numV*4, 1 , new boolean[]{false,true});

        //--------------

        //Update in transaction
        for (TitanProperty p : v.getProperties(name)) {
            if (p.<Long>getProperty(time)<(numV/2)) p.remove();
        }
        for (TitanEdge e : v.getEdges()) {
            if (e.<Long>getProperty(time)<(numV/2)) e.remove();
        }
        ns = new TitanVertex[numV*3/2];
        for (int i=numV;i<numV*3/2;i++) {
            double w = (i*0.5)%5;
            long t = i;
            TitanProperty p = v.addProperty(name,"v"+i);
            p.setProperty(weight,w);
            p.setProperty(time,t);

            ns[i]=tx.addVertex();
            for (EdgeLabel label : new EdgeLabel[]{connect,child,link}) {
                TitanEdge e = v.addEdge(label,ns[i]);
                e.setProperty(weight,w);
                e.setProperty(time,t);
            }
        }

        //######### UPDATED QUERIES ##########

        evaluateQuery(v.query().keys(name.getName()).has(weight,Cmp.GREATER_THAN,3.6),
                PROPERTY, 2*numV/10, 1, new boolean[]{true,true});
        evaluateQuery(v.query().keys(name.getName()).interval(time,numV/2-10,numV/2+10),
                PROPERTY, 10, 1, new boolean[]{false,true});
        evaluateQuery(v.query().keys(name.getName()).interval(time,numV/2-10,numV/2+10).orderBy(weight,Order.DESC),
                PROPERTY, 10, 1, new boolean[]{false,false},weight,Order.DESC);
        evaluateQuery(v.query().keys(name.getName()).interval(time,numV,numV+10).limit(5),
                PROPERTY, 5, 1, new boolean[]{false,true});

        evaluateQuery(v.query().labels(child.getName()).direction(OUT).has(time,Cmp.EQUAL,5),
                EDGE, 0, 1 , new boolean[]{true,true});
        evaluateQuery(v.query().labels(child.getName()).direction(OUT).has(time,Cmp.EQUAL,numV+5),
                EDGE, 1, 1 , new boolean[]{true,true});
        evaluateQuery(v.query().labels(child.getName()).direction(OUT).interval(time,10,20).orderBy(weight,Order.DESC).limit(5),
                EDGE, 0, 1 , new boolean[]{true,false}, weight, Order.DESC);
        evaluateQuery(v.query().labels(child.getName()).direction(OUT).interval(time,numV+10,numV+20).orderBy(weight,Order.DESC).limit(5),
                EDGE, 5, 1 , new boolean[]{true,false}, weight, Order.DESC);


        evaluateQuery(v.query(),
                RELATION, numV*4, 1 , new boolean[]{true,true});
        evaluateQuery(v.query().direction(OUT),
                RELATION, numV*4, 1 , new boolean[]{false,true});

        //######### END UPDATED QUERIES ##########

        newTx();

        weight = tx.getPropertyKey("weight");
        time = tx.getPropertyKey("time");

        name = tx.getPropertyKey("name");
        connect = tx.getEdgeLabel("connect");
        child = tx.getEdgeLabel("child");
        link = tx.getEdgeLabel("link");

        v = (TitanVertex)tx.getVertex(v);
        u = (TitanVertex)tx.getVertex(u);

        //######### UPDATED QUERIES (copied from above) ##########

        evaluateQuery(v.query().keys(name.getName()).has(weight,Cmp.GREATER_THAN,3.6),
                PROPERTY, 2*numV/10, 1, new boolean[]{true,true});
        evaluateQuery(v.query().keys(name.getName()).interval(time,numV/2-10,numV/2+10),
                PROPERTY, 10, 1, new boolean[]{false,true});
        evaluateQuery(v.query().keys(name.getName()).interval(time,numV/2-10,numV/2+10).orderBy(weight,Order.DESC),
                PROPERTY, 10, 1, new boolean[]{false,false},weight,Order.DESC);
        evaluateQuery(v.query().keys(name.getName()).interval(time,numV,numV+10).limit(5),
                PROPERTY, 5, 1, new boolean[]{false,true});

        evaluateQuery(v.query().labels(child.getName()).direction(OUT).has(time,Cmp.EQUAL,5),
                EDGE, 0, 1 , new boolean[]{true,true});
        evaluateQuery(v.query().labels(child.getName()).direction(OUT).has(time,Cmp.EQUAL,numV+5),
                EDGE, 1, 1 , new boolean[]{true,true});
        evaluateQuery(v.query().labels(child.getName()).direction(OUT).interval(time,10,20).orderBy(weight,Order.DESC).limit(5),
                EDGE, 0, 1 , new boolean[]{true,false}, weight, Order.DESC);
        evaluateQuery(v.query().labels(child.getName()).direction(OUT).interval(time,numV+10,numV+20).orderBy(weight,Order.DESC).limit(5),
                EDGE, 5, 1 , new boolean[]{true,false}, weight, Order.DESC);


        evaluateQuery(v.query(),
                RELATION, numV*4, 1 , new boolean[]{true,true});
        evaluateQuery(v.query().direction(OUT),
                RELATION, numV*4, 1 , new boolean[]{false,true});

        //######### END UPDATED QUERIES ##########

    }

    private boolean isSortedByID(VertexList vl) {
        for (int i=1;i<vl.size();i++) {
            if (vl.getID(i-1)>vl.getID(i)) return false;
        }
        return true;
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

    @Test
    public void testEdgesExceedCacheSize() {
        // Add a vertex with as many edges as the tx-cache-size. (20000 by default)
        int numEdges = graph.getConfiguration().getTxVertexCacheSize();
        Vertex parentVertex = graph.addVertex(null);
        for (int i = 0; i < numEdges; i++) {
            Vertex childVertex = graph.addVertex(null);
            parentVertex.addEdge("friend", childVertex);
        }
        graph.commit();
        assertEquals(numEdges, Iterables.size(parentVertex.getEdges(Direction.OUT)));

        // Remove an edge.
        Edge edge = parentVertex.getEdges(Direction.OUT).iterator().next();
        edge.remove();

        // Check that getEdges returns one fewer.
        assertEquals(numEdges - 1, Iterables.size(parentVertex.getEdges(Direction.OUT)));

        // Run the same check one more time.
        // This fails! (Expected: 19999. Actual: 20000.)
        assertEquals(numEdges - 1, Iterables.size(parentVertex.getEdges(Direction.OUT)));
    }


   /* ==================================================================================
                            LOGGING
     ==================================================================================*/


    @Test
    public void simpleLogTest() throws InterruptedException {
        simpleLogTest(false);
    }

    @Test
    public void simpleLogTestWithFailure() throws InterruptedException {
        simpleLogTest(true);
    }

    public void simpleLogTest(final boolean withLogFailure) throws InterruptedException {
        final String userlogName = "test";
        final Serializer serializer = graph.getDataSerializer();
        final EdgeSerializer edgeSerializer = graph.getEdgeSerializer();
        final TimestampProvider times = graph.getConfiguration().getTimestampProvider();
        final TimeUnit unit = times.getUnit();
        final long startTime = times.getTime().getTimestamp(TimeUnit.MILLISECONDS);
        clopen( option(SYSTEM_LOG_TRANSACTIONS), true,
                option(LOG_BACKEND, USER_LOG),(withLogFailure?TestMockLog.class.getName():LOG_BACKEND.getDefaultValue()),
                option(TestMockLog.LOG_MOCK_FAILADD, USER_LOG),withLogFailure,
                option(KCVSLog.LOG_READ_LAG_TIME, USER_LOG),new StandardDuration(50,TimeUnit.MILLISECONDS),
                option(LOG_READ_INTERVAL, USER_LOG),new StandardDuration(250,TimeUnit.MILLISECONDS),
                option(LOG_SEND_DELAY, USER_LOG),new StandardDuration(100,TimeUnit.MILLISECONDS),
                option(KCVSLog.LOG_READ_LAG_TIME,TRANSACTION_LOG),new StandardDuration(50,TimeUnit.MILLISECONDS),
                option(LOG_READ_INTERVAL,TRANSACTION_LOG),new StandardDuration(250,TimeUnit.MILLISECONDS),
                option(MAX_COMMIT_TIME),new StandardDuration(1,TimeUnit.SECONDS)
        );
        final String instanceid = graph.getConfiguration().getUniqueGraphId();

        PropertyKey weight = tx.makePropertyKey("weight").dataType(Decimal.class).cardinality(Cardinality.SINGLE).make();
        EdgeLabel knows = tx.makeEdgeLabel("knows").make();
        TitanVertex n1 = tx.addVertex();
        n1.addProperty(weight, 10.5);
        newTx();

        final Timepoint txTimes[] = new Timepoint[4];
        //Transaction with custom userlog name
        txTimes[0] = times.getTime();
        TitanTransaction tx2 = graph.buildTransaction().setLogIdentifier(userlogName).start();
        TitanVertex v1 = tx2.addVertex();
        v1.setProperty("weight", 111.1);
        v1.addEdge("knows", v1);
        tx2.commit();
        final long v1id = v1.getLongId();
        txTimes[1] = times.getTime();
        tx2 = graph.buildTransaction().setLogIdentifier(userlogName).start();
        TitanVertex v2 = tx2.addVertex();
        v2.setProperty("weight",222.2);
        v2.addEdge("knows",tx2.getVertex(v1id));
        tx2.commit();
        final long v2id = v2.getLongId();
        //Only read tx
        tx2 = graph.buildTransaction().setLogIdentifier(userlogName).start();
        v1 = tx2.getVertex(v1id);
        assertEquals(111.1,v1.<Decimal>getProperty("weight").doubleValue(),0.0);
        assertEquals(222.2,tx2.getVertex(v2).<Decimal>getProperty("weight").doubleValue(),0.0);
        tx2.commit();
        //Deleting transaction
        txTimes[2] = times.getTime();
        tx2 = graph.buildTransaction().setLogIdentifier(userlogName).start();
        v2 = tx2.getVertex(v2id);
        assertEquals(222.2,v2.<Decimal>getProperty("weight").doubleValue(),0.0);
        v2.remove();
        tx2.commit();
        //Edge modifying transaction
        txTimes[3] = times.getTime();
        tx2 = graph.buildTransaction().setLogIdentifier(userlogName).start();
        v1 = tx2.getVertex(v1id);
        assertEquals(111.1,v1.<Decimal>getProperty("weight").doubleValue(),0.0);
        Edge e = Iterables.getOnlyElement(v1.getEdges(Direction.OUT,"knows"));
        assertNull(e.getProperty("weight"));
        e.setProperty("weight",44.4);
        tx2.commit();
        close();
        final long endTime = times.getTime().getTimestamp(TimeUnit.MILLISECONDS);

        final ReadMarker startMarker = ReadMarker.fromTime(startTime, TimeUnit.MILLISECONDS);

        Log txlog = openTxLog();
        Log userLog = openUserLog(userlogName);
        final EnumMap<LogTxStatus,AtomicInteger> txMsgCounter = new EnumMap<LogTxStatus,AtomicInteger>(LogTxStatus.class);
        for (LogTxStatus status : LogTxStatus.values()) txMsgCounter.put(status,new AtomicInteger(0));
        final AtomicInteger userlogMeta = new AtomicInteger(0);
        txlog.registerReader(startMarker,new MessageReader() {
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
                LogTxStatus status = txEntry.getStatus();
                if (status==LogTxStatus.PRECOMMIT) {
                    assertTrue(txEntry.hasContent());
                    Object logid = txEntry.getMetadata().get(LogTxMeta.LOG_ID);
                    if (logid!=null) {
                        assertTrue(logid instanceof String);
                        assertEquals(userlogName,logid);
                        userlogMeta.incrementAndGet();
                    }
                } else if (withLogFailure) {
                    assertTrue(status.isPrimarySuccess() || status==LogTxStatus.SECONDARY_FAILURE);
                    if (status==LogTxStatus.SECONDARY_FAILURE) {
                        TransactionLogHeader.SecondaryFailures secFail = txEntry.getContentAsSecondaryFailures(serializer);
                        assertTrue(secFail.failedIndexes.isEmpty());
                        assertTrue(secFail.userLogFailure);
                    }
                } else {
                    assertFalse(txEntry.hasContent());
                    assertTrue(status.isSuccess());
                }
                txMsgCounter.get(txEntry.getStatus()).incrementAndGet();
            }
        });
        final EnumMap<Change,AtomicInteger> userChangeCounter = new EnumMap<Change,AtomicInteger>(Change.class);
        for (Change change : Change.values()) userChangeCounter.put(change,new AtomicInteger(0));
        final AtomicInteger userLogMsgCounter = new AtomicInteger(0);
        userLog.registerReader(startMarker, new MessageReader() {
            @Override
            public void read(Message message) {
                long msgTime = message.getTimestamp(TimeUnit.MILLISECONDS);
                assertTrue(msgTime >= startTime);
                assertNotNull(message.getSenderId());
                StaticBuffer content = message.getContent();
                assertTrue(content != null && content.length() > 0);
                TransactionLogHeader.Entry txentry = TransactionLogHeader.parse(content, serializer, times);

                long txTime = txentry.getHeader().getTimestamp(TimeUnit.MILLISECONDS);
                assertTrue(txTime <= msgTime);
                assertTrue(txTime >= startTime);
                long txid = txentry.getHeader().getId();
                assertTrue(txid > 0);
                for (TransactionLogHeader.Modification modification : txentry.getContentAsModifications(serializer)) {
                    assertTrue(modification.state == Change.ADDED || modification.state == Change.REMOVED);
                    userChangeCounter.get(modification.state).incrementAndGet();
                }
                userLogMsgCounter.incrementAndGet();
            }
        });
        Thread.sleep(4000);
        assertEquals(5,txMsgCounter.get(LogTxStatus.PRECOMMIT).get());
        assertEquals(4,txMsgCounter.get(LogTxStatus.PRIMARY_SUCCESS).get());
        assertEquals(1,txMsgCounter.get(LogTxStatus.COMPLETE_SUCCESS).get());
        assertEquals(4, userlogMeta.get());
        if (withLogFailure) assertEquals(4,txMsgCounter.get(LogTxStatus.SECONDARY_FAILURE).get());
        else assertEquals(4,txMsgCounter.get(LogTxStatus.SECONDARY_SUCCESS).get());
        //User-Log
        if (withLogFailure) {
            assertEquals(0, userLogMsgCounter.get());
        } else {
            assertEquals(4, userLogMsgCounter.get());
            assertEquals(7, userChangeCounter.get(Change.ADDED).get());
            assertEquals(4,userChangeCounter.get(Change.REMOVED).get());
        }

        clopen();
        /*
        Transaction Recovery
         */
        TransactionRecovery recovery = TitanFactory.startTransactionRecovery(graph,startTime,TimeUnit.MILLISECONDS);


        /*
        Use user log processing framework
         */
        final AtomicInteger userLogCount = new AtomicInteger(0);
        LogProcessorFramework userlogs = TitanFactory.openTransactionLog(graph);
        userlogs.addLogProcessor(userlogName).setStartTime(startTime, TimeUnit.MILLISECONDS).setRetryAttempts(1)
        .addProcessor(new ChangeProcessor() {
            @Override
            public void process(TitanTransaction tx, TransactionId txId, ChangeState changes) {
                assertEquals(instanceid,txId.getInstanceId());
                assertTrue(txId.getTransactionId()>0 && txId.getTransactionId()<100); //Just some reasonable upper bound
                final long txTime = txId.getTransactionTime().sinceEpoch(TimeUnit.MILLISECONDS);
                assertTrue(String.format("tx timestamp %s not between start %s and end time %s",
                        txTime,startTime,endTime),
                        txTime>=startTime && txTime<=endTime); //Times should be within a second

                assertTrue(tx.containsRelationType("knows"));
                assertTrue(tx.containsRelationType("weight"));
                EdgeLabel knows = tx.getEdgeLabel("knows");
                PropertyKey weight = tx.getPropertyKey("weight");

                long txTimeMicro = txId.getTransactionTime().sinceEpoch(TimeUnit.MICROSECONDS);

                int txNo;
                if (txTimeMicro<txTimes[1].getTimestamp(TimeUnit.MICROSECONDS)) {
                    txNo=1;
                    //v1 addition transaction
                    assertEquals(1, Iterables.size(changes.getVertices(Change.ADDED)));
                    assertEquals(0, Iterables.size(changes.getVertices(Change.REMOVED)));
                    assertEquals(1,Iterables.size(changes.getVertices(Change.ANY)));
                    assertEquals(2,Iterables.size(changes.getRelations(Change.ADDED)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.ADDED, knows)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.ADDED, weight)));
                    assertEquals(2,Iterables.size(changes.getRelations(Change.ANY)));
                    assertEquals(0,Iterables.size(changes.getRelations(Change.REMOVED)));

                    TitanVertex v = Iterables.getOnlyElement(changes.getVertices(Change.ADDED));
                    assertEquals(v1id,v.getLongId());
                    TitanProperty p = Iterables.getOnlyElement(changes.getProperties(v,Change.ADDED,"weight"));
                    assertEquals(111.1,p.<Decimal>getValue().doubleValue(),0.0001);
                    assertEquals(1,Iterables.size(changes.getEdges(v, Change.ADDED, OUT)));
                    assertEquals(1,Iterables.size(changes.getEdges(v, Change.ADDED, BOTH)));
                } else if (txTimeMicro<txTimes[2].getTimestamp(TimeUnit.MICROSECONDS)) {
                    txNo=2;
                    //v2 addition transaction
                    assertEquals(1, Iterables.size(changes.getVertices(Change.ADDED)));
                    assertEquals(0, Iterables.size(changes.getVertices(Change.REMOVED)));
                    assertEquals(2,Iterables.size(changes.getVertices(Change.ANY)));
                    assertEquals(2,Iterables.size(changes.getRelations(Change.ADDED)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.ADDED, knows)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.ADDED, weight)));
                    assertEquals(2,Iterables.size(changes.getRelations(Change.ANY)));
                    assertEquals(0,Iterables.size(changes.getRelations(Change.REMOVED)));

                    TitanVertex v = Iterables.getOnlyElement(changes.getVertices(Change.ADDED));
                    assertEquals(v2id,v.getLongId());
                    TitanProperty p = Iterables.getOnlyElement(changes.getProperties(v,Change.ADDED,"weight"));
                    assertEquals(222.2,p.<Decimal>getValue().doubleValue(),0.0001);
                    assertEquals(1,Iterables.size(changes.getEdges(v, Change.ADDED, OUT)));
                    assertEquals(1,Iterables.size(changes.getEdges(v, Change.ADDED, BOTH)));
                } else if (txTimeMicro<txTimes[3].getTimestamp(TimeUnit.MICROSECONDS)) {
                    txNo=3;
                    //v2 deletion transaction
                    assertEquals(0, Iterables.size(changes.getVertices(Change.ADDED)));
                    assertEquals(1, Iterables.size(changes.getVertices(Change.REMOVED)));
                    assertEquals(2,Iterables.size(changes.getVertices(Change.ANY)));
                    assertEquals(0,Iterables.size(changes.getRelations(Change.ADDED)));
                    assertEquals(2,Iterables.size(changes.getRelations(Change.REMOVED)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.REMOVED, knows)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.REMOVED, weight)));
                    assertEquals(2,Iterables.size(changes.getRelations(Change.ANY)));

                    TitanVertex v = Iterables.getOnlyElement(changes.getVertices(Change.REMOVED));
                    assertEquals(v2id,v.getLongId());
                    TitanProperty p = Iterables.getOnlyElement(changes.getProperties(v,Change.REMOVED,"weight"));
                    assertEquals(222.2,p.<Decimal>getValue().doubleValue(),0.0001);
                    assertEquals(1,Iterables.size(changes.getEdges(v, Change.REMOVED, OUT)));
                    assertEquals(0,Iterables.size(changes.getEdges(v, Change.ADDED, BOTH)));
                } else {
                    txNo=4;
                    //v1 edge modification
                    assertEquals(0, Iterables.size(changes.getVertices(Change.ADDED)));
                    assertEquals(0, Iterables.size(changes.getVertices(Change.REMOVED)));
                    assertEquals(1,Iterables.size(changes.getVertices(Change.ANY)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.ADDED)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.REMOVED)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.REMOVED, knows)));
                    assertEquals(2,Iterables.size(changes.getRelations(Change.ANY)));

                    TitanVertex v = Iterables.getOnlyElement(changes.getVertices(Change.ANY));
                    assertEquals(v1id,v.getLongId());
                    TitanEdge e = Iterables.getOnlyElement(changes.getEdges(v,Change.REMOVED,Direction.OUT,"knows"));
                    assertNull(e.getProperty("weight"));
                    assertEquals(v,e.getVertex(Direction.IN));
                    e = Iterables.getOnlyElement(changes.getEdges(v,Change.ADDED,Direction.OUT,"knows"));
                    assertEquals(44.4,e.<Decimal>getProperty("weight").doubleValue(),0.0);
                    assertEquals(v,e.getVertex(Direction.IN));
                }

                //See only current state of graph in transaction
                TitanVertex v1 = tx.getVertex(v1id);
                assertNotNull(v1);
                assertTrue(v1.isLoaded());
                TitanVertex v2 = tx.getVertex(v2id);
                if (txNo!=2) {
                    //In the transaction that adds v2, v2 will be considered "loaded"
                    assertTrue(txNo+ " - " + v2,v2==null || v2.isRemoved());
                }
                assertEquals(111.1,v1.<Decimal>getProperty(weight).doubleValue(),0.0);
                assertEquals(1,Iterables.size(v1.getEdges(Direction.OUT)));

                userLogCount.incrementAndGet();
            }
        }).build();

        //wait
        Thread.sleep(22000L);

        recovery.shutdown();
        long[] recoveryStats = ((StandardTransactionLogProcessor)recovery).getStatistics();
        if (withLogFailure) {
            assertEquals(1,recoveryStats[0]);
            assertEquals(4,recoveryStats[1]);
        } else {
            assertEquals(5,recoveryStats[0]);
            assertEquals(0,recoveryStats[1]);

        }

        userlogs.removeLogProcessor(userlogName);
        userlogs.shutdown();
        assertEquals(4, userLogCount.get());
    }


   /* ==================================================================================
                            GLOBAL GRAPH QUERIES
     ==================================================================================*/


    /**
     * Tests index defintions and their correct application for internal indexes only
     */
    @Test
    public void testGlobalGraphIndexingAndQueriesForInternalIndexes() {
        PropertyKey weight = makeKey("weight",Decimal.class);
        PropertyKey time = makeKey("time",Long.class);
        PropertyKey text = makeKey("text",String.class);

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.LIST).make();
        EdgeLabel connect = mgmt.makeEdgeLabel("connect").signature(weight).make();
        EdgeLabel related = mgmt.makeEdgeLabel("related").signature(time).make();

        VertexLabel person = mgmt.makeVertexLabel("person").make();
        VertexLabel org = mgmt.makeVertexLabel("organization").make();

        TitanGraphIndex edge1 = mgmt.buildIndex("edge1",Edge.class).addKey(time).addKey(weight).buildCompositeIndex();
        TitanGraphIndex edge2 = mgmt.buildIndex("edge2",Edge.class).indexOnly(connect).addKey(text).buildCompositeIndex();

        TitanGraphIndex prop1 = mgmt.buildIndex("prop1",TitanProperty.class).addKey(time).buildCompositeIndex();
        TitanGraphIndex prop2 = mgmt.buildIndex("prop2",TitanProperty.class).addKey(weight).addKey(text).buildCompositeIndex();

        TitanGraphIndex vertex1 = mgmt.buildIndex("vertex1",Vertex.class).addKey(time).indexOnly(person).unique().buildCompositeIndex();
        TitanGraphIndex vertex12 = mgmt.buildIndex("vertex12",Vertex.class).addKey(text).indexOnly(person).buildCompositeIndex();
        TitanGraphIndex vertex2 = mgmt.buildIndex("vertex2",Vertex.class).addKey(time).addKey(name).indexOnly(org).buildCompositeIndex();
        TitanGraphIndex vertex3 = mgmt.buildIndex("vertex3",Vertex.class).addKey(name).buildCompositeIndex();


        // ########### INSPECTION & FAILURE ##############
        assertTrue(mgmt.containsRelationType("name"));
        assertTrue(mgmt.containsGraphIndex("prop1"));
        assertFalse(mgmt.containsGraphIndex("prop3"));
        assertEquals(2,Iterables.size(mgmt.getGraphIndexes(Edge.class)));
        assertEquals(2,Iterables.size(mgmt.getGraphIndexes(TitanProperty.class)));
        assertEquals(4,Iterables.size(mgmt.getGraphIndexes(Vertex.class)));
        assertNull(mgmt.getGraphIndex("balblub"));

        edge1 = mgmt.getGraphIndex("edge1");
        edge2 = mgmt.getGraphIndex("edge2");
        prop1 = mgmt.getGraphIndex("prop1");
        prop2 = mgmt.getGraphIndex("prop2");
        vertex1 = mgmt.getGraphIndex("vertex1");
        vertex12 = mgmt.getGraphIndex("vertex12");
        vertex2 = mgmt.getGraphIndex("vertex2");
        vertex3 = mgmt.getGraphIndex("vertex3");

        assertTrue(vertex1.isUnique());
        assertFalse(edge2.isUnique());
        assertEquals("prop1",prop1.getName());
        assertTrue(Vertex.class.isAssignableFrom(vertex3.getIndexedElement()));
        assertTrue(TitanProperty.class.isAssignableFrom(prop1.getIndexedElement()));
        assertTrue(Edge.class.isAssignableFrom(edge2.getIndexedElement()));
        assertEquals(2,vertex2.getFieldKeys().length);
        assertEquals(1,vertex1.getFieldKeys().length);

        try {
            //Parameters not supported
            mgmt.buildIndex("blablub",Vertex.class).addKey(text, Mapping.TEXT.getParameter()).buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //Name already in use
            mgmt.buildIndex("edge1",Vertex.class).addKey(weight).buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //ImplicitKeys not allowed
            mgmt.buildIndex("jupdup",Vertex.class).addKey(ImplicitKey.ID).buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //Unique is only allowed for vertex
            mgmt.buildIndex("edgexyz",Edge.class).addKey(time).unique().buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {}

        // ########### END INSPECTION & FAILURE ##############
        finishSchema();
        clopen();

        text = mgmt.getPropertyKey("text");
        time = mgmt.getPropertyKey("time");
        weight = mgmt.getPropertyKey("weight");

        // ########### INSPECTION & FAILURE (copied from above) ##############
        assertTrue(mgmt.containsRelationType("name"));
        assertTrue(mgmt.containsGraphIndex("prop1"));
        assertFalse(mgmt.containsGraphIndex("prop3"));
        assertEquals(2,Iterables.size(mgmt.getGraphIndexes(Edge.class)));
        assertEquals(2,Iterables.size(mgmt.getGraphIndexes(TitanProperty.class)));
        assertEquals(4,Iterables.size(mgmt.getGraphIndexes(Vertex.class)));
        assertNull(mgmt.getGraphIndex("balblub"));

        edge1 = mgmt.getGraphIndex("edge1");
        edge2 = mgmt.getGraphIndex("edge2");
        prop1 = mgmt.getGraphIndex("prop1");
        prop2 = mgmt.getGraphIndex("prop2");
        vertex1 = mgmt.getGraphIndex("vertex1");
        vertex12 = mgmt.getGraphIndex("vertex12");
        vertex2 = mgmt.getGraphIndex("vertex2");
        vertex3 = mgmt.getGraphIndex("vertex3");

        assertTrue(vertex1.isUnique());
        assertFalse(edge2.isUnique());
        assertEquals("prop1",prop1.getName());
        assertTrue(Vertex.class.isAssignableFrom(vertex3.getIndexedElement()));
        assertTrue(TitanProperty.class.isAssignableFrom(prop1.getIndexedElement()));
        assertTrue(Edge.class.isAssignableFrom(edge2.getIndexedElement()));
        assertEquals(2,vertex2.getFieldKeys().length);
        assertEquals(1,vertex1.getFieldKeys().length);

        try {
            //Parameters not supported
            mgmt.buildIndex("blablub",Vertex.class).addKey(text, Mapping.TEXT.getParameter()).buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //Name already in use
            mgmt.buildIndex("edge1",Vertex.class).addKey(weight).buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //ImplicitKeys not allowed
            mgmt.buildIndex("jupdup",Vertex.class).addKey(ImplicitKey.ID).buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //Unique is only allowed for vertex
            mgmt.buildIndex("edgexyz",Edge.class).addKey(time).unique().buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {}

        // ########### END INSPECTION & FAILURE ##############

        weight = tx.getPropertyKey("weight");
        time = tx.getPropertyKey("time");
        text = tx.getPropertyKey("text");

        name = tx.getPropertyKey("name");
        connect = tx.getEdgeLabel("connect");
        related = tx.getEdgeLabel("related");

        person = tx.getVertexLabel("person");
        org = tx.getVertexLabel("organization");

        final int numV = 100;
        final boolean sorted = true;
        TitanVertex ns[] = new TitanVertex[numV];
        String[] strs = {"aaa","bbb","ccc","ddd"};

        for (int i=0;i<numV;i++) {
            ns[i]=tx.addVertex(i%2==0?person:org);
            TitanProperty p1 = ns[i].addProperty(name,"v"+i);
            TitanProperty p2 = ns[i].addProperty(name,"u"+(i%5));

            double w = (i*0.5)%5;
            long t = i;
            String txt = strs[i%(strs.length)];

            ns[i].setProperty(weight,w);
            ns[i].setProperty(time,t);
            ns[i].setProperty(text,txt);

            for (TitanProperty p : new TitanProperty[]{p1,p2}) {
                p.setProperty(weight,w);
                p.setProperty(time,t);
                p.setProperty(text,txt);
            }

            TitanVertex u = ns[(i>0?i-1:i)]; //previous or self-loop
            for (EdgeLabel label : new EdgeLabel[]{connect,related}) {
                TitanEdge e = ns[i].addEdge(label,u);
                e.setProperty(weight,(w++)%5);
                e.setProperty(time,t);
                e.setProperty(text,txt);
            }
        }


        //########## QUERIES ################
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,10).has(weight,Cmp.EQUAL,0),
                ElementCategory.EDGE,1,new boolean[]{true,sorted},edge1.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,10).has(weight,Cmp.EQUAL,0).has(text,Cmp.EQUAL,strs[10%strs.length]),
                ElementCategory.EDGE,1,new boolean[]{false,sorted},edge1.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,10).has(weight,Cmp.EQUAL,1),
                ElementCategory.EDGE,1,new boolean[]{true,sorted},edge1.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,20).has(weight,Cmp.EQUAL,0),
                ElementCategory.EDGE,1,new boolean[]{true,sorted},edge1.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,20).has(weight,Cmp.EQUAL,3),
                ElementCategory.EDGE,0,new boolean[]{true,sorted},edge1.getName());
        evaluateQuery(tx.query().has(text,Cmp.EQUAL,strs[0]).has("label",connect.getName()),
                ElementCategory.EDGE,numV/strs.length,new boolean[]{true,sorted},edge2.getName());
        evaluateQuery(tx.query().has(text,Cmp.EQUAL,strs[0]).has("label",connect.getName()).limit(10),
                ElementCategory.EDGE,10,new boolean[]{true,sorted},edge2.getName());
        evaluateQuery(tx.query().has(text,Cmp.EQUAL,strs[0]),
                ElementCategory.EDGE,numV/strs.length*2,new boolean[]{false,sorted});
        evaluateQuery(tx.query().has(weight,Cmp.EQUAL,1.5),
                ElementCategory.EDGE,numV/10*2,new boolean[]{false,sorted});

        evaluateQuery(tx.query().has(time,Cmp.EQUAL,50),
                ElementCategory.PROPERTY,2,new boolean[]{true,sorted},prop1.getName());
        evaluateQuery(tx.query().has(weight,Cmp.EQUAL,0.0).has(text,Cmp.EQUAL,strs[0]),
                ElementCategory.PROPERTY,2*numV/(4*5),new boolean[]{true,sorted},prop2.getName());
        evaluateQuery(tx.query().has(weight,Cmp.EQUAL,0.0).has(text,Cmp.EQUAL,strs[0]).has(time,Cmp.EQUAL,0),
                ElementCategory.PROPERTY,2,new boolean[]{true,sorted},prop2.getName(),prop1.getName());
        evaluateQuery(tx.query().has(weight,Cmp.EQUAL,1.5),
                ElementCategory.PROPERTY,2*numV/10,new boolean[]{false,sorted});

        evaluateQuery(tx.query().has(time,Cmp.EQUAL,50).has("label",person.getName()),
                ElementCategory.VERTEX,1,new boolean[]{true,sorted},vertex1.getName());
        evaluateQuery(tx.query().has(text,Cmp.EQUAL,strs[2]).has("label",person.getName()),
                ElementCategory.VERTEX,numV/strs.length,new boolean[]{true,sorted},vertex12.getName());
        evaluateQuery(tx.query().has(text,Cmp.EQUAL,strs[3]).has("label",person.getName()),
                ElementCategory.VERTEX,0,new boolean[]{true,sorted},vertex12.getName());
        evaluateQuery(tx.query().has(text,Cmp.EQUAL,strs[2]).has("label",person.getName()).has(time,Cmp.EQUAL,2),
                ElementCategory.VERTEX,1,new boolean[]{true,sorted},vertex12.getName(),vertex1.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,51).has(name,Cmp.EQUAL,"v51").has("label",org.getName()),
                ElementCategory.VERTEX,1,new boolean[]{true,sorted},vertex2.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,51).has(name,Cmp.EQUAL,"u1").has("label",org.getName()),
                ElementCategory.VERTEX,1,new boolean[]{true,sorted},vertex2.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,51).has("label",org.getName()),
                ElementCategory.VERTEX,1,new boolean[]{false,sorted});
        evaluateQuery(tx.query().has(name,Cmp.EQUAL,"u1"),
                ElementCategory.VERTEX,numV/5,new boolean[]{true,sorted},vertex3.getName());
        evaluateQuery(tx.query().has(name,Cmp.EQUAL,"v1"),
                ElementCategory.VERTEX,1,new boolean[]{true,sorted},vertex3.getName());
        evaluateQuery(tx.query().has(name,Cmp.EQUAL,"v1").has("label",org.getName()),
                ElementCategory.VERTEX,1,new boolean[]{false,sorted},vertex3.getName());

        clopen();
        weight = tx.getPropertyKey("weight");
        time = tx.getPropertyKey("time");
        text = tx.getPropertyKey("text");

        name = tx.getPropertyKey("name");
        connect = tx.getEdgeLabel("connect");
        related = tx.getEdgeLabel("related");

        person = tx.getVertexLabel("person");
        org = tx.getVertexLabel("organization");
        //########## QUERIES (copied from above) ################
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,10).has(weight,Cmp.EQUAL,0),
                ElementCategory.EDGE,1,new boolean[]{true,sorted},edge1.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,10).has(weight,Cmp.EQUAL,0).has(text,Cmp.EQUAL,strs[10%strs.length]),
                ElementCategory.EDGE,1,new boolean[]{false,sorted},edge1.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,10).has(weight,Cmp.EQUAL,1),
                ElementCategory.EDGE,1,new boolean[]{true,sorted},edge1.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,20).has(weight,Cmp.EQUAL,0),
                ElementCategory.EDGE,1,new boolean[]{true,sorted},edge1.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,20).has(weight,Cmp.EQUAL,3),
                ElementCategory.EDGE,0,new boolean[]{true,sorted},edge1.getName());
        evaluateQuery(tx.query().has(text,Cmp.EQUAL,strs[0]).has("label",connect.getName()),
                ElementCategory.EDGE,numV/strs.length,new boolean[]{true,sorted},edge2.getName());
        evaluateQuery(tx.query().has(text,Cmp.EQUAL,strs[0]).has("label",connect.getName()).limit(10),
                ElementCategory.EDGE,10,new boolean[]{true,sorted},edge2.getName());
        evaluateQuery(tx.query().has(text,Cmp.EQUAL,strs[0]),
                ElementCategory.EDGE,numV/strs.length*2,new boolean[]{false,sorted});
        evaluateQuery(tx.query().has(weight,Cmp.EQUAL,1.5),
                ElementCategory.EDGE,numV/10*2,new boolean[]{false,sorted});

        evaluateQuery(tx.query().has(time,Cmp.EQUAL,50),
                ElementCategory.PROPERTY,2,new boolean[]{true,sorted},prop1.getName());
        evaluateQuery(tx.query().has(weight,Cmp.EQUAL,0.0).has(text,Cmp.EQUAL,strs[0]),
                ElementCategory.PROPERTY,2*numV/(4*5),new boolean[]{true,sorted},prop2.getName());
        evaluateQuery(tx.query().has(weight,Cmp.EQUAL,0.0).has(text,Cmp.EQUAL,strs[0]).has(time,Cmp.EQUAL,0),
                ElementCategory.PROPERTY,2,new boolean[]{true,sorted},prop2.getName(),prop1.getName());
        evaluateQuery(tx.query().has(weight,Cmp.EQUAL,1.5),
                ElementCategory.PROPERTY,2*numV/10,new boolean[]{false,sorted});

        evaluateQuery(tx.query().has(time,Cmp.EQUAL,50).has("label",person.getName()),
                ElementCategory.VERTEX,1,new boolean[]{true,sorted},vertex1.getName());
        evaluateQuery(tx.query().has(text,Cmp.EQUAL,strs[2]).has("label",person.getName()),
                ElementCategory.VERTEX,numV/strs.length,new boolean[]{true,sorted},vertex12.getName());
        evaluateQuery(tx.query().has(text,Cmp.EQUAL,strs[3]).has("label",person.getName()),
                ElementCategory.VERTEX,0,new boolean[]{true,sorted},vertex12.getName());
        evaluateQuery(tx.query().has(text,Cmp.EQUAL,strs[2]).has("label",person.getName()).has(time,Cmp.EQUAL,2),
                ElementCategory.VERTEX,1,new boolean[]{true,sorted},vertex12.getName(),vertex1.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,51).has(name,Cmp.EQUAL,"v51").has("label",org.getName()),
                ElementCategory.VERTEX,1,new boolean[]{true,sorted},vertex2.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,51).has(name,Cmp.EQUAL,"u1").has("label",org.getName()),
                ElementCategory.VERTEX,1,new boolean[]{true,sorted},vertex2.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,51).has("label",org.getName()),
                ElementCategory.VERTEX,1,new boolean[]{false,sorted});
        evaluateQuery(tx.query().has(name,Cmp.EQUAL,"u1"),
                ElementCategory.VERTEX,numV/5,new boolean[]{true,sorted},vertex3.getName());
        evaluateQuery(tx.query().has(name,Cmp.EQUAL,"v1"),
                ElementCategory.VERTEX,1,new boolean[]{true,sorted},vertex3.getName());
        evaluateQuery(tx.query().has(name,Cmp.EQUAL,"v1").has("label",org.getName()),
                ElementCategory.VERTEX,1,new boolean[]{false,sorted},vertex3.getName());

        //Update in transaction
        for (int i=0;i<numV/2;i++) {
            TitanVertex v = tx.getVertex(ns[i].getLongId());
            v.remove();
        }
        ns = new TitanVertex[numV*3/2];
        for (int i=numV;i<numV*3/2;i++) {
            ns[i]=tx.addVertex(i%2==0?person:org);
            TitanProperty p1 = ns[i].addProperty(name,"v"+i);
            TitanProperty p2 = ns[i].addProperty(name,"u"+(i%5));

            double w = (i*0.5)%5;
            long t = i;
            String txt = strs[i%(strs.length)];

            ns[i].setProperty(weight,w);
            ns[i].setProperty(time,t);
            ns[i].setProperty(text,txt);

            for (TitanProperty p : new TitanProperty[]{p1,p2}) {
                p.setProperty(weight,w);
                p.setProperty(time,t);
                p.setProperty(text,txt);
            }

            TitanVertex u = ns[(i>numV?i-1:i)]; //previous or self-loop
            for (EdgeLabel label : new EdgeLabel[]{connect,related}) {
                TitanEdge e = ns[i].addEdge(label,u);
                e.setProperty(weight,(w++)%5);
                e.setProperty(time,t);
                e.setProperty(text,txt);
            }
        }


        //######### UPDATED QUERIES ##########

        evaluateQuery(tx.query().has(time,Cmp.EQUAL,10).has(weight,Cmp.EQUAL,0),
                ElementCategory.EDGE,0,new boolean[]{true,sorted},edge1.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,numV+10).has(weight,Cmp.EQUAL,0),
                ElementCategory.EDGE,1,new boolean[]{true,sorted},edge1.getName());
        evaluateQuery(tx.query().has(text,Cmp.EQUAL,strs[0]).has("label",connect.getName()).limit(10),
                ElementCategory.EDGE,10,new boolean[]{true,sorted},edge2.getName());
        evaluateQuery(tx.query().has(weight,Cmp.EQUAL,1.5),
                ElementCategory.EDGE,numV/10*2,new boolean[]{false,sorted});

        evaluateQuery(tx.query().has(time,Cmp.EQUAL,20),
                ElementCategory.PROPERTY,0,new boolean[]{true,sorted},prop1.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,numV+20),
                ElementCategory.PROPERTY,2,new boolean[]{true,sorted},prop1.getName());

        evaluateQuery(tx.query().has(time,Cmp.EQUAL,30).has("label",person.getName()),
                ElementCategory.VERTEX,0,new boolean[]{true,sorted},vertex1.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,numV+30).has("label",person.getName()),
                ElementCategory.VERTEX,1,new boolean[]{true,sorted},vertex1.getName());
        evaluateQuery(tx.query().has(name,Cmp.EQUAL,"u1"),
                ElementCategory.VERTEX,numV/5,new boolean[]{true,sorted},vertex3.getName());


        //######### END UPDATED QUERIES ##########

        newTx();
        weight = tx.getPropertyKey("weight");
        time = tx.getPropertyKey("time");
        text = tx.getPropertyKey("text");

        name = tx.getPropertyKey("name");
        connect = tx.getEdgeLabel("connect");
        related = tx.getEdgeLabel("related");

        person = tx.getVertexLabel("person");
        org = tx.getVertexLabel("organization");

        //######### UPDATED QUERIES (copied from above) ##########
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,10).has(weight,Cmp.EQUAL,0),
                ElementCategory.EDGE,0,new boolean[]{true,sorted},edge1.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,numV+10).has(weight,Cmp.EQUAL,0),
                ElementCategory.EDGE,1,new boolean[]{true,sorted},edge1.getName());
        evaluateQuery(tx.query().has(text,Cmp.EQUAL,strs[0]).has("label",connect.getName()).limit(10),
                ElementCategory.EDGE,10,new boolean[]{true,sorted},edge2.getName());
        evaluateQuery(tx.query().has(weight,Cmp.EQUAL,1.5),
                ElementCategory.EDGE,numV/10*2,new boolean[]{false,sorted});

        evaluateQuery(tx.query().has(time,Cmp.EQUAL,20),
                ElementCategory.PROPERTY,0,new boolean[]{true,sorted},prop1.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,numV+20),
                ElementCategory.PROPERTY,2,new boolean[]{true,sorted},prop1.getName());

        evaluateQuery(tx.query().has(time,Cmp.EQUAL,30).has("label",person.getName()),
                ElementCategory.VERTEX,0,new boolean[]{true,sorted},vertex1.getName());
        evaluateQuery(tx.query().has(time,Cmp.EQUAL,numV+30).has("label",person.getName()),
                ElementCategory.VERTEX,1,new boolean[]{true,sorted},vertex1.getName());
        evaluateQuery(tx.query().has(name,Cmp.EQUAL,"u1"),
                ElementCategory.VERTEX,numV/5,new boolean[]{true,sorted},vertex3.getName());

        //*** INIVIDUAL USE CASE TESTS ******

        newTx();
        //Check that index enforces uniqueness on vertices with the right label...
        try {
            TitanVertex v1 = tx.addVertex(tx.getVertexLabel(person.getName()));
            v1.setProperty(time.getName(),numV/2);
            tx.commit();
            fail();
        } catch (Exception e) {
        } finally {
            if (tx.isOpen()) tx.rollback();
        }
        newTx();
        //...but not if we use a different one
        TitanVertex v1 = tx.addVertex(tx.getVertexLabel(org.getName()));
        v1.setProperty(time.getName(),numV/2);
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

    @Test
    public void testForceIndexUsage() {
        PropertyKey age = makeKey("age",Integer.class);
        PropertyKey time = makeKey("time",Long.class);
        mgmt.buildIndex("time",Vertex.class).addKey(time).buildCompositeIndex();
        finishSchema();

        for (int i=1;i<=10;i++) {
            Vertex v = tx.addVertex();
            v.setProperty("time",i);
            v.setProperty("age",i);
        }

        //Graph query with and with-out index support
        assertEquals(1,Iterables.size(tx.query().has("time",5).vertices()));
        assertEquals(1,Iterables.size(tx.query().has("age",6).vertices()));

        clopen(option(FORCE_INDEX_USAGE), true);
        //Query with-out index support should now throw exception
        assertEquals(1,Iterables.size(tx.query().has("time",5).vertices()));
        try {
            assertEquals(1,Iterables.size(tx.query().has("age",6).vertices()));
            fail();
        } catch (Exception e) {}
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

        alice = tx.getVertex(alice.getLongId());
        bob = tx.getVertex(bob.getLongId());

        assertEquals(sharedValue, alice.getProperty(propName));
        assertEquals(null, bob.getProperty(propName));

        alice.removeProperty(propName);
        bob.addProperty(propName, sharedValue);

        clopen();
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
        mgmt.buildIndex("vuid",Vertex.class).unique().addKey(id).buildCompositeIndex();
        mgmt.buildIndex("euid",Edge.class).addKey(id).buildCompositeIndex();

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.buildIndex("vname",Vertex.class).addKey(name).buildCompositeIndex();
        mgmt.buildIndex("ename",Edge.class).addKey(name).buildCompositeIndex();
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
        PropertyKey kid = mgmt.makePropertyKey("kid").dataType(Long.class).make();
        mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.makeEdgeLabel("knows").signature(kid).make();
        finishSchema();

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


    //................................................



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

        n3 = graph.getVertex(n3.getLongId());
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

        n3 = graph.getVertex(n3.getLongId());
        assertEquals(353, n3.getProperty("uid"));

        e = (TitanEdge)Iterables.getOnlyElement(n3.getEdges(Direction.OUT,"knows"));
        assertEquals(222,e.getProperty(id));
    }

   /* ==================================================================================
                            TIME TO LIVE
     ==================================================================================*/

    @Test
    public void testEdgeTTLTiming() throws Exception {
        if (!features.hasCellTTL()) {
            return;
        }

        EdgeLabel label1 = mgmt.makeEdgeLabel("likes").make();
        int ttl1 = 1;
        int ttl2 = 4;
        mgmt.setTTL(label1, ttl1, TimeUnit.SECONDS);
        EdgeLabel label2 = mgmt.makeEdgeLabel("dislikes").make();
        mgmt.setTTL(label2, ttl2, TimeUnit.SECONDS);
        EdgeLabel label3 = mgmt.makeEdgeLabel("indifferentTo").make();
        assertEquals(ttl1, mgmt.getTTL(label1).getLength(TimeUnit.SECONDS));
        assertEquals(ttl2, mgmt.getTTL(label2).getLength(TimeUnit.SECONDS));
        assertEquals(0, mgmt.getTTL(label3).getLength(TimeUnit.SECONDS));
        mgmt.commit();

        Vertex v1 = graph.addVertex(null), v2 = graph.addVertex(null), v3 = graph.addVertex(null);

        graph.addEdge(null, v1, v2, "likes");
        graph.addEdge(null, v2, v1, "dislikes");
        graph.addEdge(null, v3, v1, "indifferentTo");

        // initial, pre-commit state of the edges.  They are not yet subject to TTL
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v2.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v3.getVertices(Direction.OUT).iterator().hasNext());

        long commitTime = System.currentTimeMillis();
        graph.commit();

        // edges are now subject to TTL, although we must commit() or rollback() to see it
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v2.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v3.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(commitTime + (ttl1 * 1000L + 200) - System.currentTimeMillis());
        graph.rollback();

        // e1 has dropped out
        assertFalse(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v2.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v3.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(commitTime + (ttl2 * 1000L + 500) - System.currentTimeMillis());
        graph.rollback();

        // both e1 and e2 have dropped out.  e3 has no TTL, and so remains
        assertFalse(v1.getVertices(Direction.OUT).iterator().hasNext());
        assertFalse(v2.getVertices(Direction.OUT).iterator().hasNext());
        assertTrue(v3.getVertices(Direction.OUT).iterator().hasNext());
    }

    @Test
    public void testEdgeTTLWithTransactions() throws Exception {
        if (!features.hasCellTTL()) {
            return;
        }

        EdgeLabel label1 = mgmt.makeEdgeLabel("likes").make();
        mgmt.setTTL(label1, 1, TimeUnit.SECONDS);
        assertEquals(1, mgmt.getTTL(label1).getLength(TimeUnit.SECONDS));
        mgmt.commit();

        Vertex v1 = graph.addVertex(null), v2 = graph.addVertex(null);

        graph.addEdge(null, v1, v2, "likes");

        // pre-commit state of the edge.  It is not yet subject to TTL
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(1001);

        // the edge should have expired by now, but only if it had been committed
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());

        graph.commit();

        // still here, because we have just committed the edge.  Its countdown starts at the commit
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());

        Thread.sleep(1001);

        // the edge has expired in Cassandra, but still appears alive in this transaction
        assertTrue(v1.getVertices(Direction.OUT).iterator().hasNext());

        // syncing with the data store, we see that the edge has expired
        graph.rollback();
        assertFalse(v1.getVertices(Direction.OUT).iterator().hasNext());
    }

    @Test
    public void testEdgeTTLWithVertexCentricIndex() throws Exception {
        if (!features.hasCellTTL()) {
            return;
        }

        int ttl = 1; // artificially low TTL for test
        final PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).make();
        EdgeLabel wavedAt = mgmt.makeEdgeLabel("wavedAt").signature(time).make();
        mgmt.buildEdgeIndex(wavedAt, "timeindex", Direction.BOTH, Order.DESC, time);
        mgmt.setTTL(wavedAt, ttl, TimeUnit.SECONDS);
        assertEquals(0, mgmt.getTTL(time).getLength(TimeUnit.SECONDS));
        assertEquals(ttl, mgmt.getTTL(wavedAt).getLength(TimeUnit.SECONDS));
        mgmt.commit();

        Vertex v1 = graph.addVertex(null), v2 = graph.addVertex(null);
        Edge e1 = graph.addEdge(null, v1, v2, "wavedAt");
        e1.setProperty("time", 42);

        assertTrue(v1.getEdges(Direction.OUT).iterator().hasNext());
        assertTrue(v1.query().direction(Direction.OUT).interval("time", 0, 100).edges().iterator().hasNext());

        graph.commit();
        long commitTime = System.currentTimeMillis();

        assertTrue(v1.getEdges(Direction.OUT).iterator().hasNext());
        assertTrue(v1.query().direction(Direction.OUT).interval("time", 0, 100).edges().iterator().hasNext());

        Thread.sleep(commitTime + (ttl * 1000L + 100) - System.currentTimeMillis());
        graph.rollback();

        assertFalse(v1.getEdges(Direction.OUT).iterator().hasNext());
        assertFalse(v1.query().direction(Direction.OUT).interval("time", 0, 100).edges().iterator().hasNext());
    }

    @Test
    public void testEdgeTTLWithCompositeIndex() throws Exception {
        if (!features.hasCellTTL()) {
            return;
        }

        PropertyKey edgeName = mgmt.makePropertyKey("edge-name").dataType(String.class).make();
        mgmt.buildIndex("edge-name", Edge.class).addKey(edgeName).buildCompositeIndex();
        EdgeLabel label = mgmt.makeEdgeLabel("likes").make();
        mgmt.setTTL(label, 1, TimeUnit.SECONDS);
        assertEquals(0, mgmt.getTTL(edgeName).getLength(TimeUnit.SECONDS));
        assertEquals(1, mgmt.getTTL(label).getLength(TimeUnit.SECONDS));
        mgmt.commit();

        Vertex v1 = graph.addVertex(null), v2 = graph.addVertex(null);

        Edge e = graph.addEdge(null, v1, v2, "likes");
        e.setProperty("edge-name", "v1-likes-v2");

        graph.commit();

        assertTrue(v1.getEdges(Direction.OUT).iterator().hasNext());
        assertTrue(graph.getEdges("edge-name", "v1-likes-v2").iterator().hasNext());

        Thread.sleep(1001);

        graph.rollback();

        // the edge is gone not only from its previous endpoints, but also from key indices
        assertFalse(graph.getEdges("edge-name", "v1-likes-v2").iterator().hasNext());
        assertFalse(v1.getEdges(Direction.OUT).iterator().hasNext());
    }

    @Test
    public void testPropertyTTLTiming() throws Exception {
        if (!features.hasCellTTL()) {
            return;
        }

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        PropertyKey place = mgmt.makePropertyKey("place").dataType(String.class).make();
        mgmt.setTTL(name, 42, TimeUnit.SECONDS);
        mgmt.setTTL(place, 1, TimeUnit.SECONDS);
        TitanGraphIndex index1 = mgmt.buildIndex("index1", Vertex.class).addKey(name).buildCompositeIndex();
        TitanGraphIndex index2 = mgmt.buildIndex("index2", Vertex.class).addKey(name).addKey(place).buildCompositeIndex();
        VertexLabel label1 = mgmt.makeVertexLabel("event").setStatic().make();
        mgmt.setTTL(label1, 2, TimeUnit.SECONDS);
        assertEquals(42, mgmt.getTTL(name).getLength(TimeUnit.SECONDS));
        assertEquals(1, mgmt.getTTL(place).getLength(TimeUnit.SECONDS));
        assertEquals(2, mgmt.getTTL(label1).getLength(TimeUnit.SECONDS));
        mgmt.commit();

        Vertex v1 = tx.addVertex("event");
        v1.setProperty("name", "some event");
        v1.setProperty("place", "somewhere");

        tx.commit();
        Object id = v1.getId();

        v1 = graph.getVertex(id);
        assertNotNull(v1);
        assertTrue(graph.query().has("name","some event").has("place","somewhere").vertices().iterator().hasNext());
        assertTrue(graph.getVertices("name", "some event").iterator().hasNext());

        Thread.sleep(1001);
        graph.rollback();

        // short-lived property expires first
        v1 = graph.getVertex(id);
        assertNotNull(v1);
        assertFalse(graph.query().has("name","some event").has("place","somewhere").vertices().iterator().hasNext());
        assertTrue(graph.getVertices("name", "some event").iterator().hasNext());

        Thread.sleep(1001);
        graph.rollback();

        // vertex expires before defined TTL of the long-lived property
        assertFalse(graph.query().has("name","some event").has("place","somewhere").vertices().iterator().hasNext());
        assertFalse(graph.getVertices("place", "somewhere").iterator().hasNext());
        v1 = graph.getVertex(id);
        assertNull(v1);
    }

    @Test
    public void testVertexTTLWithCompositeIndex() throws Exception {
        if (!features.hasCellTTL()) {
            return;
        }

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        PropertyKey time = mgmt.makePropertyKey("time").dataType(Long.class).make();
        TitanGraphIndex index1 = mgmt.buildIndex("index1", Vertex.class).addKey(name).buildCompositeIndex();
        TitanGraphIndex index2 = mgmt.buildIndex("index2", Vertex.class).addKey(name).addKey(time).buildCompositeIndex();
        VertexLabel label1 = mgmt.makeVertexLabel("event").setStatic().make();
        mgmt.setTTL(label1, 1, TimeUnit.SECONDS);
        assertEquals(0, mgmt.getTTL(name).getLength(TimeUnit.SECONDS));
        assertEquals(0, mgmt.getTTL(time).getLength(TimeUnit.SECONDS));
        assertEquals(1, mgmt.getTTL(label1).getLength(TimeUnit.SECONDS));
        mgmt.commit();

        Vertex v1 = tx.addVertex("event");
        v1.setProperty("name", "some event");
        v1.setProperty("time", System.currentTimeMillis());
        tx.commit();
        Object id = v1.getId();

        v1 = graph.getVertex(id);
        assertNotNull(v1);
        assertTrue(graph.getVertices("name", "some event").iterator().hasNext());

        Thread.sleep(1001);
        graph.rollback();

        v1 = graph.getVertex(id);
        assertNull(v1);
        assertFalse(graph.getVertices("name", "some event").iterator().hasNext());
    }

    @Test
    public void testEdgeTTLLimitedByVertexTTL() throws Exception {

        if (!features.hasCellTTL()) {
            return;
        }
        Boolean dbCache = config.get("cache.db-cache", Boolean.class);
        if (null == dbCache) {
            dbCache = false;
        }

        EdgeLabel likes = mgmt.makeEdgeLabel("likes").make();
        mgmt.setTTL(likes, 42, TimeUnit.SECONDS); // long edge TTL will be overridden by short vertex TTL
        EdgeLabel dislikes = mgmt.makeEdgeLabel("dislikes").make();
        mgmt.setTTL(dislikes, 1, TimeUnit.SECONDS);
        EdgeLabel indifferentTo = mgmt.makeEdgeLabel("indifferentTo").make();
        VertexLabel label1 = mgmt.makeVertexLabel("person").setStatic().make();
        mgmt.setTTL(label1, 2, TimeUnit.SECONDS);
        assertEquals(42, mgmt.getTTL(likes).getLength(TimeUnit.SECONDS));
        assertEquals(1, mgmt.getTTL(dislikes).getLength(TimeUnit.SECONDS));
        assertEquals(0, mgmt.getTTL(indifferentTo).getLength(TimeUnit.SECONDS));
        assertEquals(2, mgmt.getTTL(label1).getLength(TimeUnit.SECONDS));
        mgmt.commit();

        Vertex v1 = tx.addVertex("person");
        Vertex v2 = tx.addVertex();
        Edge v1LikesV2 = tx.addEdge(null, v1, v2, "likes");
        Edge v1DislikesV2 = tx.addEdge(null, v1, v2, "dislikes");
        Edge v1IndifferentToV2 = tx.addEdge(null, v1, v2, "indifferentTo");
        tx.commit();
        long commitTime = System.currentTimeMillis();

        Object v1Id = v1.getId();
        Object v2id = v2.getId();
        Object v1LikesV2Id = v1LikesV2.getId();
        Object v1DislikesV2Id = v1DislikesV2.getId();
        Object v1IndifferentToV2Id = v1IndifferentToV2.getId();

        v1 = graph.getVertex(v1Id);
        v2 = graph.getVertex(v2id);
        v1LikesV2 = graph.getEdge(v1LikesV2Id);
        v1DislikesV2 = graph.getEdge(v1DislikesV2Id);
        v1IndifferentToV2 = graph.getEdge(v1IndifferentToV2Id);
        assertNotNull(v1);
        assertNotNull(v2);
        assertNotNull(v1LikesV2);
        assertNotNull(v1DislikesV2);
        assertNotNull(v1IndifferentToV2);
        assertTrue(v2.getEdges(Direction.IN, "likes").iterator().hasNext());
        assertTrue(v2.getEdges(Direction.IN, "dislikes").iterator().hasNext());
        assertTrue(v2.getEdges(Direction.IN, "indifferentTo").iterator().hasNext());

        Thread.sleep(commitTime + 1001L - System.currentTimeMillis());
        graph.rollback();

        v1 = graph.getVertex(v1Id);
        v2 = graph.getVertex(v2id);
        v1LikesV2 = graph.getEdge(v1LikesV2Id);
        v1DislikesV2 = graph.getEdge(v1DislikesV2Id);
        v1IndifferentToV2 = graph.getEdge(v1IndifferentToV2Id);
        assertNotNull(v1);
        assertNotNull(v2);
        assertNotNull(v1LikesV2);
        // this edge has expired
        assertNull(v1DislikesV2);
        assertNotNull(v1IndifferentToV2);
        assertTrue(v2.getEdges(Direction.IN, "likes").iterator().hasNext());
        // expired
        assertFalse(v2.getEdges(Direction.IN, "dislikes").iterator().hasNext());
        assertTrue(v2.getEdges(Direction.IN, "indifferentTo").iterator().hasNext());

        Thread.sleep(commitTime + 2001L - System.currentTimeMillis());
        graph.rollback();

        v1 = graph.getVertex(v1Id);
        v2 = graph.getVertex(v2id);
        v1LikesV2 = graph.getEdge(v1LikesV2Id);
        v1DislikesV2 = graph.getEdge(v1DislikesV2Id);
        v1IndifferentToV2 = graph.getEdge(v1IndifferentToV2Id);
        // the vertex itself has expired
        assertNull(v1);
        assertNotNull(v2);
        // all incident edges have necessarily expired
        assertNull(v1LikesV2);
        assertNull(v1DislikesV2);
        assertNull(v1IndifferentToV2);

        if (dbCache) {
            /* TODO: uncomment
            assertTrue(v2.getEdges(Direction.IN, "likes").iterator().hasNext());
            assertTrue(v2.getEdges(Direction.IN, "dislikes").iterator().hasNext());
            assertTrue(v2.getEdges(Direction.IN, "indifferentTo").iterator().hasNext());
            */
        } else {
            assertFalse(v2.getEdges(Direction.IN, "likes").iterator().hasNext());
            assertFalse(v2.getEdges(Direction.IN, "dislikes").iterator().hasNext());
            assertFalse(v2.getEdges(Direction.IN, "indifferentTo").iterator().hasNext());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSettingTTLOnUnsupportedType() throws Exception {
        if (!features.hasCellTTL()) {
            throw new IllegalArgumentException();
        }

        TitanSchemaType type = ImplicitKey.ID;
        mgmt.setTTL(type, 0, TimeUnit.SECONDS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTTLFromUnsupportedType() throws Exception {
        if (!features.hasCellTTL()) {
            throw new IllegalArgumentException();
        }

        TitanSchemaType type = ImplicitKey.ID;
        mgmt.getTTL(type);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSettingTTLOnNonStaticVertexLabel() throws Exception {
        if (!features.hasCellTTL()) {
            throw new IllegalArgumentException();
        }

        VertexLabel label1 = mgmt.makeVertexLabel("event").make();
        mgmt.setTTL(label1, 42, TimeUnit.SECONDS);
    }

    @Test
    public void testEdgeTTLImplicitKey() throws Exception {
        Duration d;

        if (!features.hasCellTTL()) {
            return;
        }

        clopen(option(GraphDatabaseConfiguration.STORE_META_TTL, "edgestore"), true);

        assertEquals("$ttl", ImplicitKey.TTL.getName());

        int ttl = 24*60*60;
        EdgeLabel likes = mgmt.makeEdgeLabel("likes").make();
        EdgeLabel hasLiked = mgmt.makeEdgeLabel("hasLiked").make();
        mgmt.setTTL(likes, ttl, TimeUnit.SECONDS);
        assertEquals(ttl, mgmt.getTTL(likes).getLength(TimeUnit.SECONDS));
        assertEquals(0, mgmt.getTTL(hasLiked).getLength(TimeUnit.SECONDS));
        mgmt.commit();

        Vertex v1 = graph.addVertex(null), v2 = graph.addVertex(null);

        Edge e1 = graph.addEdge(null, v1, v2, "likes");
        Edge e2 = graph.addEdge(null, v1, v2, "hasLiked");
        graph.commit();

        // read from the edge created in this transaction
        d = e1.getProperty("$ttl");
        assertEquals(86400, d.getLength(TimeUnit.SECONDS));

        // get the edge via a vertex
        e1 = v1.getEdges(Direction.OUT, "likes").iterator().next();
        d = e1.getProperty("$ttl");
        assertEquals(86400, d.getLength(TimeUnit.SECONDS));

        // returned value of $ttl is the total time to live since commit, not remaining time
        Thread.sleep(1001);
        graph.rollback();
        e1 = v1.getEdges(Direction.OUT, "likes").iterator().next();
        d = e1.getProperty("$ttl");
        assertEquals(86400, d.getLength(TimeUnit.SECONDS));

        // no ttl on edges of this label
        d = e2.getProperty("$ttl");
        assertEquals(0, d.getLength(TimeUnit.SECONDS));
    }

    @Test
    public void testVertexTTLImplicitKey() throws Exception {
        Duration d;

        if (!features.hasCellTTL()) {
            return;
        }

        clopen(option(GraphDatabaseConfiguration.STORE_META_TTL, "edgestore"), true);

        int ttl1 = 1;
        VertexLabel label1 = mgmt.makeVertexLabel("event").setStatic().make();
        mgmt.setTTL(label1, ttl1, TimeUnit.SECONDS);
        assertEquals(ttl1, mgmt.getTTL(label1).getLength(TimeUnit.SECONDS));
        mgmt.commit();

        Vertex v1 = tx.addVertex("event");
        Vertex v2 = tx.addVertex();
        tx.commit();

        /* TODO: this fails
        d = v1.getProperty("$ttl");
        assertEquals(1, d.getLength(TimeUnit.SECONDS));
        d = v2.getProperty("$ttl");
        assertEquals(0, d.getLength(TimeUnit.SECONDS));
        */

        Object v1id = v1.getId();
        Object v2id = v2.getId();
        v1 = graph.getVertex(v1id);
        v2 = graph.getVertex(v2id);

        d = v1.getProperty("$ttl");
        assertEquals(1, d.getLength(TimeUnit.SECONDS));
        d = v2.getProperty("$ttl");
        assertEquals(0, d.getLength(TimeUnit.SECONDS));
    }
}
