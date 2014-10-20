package com.thinkaurelius.titan.graphdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.attribute.*;
import com.thinkaurelius.titan.core.log.TransactionRecovery;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.Parameter;
import com.thinkaurelius.titan.core.schema.SchemaAction;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLog;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.indexing.IndexFeatures;
import com.thinkaurelius.titan.example.GraphOfTheGodsFactory;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.log.StandardTransactionLogProcessor;
import com.thinkaurelius.titan.graphdb.types.ParameterType;
import com.thinkaurelius.titan.graphdb.types.StandardEdgeLabelMaker;
import com.thinkaurelius.titan.testutil.TestGraphConfigs;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.*;

import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.thinkaurelius.titan.graphdb.TitanGraphTest.evaluateQuery;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.MAX_COMMIT_TIME;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.TRANSACTION_LOG;
import static com.thinkaurelius.titan.testutil.TitanAssert.assertCount;
import static com.thinkaurelius.titan.testutil.TitanAssert.assertEmpty;
import static com.thinkaurelius.titan.testutil.TitanAssert.assertNotEmpty;
import static org.junit.Assert.*;
import static com.tinkerpop.gremlin.structure.Order.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanIndexTest extends TitanGraphBaseTest {

    public static final String INDEX = GraphOfTheGodsFactory.INDEX_NAME;
    public static final String VINDEX = "v"+INDEX;
    public static final String EINDEX = "e"+INDEX;
    public static final String PINDEX = "p"+INDEX;


    public final boolean supportsGeoPoint;
    public final boolean supportsNumeric;
    public final boolean supportsText;

    public IndexFeatures indexFeatures;

    private static final Logger log =
            LoggerFactory.getLogger(TitanIndexTest.class);

    protected TitanIndexTest(boolean supportsGeoPoint, boolean supportsNumeric, boolean supportsText) {
        this.supportsGeoPoint = supportsGeoPoint;
        this.supportsNumeric = supportsNumeric;
        this.supportsText = supportsText;
    }

    private Parameter getStringMapping() {
        if (indexFeatures.supportsStringMapping(Mapping.STRING)) return Mapping.STRING.getParameter();
        else if (indexFeatures.supportsStringMapping(Mapping.TEXTSTRING)) return Mapping.TEXTSTRING.getParameter();
        throw new AssertionError("String mapping not supported");
    }

    private Parameter getTextMapping() {
        if (indexFeatures.supportsStringMapping(Mapping.TEXT)) return Mapping.TEXT.getParameter();
        else if (indexFeatures.supportsStringMapping(Mapping.TEXTSTRING)) return Mapping.TEXTSTRING.getParameter();
        throw new AssertionError("Text mapping not supported");
    }

    private Parameter getFieldMap(PropertyKey key) {
        return ParameterType.MAPPED_NAME.getParameter(key.name());
    }

    public abstract boolean supportsLuceneStyleQueries();

    @Override
    public void open(WriteConfiguration config) {
        super.open(config);
        indexFeatures = graph.getBackend().getIndexFeatures().get(INDEX);
    }

    @Rule
    public TestName methodName = new TestName();

    /**
     * Tests the {@link com.thinkaurelius.titan.example.GraphOfTheGodsFactory#load(com.thinkaurelius.titan.core.TitanGraph)}
     * method used as the standard example that ships with Titan.
     */
    @Test
    public void testGraphOfTheGods() {
        GraphOfTheGodsFactory.load(graph);
        assertGraphOfTheGods(graph);
    }

    public static void assertGraphOfTheGods(TitanGraph gotg) {
        assertCount(12, gotg.V());
        assertCount(3, gotg.V().has("label", "god"));
        Vertex h = getOnlyElement(gotg.V().has("name", "hercules"));
        assertEquals(30, h.<Integer>value("age").intValue());
        assertEquals("demigod",h.label());
        assertCount(5, h.bothE());
    }

    @Test
    public void testSimpleUpdate() {
        PropertyKey name = makeKey("name",String.class);
        EdgeLabel knows = makeLabel("knows");
        mgmt.buildIndex("namev",Vertex.class).addKey(name).buildMixedIndex(INDEX);
        mgmt.buildIndex("namee",Edge.class).addKey(name).buildMixedIndex(INDEX);
        finishSchema();

        Vertex v = tx.addVertex("name", "Marko Rodriguez");
        Edge e = v.addEdge("knows",v, "name","Hulu Bubab");
        assertCount(1, tx.V().has("name", Text.CONTAINS, "marko"));
        assertCount(1, tx.E().has("name", Text.CONTAINS, "Hulu"));
        for (Vertex u : tx.V().toList()) assertEquals("Marko Rodriguez",u.value("name"));
        clopen();
        assertCount(1, tx.V().has("name", Text.CONTAINS, "marko"));
        assertCount(1, tx.E().has("name", Text.CONTAINS, "Hulu"));
        for (Vertex u : tx.V().toList()) assertEquals("Marko Rodriguez",u.value("name"));
        v = getOnlyElement(tx.V().has("name", Text.CONTAINS, "marko"));
        v.singleProperty("name", "Marko");
        e = getOnlyElement(v.outE());
        e.property("name","Tubu Rubu");
        assertCount(1, tx.V().has("name", Text.CONTAINS, "marko"));
        assertCount(1, tx.E().has("name", Text.CONTAINS, "Rubu"));
        assertCount(0, tx.E().has("name", Text.CONTAINS, "Hulu"));
        for (Vertex u : tx.V().toList()) assertEquals("Marko",u.value("name"));
        clopen();
        assertCount(1, tx.V().has("name", Text.CONTAINS, "marko"));
        assertCount(1, tx.E().has("name", Text.CONTAINS, "Rubu"));
        assertCount(0, tx.E().has("name", Text.CONTAINS, "Hulu"));
        for (Vertex u : tx.V().toList()) assertEquals("Marko",u.value("name"));
    }



    @Test
    public void testIndexing() {

        PropertyKey text = makeKey("text",String.class);
        createExternalVertexIndex(text,INDEX);
        createExternalEdgeIndex(text,INDEX);

        PropertyKey location = makeKey("location",Geoshape.class);
        createExternalVertexIndex(location,INDEX);
        createExternalEdgeIndex(location,INDEX);

        PropertyKey time = makeKey("time",Long.class);
        createExternalVertexIndex(time,INDEX);
        createExternalEdgeIndex(time,INDEX);

        PropertyKey category = makeKey("category",Integer.class);
        mgmt.buildIndex("vcategory",Vertex.class).addKey(category).buildCompositeIndex();
        mgmt.buildIndex("ecategory",Edge.class).addKey(category).buildCompositeIndex();

        PropertyKey group = makeKey("group",Byte.class);
        createExternalVertexIndex(group,INDEX);
        createExternalEdgeIndex(group,INDEX);

        PropertyKey id = makeVertexIndexedKey("uid",Integer.class);
        EdgeLabel knows = ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("knows")).sortKey(time).signature(location).make();
        finishSchema();

        clopen();
        String[] words = {"world", "aurelius", "titan", "graph"};
        int numCategories = 5;
        int numGroups = 10;
        double distance, offset;
        int numV = 100;
        final int originalNumV = numV;
        for (int i = 0; i < numV; i++) {
            Vertex v = tx.addVertex();
            v.singleProperty("uid", i);
            v.singleProperty("category", i % numCategories);
            v.singleProperty("group", i % numGroups);
            v.singleProperty("text", "Vertex " + words[i % words.length]);
            v.singleProperty("time", i);
            offset = (i % 2 == 0 ? 1 : -1) * (i * 50.0 / numV);
            v.singleProperty("location", Geoshape.point(0.0 + offset, 0.0 + offset));

            Edge e = v.addEdge("knows", getVertex("uid", Math.max(0, i - 1)));
            e.property("text", "Vertex " + words[i % words.length]);
            e.property("time", i);
            e.property("category", i % numCategories);
            e.property("group", i % numGroups);
            e.property("location", Geoshape.point(0.0 + offset, 0.0 + offset));
        }

        for (int i = 0; i < words.length; i++) {
            int expectedSize = numV / words.length;
            assertCount(expectedSize, tx.V().has("text", Text.CONTAINS, words[i]));
            assertCount(expectedSize, tx.E().has("text", Text.CONTAINS, words[i]));

            //Test ordering
            for (String orderKey : new String[]{"time", "category"}) {
                for (Order order : Order.values()) {
                    for (GraphTraversal<?,? extends Element> traversal : ImmutableList.of(
                            tx.V().has("text", Text.CONTAINS, words[i]).orderBy(orderKey, order.getTP()),
                            tx.V().has("text", Text.CONTAINS, words[i]).orderBy(orderKey, order.getTP())
                    )) {
                        verifyElementOrder(traversal.toList(),orderKey,order,expectedSize);
                    }
                }
            }
        }

        assertCount(3, tx.V().has("group", 3).orderBy("time", incr).range(0,3));
        assertCount(3, tx.E().has("group", 3).orderBy("time", decr).range(0,3));

        for (int i = 0; i < numV / 2; i += numV / 10) {
            assertCount(i, tx.V().has("time", Cmp.GREATER_THAN_EQUAL, i).has("time", Cmp.LESS_THAN, i + i));
            assertCount(i, tx.E().has("time", Cmp.GREATER_THAN_EQUAL, i).has("time", Cmp.LESS_THAN, i + i));
        }

        for (int i = 0; i < numV; i += 10) {
            offset = (i * 50.0 / originalNumV);
            distance = Geoshape.point(0.0, 0.0).getPoint().distance(Geoshape.point(offset, offset).getPoint()) + 20;
            assertCount(i + 1, tx.V().has("location", Geo.WITHIN, Geoshape.circle(0.0, 0.0, distance)));
            assertCount(i + 1, tx.E().has("location", Geo.WITHIN, Geoshape.circle(0.0, 0.0, distance)));
        }

        //Queries combining mixed and composite indexes
        assertCount(4, tx.V().has("category", 1).interval("time", 10, 28));
        assertCount(4, tx.E().has("category", 1).interval("time", 10, 28));

        assertCount(5, tx.V().has("time", Cmp.GREATER_THAN_EQUAL, 10).has("time", Cmp.LESS_THAN, 30).has("text", Text.CONTAINS, words[0]));
        offset = (19 * 50.0 / originalNumV);
        distance = Geoshape.point(0.0, 0.0).getPoint().distance(Geoshape.point(offset, offset).getPoint()) + 20;
        assertCount(5, tx.V().has("location", Geo.INTERSECT, Geoshape.circle(0.0, 0.0, distance)).has("text", Text.CONTAINS, words[0]));

        assertCount(numV, tx.V());
        assertCount(numV, tx.E());

        //--------------

        clopen();

        //##########################
        //Copied from above
        //##########################

        for (int i = 0; i < words.length; i++) {
            int expectedSize = numV / words.length;
            assertCount(expectedSize, tx.V().has("text", Text.CONTAINS, words[i]));
            assertCount(expectedSize, tx.E().has("text", Text.CONTAINS, words[i]));

            //Test ordering
            for (String orderKey : new String[]{"time", "category"}) {
                for (Order order : Order.values()) {
                    for (GraphTraversal<?,? extends Element> traversal : ImmutableList.of(
                            tx.V().has("text", Text.CONTAINS, words[i]).orderBy(orderKey, order.getTP()),
                            tx.V().has("text", Text.CONTAINS, words[i]).orderBy(orderKey, order.getTP())
                    )) {
                        verifyElementOrder(traversal.toList(),orderKey,order,expectedSize);
                    }
                }
            }
        }

        assertCount(3, tx.V().has("group", 3).orderBy("time", incr).range(0,3));
        assertCount(3, tx.E().has("group", 3).orderBy("time", decr).range(0,3));

        for (int i = 0; i < numV / 2; i += numV / 10) {
            assertCount(i, tx.V().has("time", Cmp.GREATER_THAN_EQUAL, i).has("time", Cmp.LESS_THAN, i + i));
            assertCount(i, tx.E().has("time", Cmp.GREATER_THAN_EQUAL, i).has("time", Cmp.LESS_THAN, i + i));
        }

        for (int i = 0; i < numV; i += 10) {
            offset = (i * 50.0 / originalNumV);
            distance = Geoshape.point(0.0, 0.0).getPoint().distance(Geoshape.point(offset, offset).getPoint()) + 20;
            assertCount(i + 1, tx.V().has("location", Geo.WITHIN, Geoshape.circle(0.0, 0.0, distance)));
            assertCount(i + 1, tx.E().has("location", Geo.WITHIN, Geoshape.circle(0.0, 0.0, distance)));
        }

        //Queries combining mixed and composite indexes
        assertCount(4, tx.V().has("category", 1).interval("time", 10, 28));
        assertCount(4, tx.E().has("category", 1).interval("time", 10, 28));

        assertCount(5, tx.V().has("time", Cmp.GREATER_THAN_EQUAL, 10).has("time", Cmp.LESS_THAN, 30).has("text", Text.CONTAINS, words[0]));
        offset = (19 * 50.0 / originalNumV);
        distance = Geoshape.point(0.0, 0.0).getPoint().distance(Geoshape.point(offset, offset).getPoint()) + 20;
        assertCount(5, tx.V().has("location", Geo.INTERSECT, Geoshape.circle(0.0, 0.0, distance)).has("text", Text.CONTAINS, words[0]));

        assertCount(numV, tx.V());
        assertCount(numV, tx.E());

        newTx();

        int numDelete = 12;
        for (int i = numV - numDelete; i < numV; i++) {
            getVertex("uid", i).remove();
        }

        numV = numV - numDelete;

        //Copied from above
        for (int i = 0; i < words.length; i++) {
            assertCount(numV / words.length, tx.V().has("text", Text.CONTAINS, words[i]));
            assertCount(numV / words.length, tx.E().has("text", Text.CONTAINS, words[i]));
        }

        for (int i = 0; i < numV / 2; i += numV / 10) {
            assertCount(i, tx.V().has("time", Cmp.GREATER_THAN_EQUAL, i).has("time", Cmp.LESS_THAN, i + i));
            assertCount(i, tx.E().has("time", Cmp.GREATER_THAN_EQUAL, i).has("time", Cmp.LESS_THAN, i + i));
        }

        for (int i = 0; i < numV; i += 10) {
            offset = (i * 50.0 / originalNumV);
            distance = Geoshape.point(0.0, 0.0).getPoint().distance(Geoshape.point(offset, offset).getPoint()) + 20;
            assertCount(i + 1, tx.V().has("location", Geo.WITHIN, Geoshape.circle(0.0, 0.0, distance)));
            assertCount(i + 1, tx.E().has("location", Geo.WITHIN, Geoshape.circle(0.0, 0.0, distance)));
        }

        assertCount(5, tx.V().has("time", Cmp.GREATER_THAN_EQUAL, 10).has("time", Cmp.LESS_THAN, 30).has("text", Text.CONTAINS, words[0]));
        offset = (19 * 50.0 / originalNumV);
        distance = Geoshape.point(0.0, 0.0).getPoint().distance(Geoshape.point(offset, offset).getPoint()) + 20;
        assertCount(5, tx.V().has("location", Geo.INTERSECT, Geoshape.circle(0.0, 0.0, distance)).has("text", Text.CONTAINS, words[0]));

        assertCount(numV, tx.V());
        assertCount(numV, tx.E());




    }

    /**
     * Tests conditional indexing and the different management features
     */
    @Test
    public void testConditionalIndexing() {
        PropertyKey name = makeKey("name", String.class);
        PropertyKey weight = makeKey("weight",Decimal.class);
        PropertyKey text = makeKey("text", String.class);

        VertexLabel person = mgmt.makeVertexLabel("person").make();
        VertexLabel org = mgmt.makeVertexLabel("org").make();

        TitanGraphIndex index1 = mgmt.buildIndex("index1",Vertex.class).
                addKey(name, getStringMapping()).buildMixedIndex(INDEX);
        TitanGraphIndex index2 = mgmt.buildIndex("index2",Vertex.class).indexOnly(person).
                addKey(text, getTextMapping()).addKey(weight).buildMixedIndex(INDEX);
        TitanGraphIndex index3 = mgmt.buildIndex("index3",Vertex.class).indexOnly(org).
                addKey(text, getTextMapping()).addKey(weight).buildMixedIndex(INDEX);

        // ########### INSPECTION & FAILURE ##############
        assertTrue(mgmt.containsGraphIndex("index1"));
        assertFalse(mgmt.containsGraphIndex("index"));
        assertEquals(3,Iterables.size(mgmt.getGraphIndexes(Vertex.class)));
        assertNull(mgmt.getGraphIndex("indexx"));

        name = mgmt.getPropertyKey("name");
        weight = mgmt.getPropertyKey("weight");
        text = mgmt.getPropertyKey("text");
        person = mgmt.getVertexLabel("person");
        org = mgmt.getVertexLabel("org");
        index1 = mgmt.getGraphIndex("index1");
        index2 = mgmt.getGraphIndex("index2");
        index3 = mgmt.getGraphIndex("index3");

        assertTrue(Vertex.class.isAssignableFrom(index1.getIndexedElement()));
        assertEquals("index2",index2.name());
        assertEquals(INDEX,index3.getBackingIndex());
        assertFalse(index2.isUnique());
        assertEquals(2,index3.getFieldKeys().length);
        assertEquals(1,index1.getFieldKeys().length);
        assertEquals(3,index3.getParametersFor(text).length);
        assertEquals(2,index3.getParametersFor(weight).length);

        try {
            //Already exists
            mgmt.buildIndex("index2",Vertex.class).addKey(weight).buildMixedIndex(INDEX);
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //Already exists
            mgmt.buildIndex("index2",Vertex.class).addKey(weight).buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //Key is already added
            mgmt.addIndexKey(index2,weight);
            fail();
        } catch (IllegalArgumentException e) {}

        finishSchema();
        clopen();

        // ########### INSPECTION & FAILURE (copied from above) ##############
        assertTrue(mgmt.containsGraphIndex("index1"));
        assertFalse(mgmt.containsGraphIndex("index"));
        assertEquals(3,Iterables.size(mgmt.getGraphIndexes(Vertex.class)));
        assertNull(mgmt.getGraphIndex("indexx"));

        name = mgmt.getPropertyKey("name");
        weight = mgmt.getPropertyKey("weight");
        text = mgmt.getPropertyKey("text");
        person = mgmt.getVertexLabel("person");
        org = mgmt.getVertexLabel("org");
        index1 = mgmt.getGraphIndex("index1");
        index2 = mgmt.getGraphIndex("index2");
        index3 = mgmt.getGraphIndex("index3");

        assertTrue(Vertex.class.isAssignableFrom(index1.getIndexedElement()));
        assertEquals("index2",index2.name());
        assertEquals(INDEX,index3.getBackingIndex());
        assertFalse(index2.isUnique());
        assertEquals(2,index3.getFieldKeys().length);
        assertEquals(1,index1.getFieldKeys().length);
        assertEquals(3,index3.getParametersFor(text).length);
        assertEquals(2,index3.getParametersFor(weight).length);

        try {
            //Already exists
            mgmt.buildIndex("index2",Vertex.class).addKey(weight).buildMixedIndex(INDEX);
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //Already exists
            mgmt.buildIndex("index2",Vertex.class).addKey(weight).buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            //Key is already added
            mgmt.addIndexKey(index2,weight);
            fail();
        } catch (IllegalArgumentException e) {}


        // ########### TRANSACTIONAL ##############
        weight = tx.getPropertyKey("weight");


        final int numV = 200;
        String[] strs = {"houseboat","humanoid","differential","extraordinary"};
        String[] strs2= new String[strs.length];
        for (int i=0;i<strs.length;i++) strs2[i]=strs[i]+" "+strs[i];
        final int modulo = 5;
        assert numV%(modulo*strs.length*2)==0;

        for (int i=0;i<numV;i++) {
            Vertex v = tx.addVertex(i % 2 == 0 ? "person" : "org");
            v.property("name",strs[i%strs.length]);
            v.property("text", strs[i % strs.length]);
            v.property("weight", (i % modulo) + 0.5);
        }

        //########## QUERIES ################
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[0]).has("label",Cmp.EQUAL,"person"),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{true,true},index2.name());
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[0]).has("label",Cmp.EQUAL,"person").orderBy("weight",Order.DESC),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{true,true},weight,Order.DESC,index2.name());
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[3]).has("label",Cmp.EQUAL,"org"),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{true,true},index3.name());
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[1]).has("label",Cmp.EQUAL,"org").orderBy("weight",Order.DESC),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{true,true},weight,Order.DESC,index3.name());
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[0]).has("weight",Cmp.EQUAL,2.5).has("label",Cmp.EQUAL,"person"),ElementCategory.VERTEX,
                numV/(modulo*strs.length),new boolean[]{true,true},index2.name());
        evaluateQuery(tx.query().has("name",Cmp.EQUAL,strs[2]).has("label",Cmp.EQUAL,"person"),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{false,true},index1.name());
        evaluateQuery(tx.query().has("name",Cmp.EQUAL,strs[3]).has("label",Cmp.EQUAL,"person"),ElementCategory.VERTEX,
                0,new boolean[]{false,true},index1.name());
        evaluateQuery(tx.query().has("name",Cmp.EQUAL,strs[0]),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{true,true},index1.name());
        evaluateQuery(tx.query().has("name",Cmp.EQUAL,strs[2]).has("text",Text.CONTAINS,strs[2]).has("label",Cmp.EQUAL,"person"),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{true,true},index1.name(),index2.name());
        evaluateQuery(tx.query().has("name",Cmp.EQUAL,strs[0]).has("text",Text.CONTAINS,strs[0]).has("label",Cmp.EQUAL,"person").orderBy("weight",Order.ASC),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{true,true},weight,Order.ASC,index1.name(),index2.name());

        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[0]),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{false,true});
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[0]).orderBy("weight",Order.ASC),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{false,false},weight,Order.ASC);

        clopen();
        weight = tx.getPropertyKey("weight");

        //########## QUERIES (copied from above) ################
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[0]).has("label",Cmp.EQUAL,"person"),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{true,true},index2.name());
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[0]).has("label",Cmp.EQUAL,"person").orderBy("weight",Order.DESC),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{true,true},weight,Order.DESC,index2.name());
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[3]).has("label",Cmp.EQUAL,"org"),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{true,true},index3.name());
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[1]).has("label",Cmp.EQUAL,"org").orderBy("weight",Order.DESC),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{true,true},weight,Order.DESC,index3.name());
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[0]).has("weight",Cmp.EQUAL,2.5).has("label",Cmp.EQUAL,"person"),ElementCategory.VERTEX,
                numV/(modulo*strs.length),new boolean[]{true,true},index2.name());
        evaluateQuery(tx.query().has("name",Cmp.EQUAL,strs[2]).has("label",Cmp.EQUAL,"person"),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{false,true},index1.name());
        evaluateQuery(tx.query().has("name",Cmp.EQUAL,strs[3]).has("label",Cmp.EQUAL,"person"),ElementCategory.VERTEX,
                0,new boolean[]{false,true},index1.name());
        evaluateQuery(tx.query().has("name",Cmp.EQUAL,strs[0]),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{true,true},index1.name());
        evaluateQuery(tx.query().has("name",Cmp.EQUAL,strs[2]).has("text",Text.CONTAINS,strs[2]).has("label",Cmp.EQUAL,"person"),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{true,true},index1.name(),index2.name());
        evaluateQuery(tx.query().has("name",Cmp.EQUAL,strs[0]).has("text",Text.CONTAINS,strs[0]).has("label",Cmp.EQUAL,"person").orderBy("weight",Order.ASC),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{true,true},weight,Order.ASC,index1.name(),index2.name());

        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[0]),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{false,true});
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[0]).orderBy("weight",Order.ASC),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{false,false},weight,Order.ASC);
    }

    @Test
    public void testCompositeAndMixedIndexing() {
        PropertyKey name = makeKey("name", String.class);
        PropertyKey weight = makeKey("weight",Decimal.class);
        PropertyKey text = makeKey("text", String.class);
        PropertyKey flag = makeKey("flag",Boolean.class);

        TitanGraphIndex composite = mgmt.buildIndex("composite",Vertex.class).addKey(name).addKey(weight).buildCompositeIndex();
        TitanGraphIndex mixed = mgmt.buildIndex("mixed", Vertex.class).addKey(weight)
                                    .addKey(text, getTextMapping()).buildMixedIndex(INDEX);
        mixed.name(); composite.name();
        finishSchema();

        final int numV = 100;
        String[] strs = {"houseboat","humanoid","differential","extraordinary"};
        String[] strs2= new String[strs.length];
        for (int i=0;i<strs.length;i++) strs2[i]=strs[i]+" "+strs[i];
        final int modulo = 5;
        final int divisor = modulo*strs.length;

        for (int i=0;i<numV;i++) {
            Vertex v = tx.addVertex();
            v.property("name", strs[i % strs.length]);
            v.property("text", strs[i % strs.length]);
            v.property("weight", (i % modulo) + 0.5);
            v.property("flag", true);
        }

        evaluateQuery(tx.query().has("name",Cmp.EQUAL,strs[0]),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{false,true});
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[0]),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{true,true},mixed.name());
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[0]).has("flag"),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{false,true},mixed.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[0]).has("weight", Cmp.EQUAL, 1.5), ElementCategory.VERTEX,
                numV / divisor, new boolean[]{true, true}, composite.name());
        evaluateQuery(tx.query().has("name",Cmp.EQUAL,strs[0]).has("weight",Cmp.EQUAL,1.5).has("flag"),ElementCategory.VERTEX,
                numV/divisor,new boolean[]{false,true},composite.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[2]).has("weight", Cmp.EQUAL, 2.5), ElementCategory.VERTEX,
                numV / divisor, new boolean[]{true, true}, mixed.name());
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[2]).has("weight",Cmp.EQUAL,2.5).has("flag"),ElementCategory.VERTEX,
                numV/divisor,new boolean[]{false,true},mixed.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[3]).has("name",Cmp.EQUAL,strs[3]).has("weight",Cmp.EQUAL,3.5),ElementCategory.VERTEX,
                numV/divisor,new boolean[]{true,true},mixed.name(),composite.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[3]).has("name",Cmp.EQUAL,strs[3]).has("weight",Cmp.EQUAL,3.5).has("flag"),ElementCategory.VERTEX,
                numV/divisor,new boolean[]{false,true},mixed.name(),composite.name());

        clopen();

        //Same queries as above
        evaluateQuery(tx.query().has("name",Cmp.EQUAL,strs[0]),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{false,true});
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[0]),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{true,true},mixed.name());
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[0]).has("flag"),ElementCategory.VERTEX,
                numV/strs.length,new boolean[]{false,true},mixed.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[0]).has("weight", Cmp.EQUAL, 1.5), ElementCategory.VERTEX,
                numV / divisor, new boolean[]{true, true}, composite.name());
        evaluateQuery(tx.query().has("name",Cmp.EQUAL,strs[0]).has("weight",Cmp.EQUAL,1.5).has("flag"),ElementCategory.VERTEX,
                numV/divisor,new boolean[]{false,true},composite.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[2]).has("weight", Cmp.EQUAL, 2.5), ElementCategory.VERTEX,
                numV / divisor, new boolean[]{true, true}, mixed.name());
        evaluateQuery(tx.query().has("text",Text.CONTAINS,strs[2]).has("weight",Cmp.EQUAL,2.5).has("flag"),ElementCategory.VERTEX,
                numV/divisor,new boolean[]{false,true},mixed.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[3]).has("name",Cmp.EQUAL,strs[3]).has("weight",Cmp.EQUAL,3.5),ElementCategory.VERTEX,
                numV/divisor,new boolean[]{true,true},mixed.name(),composite.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[3]).has("name",Cmp.EQUAL,strs[3]).has("weight",Cmp.EQUAL,3.5).has("flag"),ElementCategory.VERTEX,
                numV/divisor,new boolean[]{false,true},mixed.name(),composite.name());

    }


    private void setupChainGraph(int numV, String[] strs, boolean sameNameMapping) {
        clopen(option(INDEX_NAME_MAPPING,INDEX),sameNameMapping);
        TitanGraphIndex vindex = getExternalIndex(Vertex.class,INDEX);
        TitanGraphIndex eindex = getExternalIndex(Edge.class,INDEX);
        TitanGraphIndex pindex = getExternalIndex(TitanVertexProperty.class,INDEX);
        PropertyKey name = makeKey("name",String.class);

        mgmt.addIndexKey(vindex, name, getStringMapping());
        mgmt.addIndexKey(eindex, name, getStringMapping());
        mgmt.addIndexKey(pindex, name, getStringMapping(), Parameter.of("mapped-name", "xstr"));
        PropertyKey text = makeKey("text",String.class);
        mgmt.addIndexKey(vindex, text, getTextMapping(), Parameter.of("mapped-name","xtext"));
        mgmt.addIndexKey(eindex, text, getTextMapping());
        mgmt.addIndexKey(pindex, text, getTextMapping());
        mgmt.makeEdgeLabel("knows").signature(name).make();
        mgmt.makePropertyKey("uid").dataType(String.class).signature(text).make();
        finishSchema();
        Vertex previous = null;
        for (int i=0;i<numV;i++) {
            Vertex v = graph.addVertex("name",strs[i%strs.length],"text",strs[i%strs.length]);
            Edge e = v.addEdge("knows",previous==null?v:previous,
                    "name",strs[i%strs.length],"text",strs[i%strs.length]);
            VertexProperty p = v.property("uid", "v" + i,
                    "name", strs[i % strs.length], "text", strs[i % strs.length]);
            previous=v;
        }
    }

    /**
     * Tests index parameters (mapping and names) and various string predicates
     */
    @Test
    public void testIndexParameters() {
        int numV = 1000;
        String[] strs = {"Uncle Berry has a farm","and on his farm he has five ducks","ducks are beautiful animals","the sky is very blue today"};
        setupChainGraph(numV,strs,false);

        evaluateQuery(graph.query().has("text",Text.CONTAINS,"ducks"),
                ElementCategory.VERTEX,numV/strs.length*2,new boolean[]{true,true},VINDEX);
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"ducks").vertices()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"farm").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS,"beautiful").vertices()));
        evaluateQuery(graph.query().has("text",Text.CONTAINS_PREFIX,"beauti"),
                ElementCategory.VERTEX,numV/strs.length,new boolean[]{true,true},VINDEX);
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS_REGEX,"be[r]+y").vertices()));
        assertEquals(0,Iterables.size(graph.query().has("text",Text.CONTAINS,"lolipop").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).vertices()));
        assertEquals(numV/strs.length*(strs.length-1),Iterables.size(graph.query().has("name",Cmp.NOT_EQUAL,strs[2]).vertices()));
        assertEquals(0,Iterables.size(graph.query().has("name",Cmp.EQUAL,"farm").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Text.PREFIX,"ducks").vertices()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("name",Text.REGEX,"(.*)ducks(.*)").vertices()));

        //Same queries for edges
        evaluateQuery(graph.query().has("text",Text.CONTAINS,"ducks"),
                ElementCategory.EDGE,numV/strs.length*2,new boolean[]{true,true},EINDEX);
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"ducks").edges()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"farm").edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS,"beautiful").edges()));
        evaluateQuery(graph.query().has("text",Text.CONTAINS_PREFIX,"beauti"),
                ElementCategory.EDGE,numV/strs.length,new boolean[]{true,true},EINDEX);
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS_REGEX,"be[r]+y").edges()));
        assertEquals(0,Iterables.size(graph.query().has("text",Text.CONTAINS,"lolipop").edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).edges()));
        assertEquals(numV/strs.length*(strs.length-1),Iterables.size(graph.query().has("name",Cmp.NOT_EQUAL,strs[2]).edges()));
        assertEquals(0,Iterables.size(graph.query().has("name",Cmp.EQUAL,"farm").edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Text.PREFIX,"ducks").edges()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("name",Text.REGEX,"(.*)ducks(.*)").edges()));

        //Same queries for properties
        evaluateQuery(graph.query().has("text",Text.CONTAINS,"ducks"),
                ElementCategory.PROPERTY,numV/strs.length*2,new boolean[]{true,true},PINDEX);
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"ducks").properties()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"farm").properties()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS,"beautiful").properties()));
        evaluateQuery(graph.query().has("text",Text.CONTAINS_PREFIX,"beauti"),
                ElementCategory.PROPERTY,numV/strs.length,new boolean[]{true,true},PINDEX);
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS_REGEX,"be[r]+y").properties()));
        assertEquals(0,Iterables.size(graph.query().has("text",Text.CONTAINS,"lolipop").properties()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).properties()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).properties()));
        assertEquals(numV/strs.length*(strs.length-1),Iterables.size(graph.query().has("label","uid").has("name",Cmp.NOT_EQUAL,strs[2]).properties()));
        assertEquals(0,Iterables.size(graph.query().has("name",Cmp.EQUAL,"farm").properties()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Text.PREFIX,"ducks").properties()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("name",Text.REGEX,"(.*)ducks(.*)").properties()));


        clopen();
        /* =======================================
        Same queries as above but against backend */

        evaluateQuery(graph.query().has("text",Text.CONTAINS,"ducks"),
                ElementCategory.VERTEX,numV/strs.length*2,new boolean[]{true,true},VINDEX);
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"ducks").vertices()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"farm").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS,"beautiful").vertices()));
        evaluateQuery(graph.query().has("text",Text.CONTAINS_PREFIX,"beauti"),
                ElementCategory.VERTEX,numV/strs.length,new boolean[]{true,true},VINDEX);
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS_REGEX,"be[r]+y").vertices()));
        assertEquals(0,Iterables.size(graph.query().has("text",Text.CONTAINS,"lolipop").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).vertices()));
        assertEquals(numV/strs.length*(strs.length-1),Iterables.size(graph.query().has("name",Cmp.NOT_EQUAL,strs[2]).vertices()));
        assertEquals(0,Iterables.size(graph.query().has("name",Cmp.EQUAL,"farm").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Text.PREFIX,"ducks").vertices()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("name",Text.REGEX,"(.*)ducks(.*)").vertices()));

        //Same queries for edges
        evaluateQuery(graph.query().has("text",Text.CONTAINS,"ducks"),
                ElementCategory.EDGE,numV/strs.length*2,new boolean[]{true,true},EINDEX);
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"ducks").edges()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"farm").edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS,"beautiful").edges()));
        evaluateQuery(graph.query().has("text",Text.CONTAINS_PREFIX,"beauti"),
                ElementCategory.EDGE,numV/strs.length,new boolean[]{true,true},EINDEX);
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS_REGEX,"be[r]+y").edges()));
        assertEquals(0,Iterables.size(graph.query().has("text",Text.CONTAINS,"lolipop").edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).edges()));
        assertEquals(numV/strs.length*(strs.length-1),Iterables.size(graph.query().has("name",Cmp.NOT_EQUAL,strs[2]).edges()));
        assertEquals(0,Iterables.size(graph.query().has("name",Cmp.EQUAL,"farm").edges()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Text.PREFIX,"ducks").edges()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("name",Text.REGEX,"(.*)ducks(.*)").edges()));

        //Same queries for properties
        evaluateQuery(graph.query().has("text",Text.CONTAINS,"ducks"),
                ElementCategory.PROPERTY,numV/strs.length*2,new boolean[]{true,true},PINDEX);
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"ducks").properties()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("text",Text.CONTAINS,"farm").properties()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS,"beautiful").properties()));
        evaluateQuery(graph.query().has("text",Text.CONTAINS_PREFIX,"beauti"),
                ElementCategory.PROPERTY,numV/strs.length,new boolean[]{true,true},PINDEX);
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("text",Text.CONTAINS_REGEX,"be[r]+y").properties()));
        assertEquals(0,Iterables.size(graph.query().has("text",Text.CONTAINS,"lolipop").properties()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).properties()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Cmp.EQUAL,strs[1]).properties()));
        assertEquals(numV/strs.length*(strs.length-1),Iterables.size(graph.query().has("label","uid").has("name",Cmp.NOT_EQUAL,strs[2]).properties()));
        assertEquals(0,Iterables.size(graph.query().has("name",Cmp.EQUAL,"farm").properties()));
        assertEquals(numV/strs.length,Iterables.size(graph.query().has("name",Text.PREFIX,"ducks").properties()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.query().has("name",Text.REGEX,"(.*)ducks(.*)").properties()));

        //Test name mapping
        if (supportsLuceneStyleQueries()) {
            assertEquals(numV / strs.length * 2, Iterables.size(graph.indexQuery(VINDEX, "xtext:ducks").vertices()));
            assertEquals(0, Iterables.size(graph.indexQuery(EINDEX, "xtext:ducks").edges()));
        }
    }

    /**
     * Tests index parameters (mapping and names) with raw indexQuery
     */
    @Test
    public void testRawQueries() {
        if (!supportsLuceneStyleQueries()) return;

        int numV = 1000;
        String[] strs = {"Uncle Berry has a farm","and on his farm he has five ducks","ducks are beautiful animals","the sky is very blue today"};
        setupChainGraph(numV,strs,true);
        clopen();

        assertEquals(numV / strs.length * 2, Iterables.size(graph.indexQuery(VINDEX, "v.text:ducks").vertices()));
        assertEquals(numV / strs.length * 2, Iterables.size(graph.indexQuery(VINDEX, "v.text:(farm uncle berry)").vertices()));
        assertEquals(numV/strs.length,Iterables.size(graph.indexQuery(VINDEX,"v.text:(farm uncle berry) AND v.name:\"Uncle Berry has a farm\"").vertices()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.indexQuery(VINDEX,"v.text:(beautiful are ducks)").vertices()));
        assertEquals(numV / strs.length * 2 - 10, Iterables.size(graph.indexQuery(VINDEX, "v.text:(beautiful are ducks)").offset(10).vertices()));
        assertEquals(10, Iterables.size(graph.indexQuery(VINDEX, "v.\"text\":(beautiful are ducks)").limit(10).vertices()));
        assertEquals(10,Iterables.size(graph.indexQuery(VINDEX,"v.\"text\":(beautiful are ducks)").limit(10).offset(10).vertices()));
        assertEquals(0,Iterables.size(graph.indexQuery(VINDEX,"v.\"text\":(beautiful are ducks)").limit(10).offset(numV).vertices()));
        //Test name mapping
        assertEquals(numV / strs.length * 2, Iterables.size(graph.indexQuery(VINDEX, "xtext:ducks").vertices()));
        assertEquals(0, Iterables.size(graph.indexQuery(VINDEX, "text:ducks").vertices()));
        //Test custom element identifier
        assertEquals(numV / strs.length * 2, Iterables.size(graph.indexQuery(VINDEX, "$v$text:ducks").setElementIdentifier("$v$").vertices()));
        //assertEquals(0, Iterables.size(graph.indexQuery(VINDEX, "v.\"text\":ducks").setElementIdentifier("$v$").vertices()));

        //Same queries for edges
        assertEquals(numV / strs.length * 2, Iterables.size(graph.indexQuery(EINDEX, "e.text:ducks").edges()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.indexQuery(EINDEX,"e.text:(farm uncle berry)").edges()));
        assertEquals(numV / strs.length, Iterables.size(graph.indexQuery(EINDEX, "e.text:(farm uncle berry) AND e.name:\"Uncle Berry has a farm\"").edges()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.indexQuery(EINDEX,"e.text:(beautiful are ducks)").edges()));
        assertEquals(numV / strs.length * 2 - 10, Iterables.size(graph.indexQuery(EINDEX, "e.text:(beautiful are ducks)").offset(10).edges()));
        assertEquals(10, Iterables.size(graph.indexQuery(EINDEX, "e.\"text\":(beautiful are ducks)").limit(10).edges()));
        assertEquals(10, Iterables.size(graph.indexQuery(EINDEX, "e.\"text\":(beautiful are ducks)").limit(10).offset(10).edges()));
        assertEquals(0,Iterables.size(graph.indexQuery(EINDEX,"e.\"text\":(beautiful are ducks)").limit(10).offset(numV).edges()));
        //Test name mapping
        assertEquals(numV / strs.length * 2, Iterables.size(graph.indexQuery(EINDEX, "text:ducks").edges()));

        //Same queries for edges
        assertEquals(numV/strs.length*2,Iterables.size(graph.indexQuery(PINDEX,"p.text:ducks").properties()));
        assertEquals(numV / strs.length * 2, Iterables.size(graph.indexQuery(PINDEX, "p.text:(farm uncle berry)").properties()));
        assertEquals(numV/strs.length,Iterables.size(graph.indexQuery(PINDEX,"p.text:(farm uncle berry) AND p.name:\"Uncle Berry has a farm\"").properties()));
        assertEquals(numV/strs.length*2,Iterables.size(graph.indexQuery(PINDEX,"p.text:(beautiful are ducks)").properties()));
        assertEquals(numV/strs.length*2-10,Iterables.size(graph.indexQuery(PINDEX,"p.text:(beautiful are ducks)").offset(10).properties()));
        assertEquals(10, Iterables.size(graph.indexQuery(PINDEX, "p.\"text\":(beautiful are ducks)").limit(10).properties()));
        assertEquals(10, Iterables.size(graph.indexQuery(PINDEX, "p.\"text\":(beautiful are ducks)").limit(10).offset(10).properties()));
        assertEquals(0,Iterables.size(graph.indexQuery(PINDEX,"p.\"text\":(beautiful are ducks)").limit(10).offset(numV).properties()));
        //Test name mapping
        assertEquals(numV/strs.length*2,Iterables.size(graph.indexQuery(PINDEX,"text:ducks").properties()));
    }

    @Test
    public void testDualMapping() {
        if (!indexFeatures.supportsStringMapping(Mapping.TEXTSTRING)) return;

        PropertyKey name = makeKey("name", String.class);
        TitanGraphIndex mixed = mgmt.buildIndex("mixed", Vertex.class).addKey(name, Mapping.TEXTSTRING.getParameter()).buildMixedIndex(INDEX);
        mixed.name();
        finishSchema();

        Vertex v = tx.addVertex("name","Long John Don");
        v = tx.addVertex("name","Long Little Lewis");

        clopen();
        evaluateQuery(tx.query().has("name",Cmp.EQUAL,"Long John Don"),ElementCategory.VERTEX,
                1,new boolean[]{true,true},"mixed");
        evaluateQuery(tx.query().has("name",Text.CONTAINS,"Long"),ElementCategory.VERTEX,
                2,new boolean[]{true,true},"mixed");
        evaluateQuery(tx.query().has("name",Text.CONTAINS,"Long Don"),ElementCategory.VERTEX,
                1,new boolean[]{true,true},"mixed");
        evaluateQuery(tx.query().has("name",Text.CONTAINS_PREFIX,"Lon"),ElementCategory.VERTEX,
                2,new boolean[]{true,true},"mixed");
        evaluateQuery(tx.query().has("name",Text.CONTAINS_REGEX,"Lit*le"),ElementCategory.VERTEX,
                1,new boolean[]{true,true},"mixed");
        evaluateQuery(tx.query().has("name",Text.REGEX,"Long.*"),ElementCategory.VERTEX,
                2,new boolean[]{true,true},"mixed");

        for (Vertex u : tx.V().toList()) {
            String n = u.<String>value("name");
            if (n.endsWith("Don")) {
                u.remove();
            } else if (n.endsWith("Lewis")) {
                u.singleProperty("name", "Big Brother Bob");
            }
        }

        clopen();

        evaluateQuery(tx.query().has("name",Text.CONTAINS,"Long"),ElementCategory.VERTEX,
                0,new boolean[]{true,true},"mixed");
        evaluateQuery(tx.query().has("name",Text.CONTAINS,"Big"),ElementCategory.VERTEX,
                1,new boolean[]{true,true},"mixed");
        evaluateQuery(tx.query().has("name",Text.PREFIX,"Big"),ElementCategory.VERTEX,
                1,new boolean[]{true,true},"mixed");
    }

    @Test
    public void testIndexReplay() throws Exception {
        final TimestampProvider times = graph.getConfiguration().getTimestampProvider();
        final long startTime = times.getTime().getTimestamp(TimeUnit.MILLISECONDS);
        clopen( option(SYSTEM_LOG_TRANSACTIONS), true
                ,option(KCVSLog.LOG_READ_LAG_TIME,TRANSACTION_LOG),new StandardDuration(50,TimeUnit.MILLISECONDS)
                ,option(LOG_READ_INTERVAL,TRANSACTION_LOG),new StandardDuration(250,TimeUnit.MILLISECONDS)
                ,option(MAX_COMMIT_TIME),new StandardDuration(1,TimeUnit.SECONDS)
                ,option(STORAGE_WRITE_WAITTIME), new StandardDuration(300, TimeUnit.MILLISECONDS)
                ,option(TestMockIndexProvider.INDEX_BACKEND_PROXY,INDEX), adjustedConfig.get(INDEX_BACKEND,INDEX)
                ,option(INDEX_BACKEND,INDEX), TestMockIndexProvider.class.getName()
                ,option(TestMockIndexProvider.INDEX_MOCK_FAILADD,INDEX), true
        );

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).make();
        mgmt.buildIndex("mi",Vertex.class).addKey(name, getTextMapping()).addKey(age).buildMixedIndex(INDEX);
        finishSchema();
        Vertex vs[] = new TitanVertex[4];

        vs[0] = tx.addVertex("name","Big Boy Bobson","age",55);
        newTx();
        vs[1] = tx.addVertex("name","Long Little Lewis","age",35);
        vs[2] = tx.addVertex("name","Tall Long Tiger","age",75);
        vs[3] = tx.addVertex("name","Long John Don","age",15);
        newTx();
        vs[2] = tx.v(vs[2]);
        vs[2].remove();
        vs[3] = tx.v(vs[3]);
        vs[3].singleProperty("name", "Bad Boy Badsy");
        vs[3].property("age").remove();
        newTx();
        vs[0] = tx.v(vs[0]);
        vs[0].singleProperty("age", 66);
        newTx();

        clopen();
        //Just to make sure nothing has been persisted to index
        evaluateQuery(tx.query().has("name",Text.CONTAINS,"boy"),
                ElementCategory.VERTEX,0,new boolean[]{true,true},"mi");
        /*
        Transaction Recovery
         */
        TransactionRecovery recovery = TitanFactory.startTransactionRecovery(graph,startTime,TimeUnit.MILLISECONDS);
        //wait
        Thread.sleep(12000L);

        recovery.shutdown();
        long[] recoveryStats = ((StandardTransactionLogProcessor)recovery).getStatistics();

        clopen();

        evaluateQuery(tx.query().has("name",Text.CONTAINS,"boy"),
                ElementCategory.VERTEX,2,new boolean[]{true,true},"mi");
        evaluateQuery(tx.query().has("name",Text.CONTAINS,"long"),
                ElementCategory.VERTEX,1,new boolean[]{true,true},"mi");
//        Vertex v = Iterables.getOnlyElement(tx.query().has("name",Text.CONTAINS,"long").vertices());
//        System.out.println(v.getProperty("age"));
        evaluateQuery(tx.query().has("name", Text.CONTAINS, "long").interval("age", 30, 40),
                ElementCategory.VERTEX, 1, new boolean[]{true, true}, "mi");
        evaluateQuery(tx.query().has("age",75),
                ElementCategory.VERTEX,0,new boolean[]{true,true},"mi");
        evaluateQuery(tx.query().has("name", Text.CONTAINS, "boy").interval("age", 60, 70),
                ElementCategory.VERTEX,1,new boolean[]{true,true},"mi");
        evaluateQuery(tx.query().interval("age",0,100),
                ElementCategory.VERTEX,2,new boolean[]{true,true},"mi");


        assertEquals(1,recoveryStats[0]); //schema transaction was successful
        assertEquals(4,recoveryStats[1]); //all 4 index transaction had provoked errors in the indexing backend
    }

    @Test
    public void testIndexUpdatesWithoutReindex() throws InterruptedException {
        Object[] settings = new Object[]{option(LOG_SEND_DELAY,MANAGEMENT_LOG),new StandardDuration(0, TimeUnit.MILLISECONDS),
                option(KCVSLog.LOG_READ_LAG_TIME,MANAGEMENT_LOG),new StandardDuration(50,TimeUnit.MILLISECONDS),
                option(LOG_READ_INTERVAL,MANAGEMENT_LOG),new StandardDuration(250,TimeUnit.MILLISECONDS)
        };

        clopen(settings);
        final String defText = "Mountain rocks are great friends";
        final int defTime = 5;
        final double defHeight = 101.1;

        //Creates types and index only one key
        PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).make();
        PropertyKey text = mgmt.makePropertyKey("text").dataType(String.class).make();
        PropertyKey height = mgmt.makePropertyKey("height").dataType(Decimal.class).make();
        TitanGraphIndex index = mgmt.buildIndex("theIndex",Vertex.class)
                .addKey(text, getTextMapping(), getFieldMap(text)).buildMixedIndex(INDEX);
        finishSchema();

        //Add initial data
        addVertex(defTime,defText,defHeight);

        //Indexes should not yet be in use
        clopen(settings);
        evaluateQuery(tx.query().has("text",Text.CONTAINS,"rocks"),
                ElementCategory.VERTEX,1,new boolean[]{true,true},"theIndex");
        evaluateQuery(tx.query().has("time",5),
                ElementCategory.VERTEX,1,new boolean[]{false,true});
        evaluateQuery(tx.query().interval("height",100,200),
                ElementCategory.VERTEX,1,new boolean[]{false,true});
        evaluateQuery(tx.query().interval("height",100,200).has("time",5),
                ElementCategory.VERTEX,1,new boolean[]{false,true});
        evaluateQuery(tx.query().has("text",Text.CONTAINS,"rocks").has("time",5).interval("height",100,200),
                ElementCategory.VERTEX,1,new boolean[]{false,true},"theIndex");
        newTx();

        //Add another key to index ------------------------------------------------------
        finishSchema();
        time = mgmt.getPropertyKey("time");
        mgmt.addIndexKey(mgmt.getGraphIndex("theIndex"),time, getFieldMap(time));
        finishSchema();
        newTx();

        //Add more data
        addVertex(defTime,defText,defHeight);
        tx.commit();
        //Should not yet be able to enable since not yet registered
        try {
            mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.ENABLE_INDEX);
            fail();
        } catch (IllegalArgumentException e) {}
//        mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.REGISTER_INDEX);
        mgmt.commit();

        Thread.sleep(2000);
        finishSchema();
        mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.ENABLE_INDEX);
        finishSchema();

        //Add more data
        addVertex(defTime,defText,defHeight);

        //One more key should be indexed but only sees partial data
        clopen(settings);
        evaluateQuery(tx.query().has("text",Text.CONTAINS,"rocks"),
                ElementCategory.VERTEX,3,new boolean[]{true,true},"theIndex");
        evaluateQuery(tx.query().has("time",5),
                ElementCategory.VERTEX,2,new boolean[]{true,true},"theIndex");
        evaluateQuery(tx.query().interval("height",100,200),
                ElementCategory.VERTEX,3,new boolean[]{false,true});
        evaluateQuery(tx.query().interval("height",100,200).has("time",5),
                ElementCategory.VERTEX,2,new boolean[]{false,true},"theIndex");
        evaluateQuery(tx.query().has("text",Text.CONTAINS,"rocks").has("time",5).interval("height",100,200),
                ElementCategory.VERTEX,2,new boolean[]{false,true},"theIndex");
        newTx();

        //Add another key to index ------------------------------------------------------
        finishSchema();
        height = mgmt.getPropertyKey("height");
        mgmt.addIndexKey(mgmt.getGraphIndex("theIndex"),height);
        finishSchema();

        //Add more data
        addVertex(defTime,defText,defHeight);
        tx.commit();
        mgmt.commit();

        Thread.sleep(2000);
        finishSchema();
        mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.ENABLE_INDEX);
        finishSchema();

        //Add more data
        addVertex(defTime,defText,defHeight);

        //One more key should be indexed but only sees partial data
        clopen(settings);
        evaluateQuery(tx.query().has("text",Text.CONTAINS,"rocks"),
                ElementCategory.VERTEX,5,new boolean[]{true,true},"theIndex");
        evaluateQuery(tx.query().has("time",5),
                ElementCategory.VERTEX,4,new boolean[]{true,true},"theIndex");
        evaluateQuery(tx.query().interval("height",100,200),
                ElementCategory.VERTEX,2,new boolean[]{true,true},"theIndex");
        evaluateQuery(tx.query().interval("height",100,200).has("time",5),
                ElementCategory.VERTEX,2,new boolean[]{true,true},"theIndex");
        evaluateQuery(tx.query().has("text",Text.CONTAINS,"rocks").has("time",5).interval("height",100,200),
                ElementCategory.VERTEX,2,new boolean[]{true,true},"theIndex");
        newTx();
    }

    private void addVertex(int time, String text, double height) {
        newTx();
        Vertex v = tx.addVertex("text",text,"time",time,"height",height);
        newTx();
    }



   /* ==================================================================================
                                     TIME-TO-LIVE
     ==================================================================================*/

    @Test
    public void testVertexTTLWithMixedIndices() throws Exception {
        if (!features.hasCellTTL() || !indexFeatures.supportsDocumentTTL()) {
            return;
        }

        PropertyKey name = makeKey("name", String.class);
        PropertyKey time = makeKey("time", Long.class);
        PropertyKey text = makeKey("text", String.class);

        VertexLabel event = mgmt.makeVertexLabel("event").setStatic().make();
        final int eventTTLSeconds = (int)TestGraphConfigs.getTTL(TimeUnit.SECONDS);
        mgmt.setTTL(event, eventTTLSeconds, TimeUnit.SECONDS);

        mgmt.buildIndex("index1",Vertex.class).
                addKey(name, getStringMapping()).addKey(time).buildMixedIndex(INDEX);
        mgmt.buildIndex("index2",Vertex.class).indexOnly(event).
                addKey(text, getTextMapping()).buildMixedIndex(INDEX);

        assertEquals(0, mgmt.getTTL(name).getLength(TimeUnit.SECONDS));
        assertEquals(0, mgmt.getTTL(time).getLength(TimeUnit.SECONDS));
        assertEquals(eventTTLSeconds, mgmt.getTTL(event).getLength(TimeUnit.SECONDS));
        finishSchema();

        Vertex v1 = tx.addVertex("event");
        v1.singleProperty("name", "first event");
        v1.singleProperty("text", "this text will help to identify the first event");
        long time1 = System.currentTimeMillis();
        v1.singleProperty("time", time1);
        Vertex v2 = tx.addVertex("event");
        v2.singleProperty("name", "second event");
        v2.singleProperty("text", "this text won't match");
        long time2 = time1 + 1;
        v2.singleProperty("time", time2);

        evaluateQuery(tx.query().has("name","first event").orderBy("time",Order.DESC),
                ElementCategory.VERTEX,1,new boolean[]{true,true}, tx.getPropertyKey("time"), Order.DESC,"index1");
        evaluateQuery(tx.query().has("text",Text.CONTAINS,"help").has("label","event"),
                ElementCategory.VERTEX,1,new boolean[]{true,true},"index2");

        clopen();

        Object v1Id = v1.id();
        Object v2Id = v2.id();

        evaluateQuery(tx.query().has("name","first event").orderBy("time",Order.DESC),
                ElementCategory.VERTEX,1,new boolean[]{true,true}, tx.getPropertyKey("time"), Order.DESC,"index1");
        evaluateQuery(tx.query().has("text",Text.CONTAINS,"help").has("label","event"),
                ElementCategory.VERTEX,1,new boolean[]{true,true},"index2");

        v1 = tx.v(v1Id);
        v2 = tx.v(v1Id);
        assertNotNull(v1);
        assertNotNull(v2);

        Thread.sleep(TimeUnit.MILLISECONDS.convert((long)Math.ceil(eventTTLSeconds * 1.25), TimeUnit.SECONDS));

        clopen();

        evaluateQuery(tx.query().has("text",Text.CONTAINS,"help").has("label","event"),
                ElementCategory.VERTEX,0,new boolean[]{true,true},"index2");
        evaluateQuery(tx.query().has("name","first event").orderBy("time",Order.DESC),
                ElementCategory.VERTEX,0,new boolean[]{true,true}, tx.getPropertyKey("time"), Order.DESC,"index1");


        v1 = tx.v(v1Id);
        v2 = tx.v(v2Id);
        assertNull(v1);
        assertNull(v2);
    }

    @Test
    public void testEdgeTTLWithMixedIndices() throws Exception {
        if (!features.hasCellTTL() || !indexFeatures.supportsDocumentTTL()) {
            return;
        }

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        PropertyKey text = mgmt.makePropertyKey("text").dataType(String.class).make();
        PropertyKey time = makeKey("time", Long.class);

        EdgeLabel label = mgmt.makeEdgeLabel("likes").make();
        final int likesTTLSeconds = (int)TestGraphConfigs.getTTL(TimeUnit.SECONDS);
        mgmt.setTTL(label, likesTTLSeconds, TimeUnit.SECONDS);

        mgmt.buildIndex("index1",Edge.class).
                addKey(name, getStringMapping()).addKey(time).buildMixedIndex(INDEX);
        mgmt.buildIndex("index2",Edge.class).indexOnly(label).
                addKey(text, getTextMapping()).buildMixedIndex(INDEX);

        assertEquals(0, mgmt.getTTL(name).getLength(TimeUnit.SECONDS));
        assertEquals(likesTTLSeconds, mgmt.getTTL(label).getLength(TimeUnit.SECONDS));
        finishSchema();

        Vertex v1 = tx.addVertex(), v2 = tx.addVertex(), v3 = tx.addVertex();

        Edge e1 = v1.addEdge("likes", v2,"name", "v1 likes v2", "text", "this will help to identify the edge");
        long time1 = System.currentTimeMillis();
        e1.property("time", time1);
        Edge e2 = v2.addEdge("likes", v3, "name", "v2 likes v3", "text", "this won't match anything");
        long time2 = time1 + 1;
        e2.property("time", time2);

        tx.commit();

        clopen();
        Object e1Id = e1.id();
        Object e2Id = e2.id();

        evaluateQuery(tx.query().has("text",Text.CONTAINS,"help").has("label","likes"),
                ElementCategory.EDGE,1,new boolean[]{true,true},"index2");
        evaluateQuery(tx.query().has("name","v2 likes v3").orderBy("time",Order.DESC),
                ElementCategory.EDGE,1,new boolean[]{true,true}, tx.getPropertyKey("time"), Order.DESC,"index1");
        v1 = tx.v(v1.id());
        v2 = tx.v(v2.id());
        v3 = tx.v(v3.id());
        e1 = tx.e(e1Id);
        e2 = tx.e(e1Id);
        assertNotNull(v1);
        assertNotNull(v2);
        assertNotNull(v3);
        assertNotNull(e1);
        assertNotNull(e2);
        assertNotEmpty(v1.outE());
        assertNotEmpty(v2.outE());


        Thread.sleep(TimeUnit.MILLISECONDS.convert((long)Math.ceil(likesTTLSeconds * 1.25), TimeUnit.SECONDS));
        clopen();

        // ...indexes have expired
        evaluateQuery(tx.query().has("text",Text.CONTAINS,"help").has("label","likes"),
                ElementCategory.EDGE,0,new boolean[]{true,true},"index2");
        evaluateQuery(tx.query().has("name","v2 likes v3").orderBy("time",Order.DESC),
                ElementCategory.EDGE,0,new boolean[]{true,true}, tx.getPropertyKey("time"), Order.DESC,"index1");

        v1 = tx.v(v1.id());
        v2 = tx.v(v2.id());
        v3 = tx.v(v3.id());
        e1 = tx.e(e1Id);
        e2 = tx.e(e1Id);
        assertNotNull(v1);
        assertNotNull(v2);
        assertNotNull(v3);
        // edges have expired from the graph...
        assertNull(e1);
        assertNull(e2);
        assertEmpty(v1.outE());
        assertEmpty(v2.outE());
    }

   /* ==================================================================================
                            SPECIAL CONCURRENT UPDATE CASES
     ==================================================================================*/

    /**
     * Create a vertex with an indexed property and commit. Open two new
     * transactions; delete vertex in one and delete just the property in the
     * other, then commit in the same order. Neither commit throws an exception.
     */
    @Test
    public void testDeleteVertexThenDeleteProperty() throws BackendException {
        testNestedWrites("x", null);
    }

    /**
     * Create a vertex and commit. Open two new transactions; delete vertex in
     * one and add an indexed property in the other, then commit in the same
     * order. Neither commit throws an exception.
     */
    @Test
    public void testDeleteVertexThenAddProperty() throws BackendException {
        testNestedWrites(null, "y");
    }

    /**
     * Create a vertex with an indexed property and commit. Open two new
     * transactions; delete vertex in one and modify the property in the other,
     * then commit in the same order. Neither commit throws an exception.
     */
    @Test
    public void testDeleteVertexThenModifyProperty() throws BackendException {
        testNestedWrites("x", "y");
    }

    private void testNestedWrites(String initialValue, String updatedValue) throws BackendException {
        // This method touches a single vertex with multiple transactions,
        // leading to deadlock under BDB and cascading test failures. Check for
        // the hasTxIsolation() store feature, which is currently true for BDB
        // but false for HBase/Cassandra. This is kind of a hack; a more robust
        // approach might implement different methods/assertions depending on
        // whether the store is capable of deadlocking or detecting conflicting
        // writes and aborting a transaction.
        Backend b = null;
        try {
            b = graph.getConfiguration().getBackend();
            if (b.getStoreFeatures().hasTxIsolation()) {
                log.info("Skipping "  + getClass().getSimpleName() + "." + methodName.getMethodName());
                return;
            }
        } finally {
            if (null != b)
                b.close();
        }

        final String propName = "foo";

        // Write schema and one vertex
        PropertyKey prop = makeKey(propName, String.class);
        createExternalVertexIndex(prop, INDEX);
        finishSchema();

        Vertex v = graph.addVertex();
        if (null != initialValue)
            v.singleProperty(propName, initialValue);
        graph.tx().commit();

        Object id = v.id();

        // Open two transactions and modify the same vertex
        TitanTransaction vertexDeleter = graph.newTransaction();
        TitanTransaction propDeleter = graph.newTransaction();

        vertexDeleter.v(id).remove();
        if (null == updatedValue)
            propDeleter.v(id).property(propName).remove();
        else
            propDeleter.v(id).singleProperty(propName,updatedValue);

        vertexDeleter.commit();
        propDeleter.commit();

        // The vertex must not exist after deletion
        graph.tx().rollback();
        assertEquals(null, graph.v(id));
        assertEmpty(graph.V().has(propName));
        if (null != updatedValue)
            assertEmpty(graph.V().has(propName, updatedValue));
        graph.tx().rollback();
    }
}
