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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphQuery;
import org.janusgraph.core.JanusGraphIndexQuery;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.log.TransactionRecovery;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.util.ManagementUtil;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.indexing.IndexFeatures;
import org.janusgraph.diskstorage.indexing.IndexInformation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.log.StandardTransactionLogProcessor;
import org.janusgraph.graphdb.types.ParameterType;
import org.janusgraph.graphdb.types.StandardEdgeLabelMaker;
import org.janusgraph.testcategory.BrittleTests;
import org.janusgraph.testutil.TestGraphConfigs;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.janusgraph.graphdb.JanusGraphTest.evaluateQuery;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;
import static org.janusgraph.testutil.JanusGraphAssert.*;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.decr;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.incr;
import static org.junit.Assert.*;


/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class JanusGraphIndexTest extends JanusGraphBaseTest {

    public static final String INDEX = GraphOfTheGodsFactory.INDEX_NAME;
    public static final String VINDEX = "v" + INDEX;
    public static final String EINDEX = "e" + INDEX;
    public static final String PINDEX = "p" + INDEX;

    private static final int RETRY_COUNT = 30;
    private static final long RETRY_INTERVAL = 1000l;

    public final boolean supportsGeoPoint;
    public final boolean supportsNumeric;
    public final boolean supportsText;

    public IndexFeatures indexFeatures;

    private static final Logger log =
            LoggerFactory.getLogger(JanusGraphIndexTest.class);

    protected JanusGraphIndexTest(boolean supportsGeoPoint, boolean supportsNumeric, boolean supportsText) {
        this.supportsGeoPoint = supportsGeoPoint;
        this.supportsNumeric = supportsNumeric;
        this.supportsText = supportsText;
    }

    private Parameter getStringMapping() {
        if (indexFeatures.supportsStringMapping(Mapping.STRING)) return Mapping.STRING.asParameter();
        else if (indexFeatures.supportsStringMapping(Mapping.TEXTSTRING)) return Mapping.TEXTSTRING.asParameter();
        throw new AssertionError("String mapping not supported");
    }

    private Parameter getTextMapping() {
        if (indexFeatures.supportsStringMapping(Mapping.TEXT)) return Mapping.TEXT.asParameter();
        else if (indexFeatures.supportsStringMapping(Mapping.TEXTSTRING)) return Mapping.TEXTSTRING.asParameter();
        throw new AssertionError("Text mapping not supported");
    }

    private Parameter getFieldMap(PropertyKey key) {
        return ParameterType.MAPPED_NAME.getParameter(key.name());
    }

    public abstract boolean supportsLuceneStyleQueries();


    public abstract boolean supportsWildcardQuery();

    @Override
    public void open(WriteConfiguration config) {
        super.open(config);
        indexFeatures = graph.getBackend().getIndexFeatures().get(INDEX);
    }

    @Override
    public void clopen(Object... settings) {
        graph.tx().commit();
        super.clopen(settings);
    }

    @Rule
    public TestName methodName = new TestName();

    /**
     * Tests the {@link org.janusgraph.example.GraphOfTheGodsFactory#load(org.janusgraph.core.JanusGraph)}
     * method used as the standard example that ships with JanusGraph.
     */
    @Test
    public void testGraphOfTheGods() {
        GraphOfTheGodsFactory.load(graph);
        assertGraphOfTheGods(graph);
    }

    public static void assertGraphOfTheGods(JanusGraph gotg) {
        assertCount(12, gotg.query().vertices());
        assertCount(3, gotg.query().has(LABEL_NAME, "god").vertices());
        JanusGraphVertex h = getOnlyVertex(gotg.query().has("name", "hercules"));
        assertEquals(30, h.<Integer>value("age").intValue());
        assertEquals("demigod", h.label());
        assertCount(5, h.query().direction(Direction.BOTH).edges());
        gotg.tx().commit();
    }

    /**
     * Ensure clearing storage actually removes underlying graph and index databases.
     * @throws Exception
     */
    @Test
    public void testClearStorage() throws Exception {
        GraphOfTheGodsFactory.load(graph);
        tearDown();
        config.set(ConfigElement.getPath(GraphDatabaseConfiguration.DROP_ON_CLEAR), true);
        final Backend backend = getBackend(config, false);
        assertStorageExists(backend, true);
        clearGraph(config);
        try { backend.close(); } catch (Exception e) { /* Most backends do not support closing after clearing */}
        try (final Backend newBackend = getBackend(config, false)) {
            assertStorageExists(newBackend, false);
        }
    }

    private static void assertStorageExists(Backend backend, boolean exists) throws Exception {
        final String suffix = exists ? "should exist before clearing" : "should not exist after clearing";
        assertTrue("graph " + suffix, backend.getStoreManager().exists() == exists);
        for (final IndexInformation index : backend.getIndexInformation().values()) {
            assertTrue("index " + suffix, ((IndexProvider) index).exists() == exists);
        }
    }

    @Test
    public void testSimpleUpdate() {
        PropertyKey name = makeKey("name", String.class);
        makeLabel("knows");
        mgmt.buildIndex("namev", Vertex.class).addKey(name).buildMixedIndex(INDEX);
        mgmt.buildIndex("namee", Edge.class).addKey(name).buildMixedIndex(INDEX);
        finishSchema();

        JanusGraphVertex v = tx.addVertex("name", "Marko Rodriguez");
        Edge e = v.addEdge("knows", v, "name", "Hulu Bubab");
        assertCount(1, tx.query().has("name", Text.CONTAINS, "marko").vertices());
        assertCount(1, tx.query().has("name", Text.CONTAINS, "Hulu").edges());
        for (Vertex u : tx.getVertices()) assertEquals("Marko Rodriguez", u.value("name"));
        clopen();
        assertCount(1, tx.query().has("name", Text.CONTAINS, "marko").vertices());
        assertCount(1, tx.query().has("name", Text.CONTAINS, "Hulu").edges());
        for (Vertex u : tx.getVertices()) assertEquals("Marko Rodriguez", u.value("name"));
        v = getOnlyVertex(tx.query().has("name", Text.CONTAINS, "marko"));
        v.property(VertexProperty.Cardinality.single, "name", "Marko");
        e = getOnlyEdge(v.query().direction(Direction.OUT));
        e.property("name", "Tubu Rubu");
        assertCount(1, tx.query().has("name", Text.CONTAINS, "marko").vertices());
        assertCount(1, tx.query().has("name", Text.CONTAINS, "Rubu").edges());
        assertCount(0, tx.query().has("name", Text.CONTAINS, "Hulu").edges());
        for (Vertex u : tx.getVertices()) assertEquals("Marko", u.value("name"));
        clopen();
        assertCount(1, tx.query().has("name", Text.CONTAINS, "marko").vertices());
        assertCount(1, tx.query().has("name", Text.CONTAINS, "Rubu").edges());
        assertCount(0, tx.query().has("name", Text.CONTAINS, "Hulu").edges());
        for (Vertex u : tx.getVertices()) assertEquals("Marko", u.value("name"));
    }

    @Test
    public void testListUpdate() {
        if (!indexFeatures.supportsCardinality(Cardinality.LIST)) {
            return;
        }
        PropertyKey name = makeKey("name", String.class);
        if (!indexFeatures.supportsStringMapping(Mapping.TEXTSTRING)) {
            
        }
        PropertyKey alias = mgmt.makePropertyKey("alias") .dataType(String.class).cardinality(Cardinality.LIST).make();
        mgmt.buildIndex("namev", Vertex.class).addKey(name).addKey(alias, indexFeatures.supportsStringMapping(Mapping.TEXTSTRING) ?Mapping.TEXTSTRING.asParameter(): Mapping.DEFAULT.asParameter()).buildMixedIndex(INDEX);
        finishSchema();
        JanusGraphVertex v = tx.addVertex("name", "Marko Rodriguez");
        assertCount(1, tx.query().has("name", Text.CONTAINS, "marko").vertices());
        clopen();
        assertCount(1, tx.query().has("name", Text.CONTAINS, "marko").vertices());
        v = getOnlyVertex(tx.query().has("name", Text.CONTAINS, "marko"));
        v.property(VertexProperty.Cardinality.list, "alias", "Marko");
        assertCount(1, tx.query().has("alias", Text.CONTAINS, "Marko").vertices());
        clopen();
        assertCount(1, tx.query().has("alias", Text.CONTAINS, "Marko").vertices());
        v = getOnlyVertex(tx.query().has("name", Text.CONTAINS, "marko"));
        v.property(VertexProperty.Cardinality.list, "alias", "mRodriguez");
        assertCount(1, tx.query().has("alias", Text.CONTAINS, "mRodriguez").vertices());
        clopen();
        assertCount(1, tx.query().has("alias", Text.CONTAINS, "Marko").vertices());
        assertCount(1, tx.query().has("alias", Text.CONTAINS, "mRodriguez").vertices());
        if (indexFeatures.supportsStringMapping(Mapping.TEXTSTRING)) {
            assertCount(1, tx.query().has("alias", Cmp.EQUAL, "Marko").vertices());
            assertCount(1, tx.query().has("alias", Cmp.EQUAL, "mRodriguez").vertices());
        }
    }
    
    @Test
    public void testSetUpdate() {
        if (!indexFeatures.supportsCardinality(Cardinality.SET)) {
            return;
        }
        PropertyKey name = makeKey("name", String.class);
        PropertyKey alias = mgmt.makePropertyKey("alias").dataType(String.class).cardinality(Cardinality.SET).make();
        mgmt.buildIndex("namev", Vertex.class).addKey(name).addKey(alias, indexFeatures.supportsStringMapping(Mapping.TEXTSTRING) ?Mapping.TEXTSTRING.asParameter(): Mapping.DEFAULT.asParameter()).buildMixedIndex(INDEX);
        finishSchema();
        JanusGraphVertex v = tx.addVertex("name", "Marko Rodriguez");
        assertCount(1, tx.query().has("name", Text.CONTAINS, "marko").vertices());
        clopen();
        assertCount(1, tx.query().has("name", Text.CONTAINS, "marko").vertices());
        v = getOnlyVertex(tx.query().has("name", Text.CONTAINS, "marko"));
        v.property(VertexProperty.Cardinality.set, "alias", "Marko");
        assertCount(1, tx.query().has("alias", Text.CONTAINS, "Marko").vertices());
        clopen();
        assertCount(1, tx.query().has("alias", Text.CONTAINS, "Marko").vertices());
        v = getOnlyVertex(tx.query().has("name", Text.CONTAINS, "marko"));
        v.property(VertexProperty.Cardinality.set, "alias", "mRodriguez");
        assertCount(1, tx.query().has("alias", Text.CONTAINS, "mRodriguez").vertices());
        clopen();
        assertCount(1, tx.query().has("alias", Text.CONTAINS, "Marko").vertices());
        assertCount(1, tx.query().has("alias", Text.CONTAINS, "mRodriguez").vertices());
        if (indexFeatures.supportsStringMapping(Mapping.TEXTSTRING)) {
            assertCount(1, tx.query().has("alias", Cmp.EQUAL, "Marko").vertices());
            assertCount(1, tx.query().has("alias", Cmp.EQUAL, "mRodriguez").vertices());
        }
    }


    @Test
    public void testIndexing() throws InterruptedException {

        PropertyKey text = makeKey("text", String.class);
        createExternalVertexIndex(text, INDEX);
        createExternalEdgeIndex(text, INDEX);

        PropertyKey location = makeKey("location", Geoshape.class);
        createExternalVertexIndex(location, INDEX);
        createExternalEdgeIndex(location, INDEX);

        PropertyKey boundary = makeKey("boundary", Geoshape.class);
        mgmt.addIndexKey(getExternalIndex(Vertex.class,INDEX),boundary, Parameter.of("mapping", Mapping.PREFIX_TREE), Parameter.of("index-geo-dist-error-pct", 0.0025));
        mgmt.addIndexKey(getExternalIndex(Edge.class,INDEX),boundary, Parameter.of("mapping", Mapping.PREFIX_TREE), Parameter.of("index-geo-dist-error-pct", 0.0025));

        PropertyKey time = makeKey("time", Long.class);
        createExternalVertexIndex(time, INDEX);
        createExternalEdgeIndex(time, INDEX);

        PropertyKey category = makeKey("category", Integer.class);
        mgmt.buildIndex("vcategory", Vertex.class).addKey(category).buildCompositeIndex();
        mgmt.buildIndex("ecategory", Edge.class).addKey(category).buildCompositeIndex();

        PropertyKey group = makeKey("group", Byte.class);
        createExternalVertexIndex(group, INDEX);
        createExternalEdgeIndex(group, INDEX);

        makeVertexIndexedKey("uid", Integer.class);
        ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("knows")).sortKey(time).signature(location,boundary).make();
        finishSchema();

        clopen();
        String[] words = {"world", "aurelius", "janusgraph", "graph"};
        int numCategories = 5;
        int numGroups = 10;
        double distance, offset;
        int numV = 100;
        final int originalNumV = numV;
        for (int i = 0; i < numV; i++) {
            JanusGraphVertex v = tx.addVertex();
            v.property(VertexProperty.Cardinality.single, "uid", i);
            v.property(VertexProperty.Cardinality.single, "category", i % numCategories);
            v.property(VertexProperty.Cardinality.single, "group", i % numGroups);
            v.property(VertexProperty.Cardinality.single, "text", "Vertex " + words[i % words.length]);
            v.property(VertexProperty.Cardinality.single, "time", i);
            offset = (i % 2 == 0 ? 1 : -1) * (i * 50.0 / numV);
            v.property(VertexProperty.Cardinality.single, "location", Geoshape.point(0.0 + offset, 0.0 + offset));
            if (i % 2 == 0) {
                v.property(VertexProperty.Cardinality.single, "boundary", Geoshape.line(Arrays.asList(new double[][] {
                        {offset-0.1, offset-0.1}, {offset+0.1, offset-0.1}, {offset+0.1, offset+0.1}, {offset-0.1, offset+0.1}})));
            } else {
                v.property(VertexProperty.Cardinality.single, "boundary", Geoshape.polygon(Arrays.asList(new double[][]
                        {{offset-0.1,offset-0.1},{offset+0.1,offset-0.1},{offset+0.1,offset+0.1},{offset-0.1,offset+0.1},{offset-0.1,offset-0.1}})));
            }
            Edge e = v.addEdge("knows", getVertex("uid", Math.max(0, i - 1)));
            e.property("text", "Vertex " + words[i % words.length]);
            e.property("time", i);
            e.property("category", i % numCategories);
            e.property("group", i % numGroups);
            e.property("location", Geoshape.point(0.0 + offset, 0.0 + offset));
            if (i % 2 == 0) {
                e.property("boundary", Geoshape.line(Arrays.asList(new double[][] {
                        {offset-0.1, offset-0.1}, {offset+0.1, offset-0.1}, {offset+0.1, offset+0.1}, {offset-0.1, offset+0.1}})));
            } else {
                e.property("boundary", Geoshape.polygon(Arrays.asList(new double[][]
                        {{offset-0.1,offset-0.1},{offset+0.1,offset-0.1},{offset+0.1,offset+0.1},{offset-0.1,offset+0.1},{offset-0.1,offset-0.1}})));
            }
        }

        checkIndexingCounts(words, numV, originalNumV, true);

        //--------------

        // some indexing backends may guarantee only eventual consistency
        for (int retry = 0, status = 1; retry < RETRY_COUNT && status > 0; retry++) {
            clopen();
            try {
                checkIndexingCounts(words, numV, originalNumV, true);
                status = 0;
            } catch (AssertionError e) {
                if (retry >= RETRY_COUNT-1) throw e;
                Thread.sleep(RETRY_INTERVAL);
            }
        }

        newTx();

        int numDelete = 12;
        for (int i = numV - numDelete; i < numV; i++) {
            getVertex("uid", i).remove();
        }

        numV -= numDelete;

        checkIndexingCounts(words, numV, originalNumV, false);
    }

    private void checkIndexingCounts(String[] words, int numV, int originalNumV, boolean checkOrder) {
        for (int i = 0; i < words.length; i++) {
            int expectedSize = numV / words.length;
            assertCount(expectedSize, tx.query().has("text", Text.CONTAINS, words[i]).vertices());
            assertCount(expectedSize, tx.query().has("text", Text.CONTAINS, words[i]).edges());

            //Test ordering
            if (checkOrder) {
                for (String orderKey : new String[]{"time", "category"}) {
                    for (Order order : Order.values()) {
                        for (JanusGraphQuery traversal : ImmutableList.of(
                            tx.query().has("text", Text.CONTAINS, words[i]).orderBy(orderKey, order.getTP()),
                            tx.query().has("text", Text.CONTAINS, words[i]).orderBy(orderKey, order.getTP())
                        )) {
                            verifyElementOrder(traversal.vertices(), orderKey, order, expectedSize);
                        }
                    }
                }
            }
        }

        assertCount(3, tx.query().has("group", 3).orderBy("time", incr).limit(3).vertices());
        assertCount(3, tx.query().has("group", 3).orderBy("time", decr).limit(3).edges());

        for (int i = 0; i < numV / 2; i += numV / 10) {
            assertCount(i, tx.query().has("time", Cmp.GREATER_THAN_EQUAL, i).has("time", Cmp.LESS_THAN, i + i).vertices());
            assertCount(i, tx.query().has("time", Cmp.GREATER_THAN_EQUAL, i).has("time", Cmp.LESS_THAN, i + i).edges());
        }

        for (int i = 0; i < numV; i += 5) {
            testGeo(i, originalNumV, numV, "location", "boundary");
        }

        //Queries combining mixed and composite indexes
        assertCount(4, tx.query().has("category", 1).interval("time", 10, 28).vertices());
        assertCount(4, tx.query().has("category", 1).interval("time", 10, 28).edges());

        assertCount(5, tx.query().has("time", Cmp.GREATER_THAN_EQUAL, 10).has("time", Cmp.LESS_THAN, 30).has("text", Text.CONTAINS, words[0]).vertices());
        double offset = (19 * 50.0 / originalNumV);
        double distance = Geoshape.point(0.0, 0.0).getPoint().distance(Geoshape.point(offset, offset).getPoint()) + 20;
        assertCount(5, tx.query().has("location", Geo.INTERSECT, Geoshape.circle(0.0, 0.0, distance)).has("text", Text.CONTAINS, words[0]).vertices());
        assertCount(5, tx.query().has("boundary", Geo.INTERSECT, Geoshape.circle(0.0, 0.0, distance)).has("text", Text.CONTAINS, words[0]).vertices());

        assertCount(numV, tx.query().vertices());
        assertCount(numV, tx.query().edges());
    }

    /**
     * Tests indexing boolean
     */
    @Test
    public void testBooleanIndexing() {
        PropertyKey name = makeKey("visible", Boolean.class);
        mgmt.buildIndex("booleanIndex", Vertex.class).
                addKey(name).buildMixedIndex(INDEX);
        finishSchema();
        clopen();

        JanusGraphVertex v1 = graph.addVertex();
        v1.property("visible", true);

        JanusGraphVertex v2 = graph.addVertex();
        v2.property("visible", false);

        assertCount(2, graph.vertices());
        assertEquals(v1, getOnlyVertex(graph.query().has("visible", true)));
        assertEquals(v2, getOnlyVertex(graph.query().has("visible", false)));
        assertEquals(v2, getOnlyVertex(graph.query().has("visible", Cmp.NOT_EQUAL, true)));
        assertEquals(v1, getOnlyVertex(graph.query().has("visible", Cmp.NOT_EQUAL, false)));

        clopen();//Flush the index
        assertCount(2, graph.vertices());
        assertEquals(v1, getOnlyVertex(graph.query().has("visible", true)));
        assertEquals(v2, getOnlyVertex(graph.query().has("visible", false)));
        assertEquals(v2, getOnlyVertex(graph.query().has("visible", Cmp.NOT_EQUAL, true)));
        assertEquals(v1, getOnlyVertex(graph.query().has("visible", Cmp.NOT_EQUAL, false)));
    }


    /**
     * Tests indexing dates
     */
    @Test
    public void testDateIndexing() {
        PropertyKey name = makeKey("date", Date.class);
        mgmt.buildIndex("dateIndex", Vertex.class).
                addKey(name).buildMixedIndex(INDEX);
        finishSchema();
        clopen();

        JanusGraphVertex v1 = graph.addVertex();
        v1.property("date", new Date(1));

        JanusGraphVertex v2 = graph.addVertex();
        v2.property("date", new Date(2000));


        assertEquals(v1, getOnlyVertex(graph.query().has("date", Cmp.EQUAL, new Date(1))));
        assertEquals(v2, getOnlyVertex(graph.query().has("date", Cmp.GREATER_THAN, new Date(1))));
        assertEquals(Sets.newHashSet(v1, v2), Sets.newHashSet(graph.query().has("date", Cmp.GREATER_THAN_EQUAL, new Date(1)).vertices()));
        assertEquals(v1, getOnlyVertex(graph.query().has("date", Cmp.LESS_THAN, new Date(2000))));
        assertEquals(Sets.newHashSet(v1, v2), Sets.newHashSet(graph.query().has("date", Cmp.LESS_THAN_EQUAL, new Date(2000)).vertices()));
        assertEquals(v2, getOnlyVertex(graph.query().has("date", Cmp.NOT_EQUAL, new Date(1))));

        clopen();//Flush the index
        assertEquals(v1, getOnlyVertex(graph.query().has("date", Cmp.EQUAL, new Date(1))));
        assertEquals(v2, getOnlyVertex(graph.query().has("date", Cmp.GREATER_THAN, new Date(1))));
        assertEquals(Sets.newHashSet(v1, v2), Sets.newHashSet(graph.query().has("date", Cmp.GREATER_THAN_EQUAL, new Date(1)).vertices()));
        assertEquals(v1, getOnlyVertex(graph.query().has("date", Cmp.LESS_THAN, new Date(2000))));
        assertEquals(Sets.newHashSet(v1, v2), Sets.newHashSet(graph.query().has("date", Cmp.LESS_THAN_EQUAL, new Date(2000)).vertices()));
        assertEquals(v2, getOnlyVertex(graph.query().has("date", Cmp.NOT_EQUAL, new Date(1))));


    }


    /**
     * Tests indexing instants
     */
    @Test
    public void testInstantIndexing() {
        PropertyKey name = makeKey("instant", Instant.class);
        mgmt.buildIndex("instantIndex", Vertex.class).
                addKey(name).buildMixedIndex(INDEX);
        finishSchema();
        clopen();
        Instant firstTimestamp = Instant.ofEpochMilli(1);
        Instant secondTimestamp = Instant.ofEpochMilli(2000);

        JanusGraphVertex v1 = graph.addVertex();
        v1.property("instant", firstTimestamp);

        JanusGraphVertex v2 = graph.addVertex();
        v2.property("instant", secondTimestamp);

        testInstant(firstTimestamp, secondTimestamp, v1, v2);

        firstTimestamp = Instant.ofEpochSecond(0, 1);
        v1 = (JanusGraphVertex) graph.vertices(v1.id()).next();
        v1.property("instant", firstTimestamp);
        if (indexFeatures.supportsNanoseconds()) {
            testInstant(firstTimestamp, secondTimestamp, v1, v2);
        } else {
            clopen();//Flush the index
            try {
                assertEquals(v1, getOnlyVertex(graph.query().has("instant", Cmp.EQUAL, firstTimestamp)));
                Assert.fail("Should have failed to update the index");
            } catch (Exception e) {

            }
        }

    }

    private void testInstant(Instant firstTimestamp, Instant secondTimestamp, JanusGraphVertex v1, JanusGraphVertex v2) {
        assertEquals(v1, getOnlyVertex(graph.query().has("instant", Cmp.EQUAL, firstTimestamp)));
        assertEquals(v2, getOnlyVertex(graph.query().has("instant", Cmp.GREATER_THAN, firstTimestamp)));
        assertEquals(Sets.newHashSet(v1, v2), Sets.newHashSet(graph.query().has("instant", Cmp.GREATER_THAN_EQUAL, firstTimestamp).vertices()));
        assertEquals(v1, getOnlyVertex(graph.query().has("instant", Cmp.LESS_THAN, secondTimestamp)));
        assertEquals(Sets.newHashSet(v1, v2), Sets.newHashSet(graph.query().has("instant", Cmp.LESS_THAN_EQUAL, secondTimestamp).vertices()));
        assertEquals(v2, getOnlyVertex(graph.query().has("instant", Cmp.NOT_EQUAL, firstTimestamp)));


        clopen();//Flush the index
        assertEquals(v1, getOnlyVertex(graph.query().has("instant", Cmp.EQUAL, firstTimestamp)));
        assertEquals(v2, getOnlyVertex(graph.query().has("instant", Cmp.GREATER_THAN, firstTimestamp)));
        assertEquals(Sets.newHashSet(v1, v2), Sets.newHashSet(graph.query().has("instant", Cmp.GREATER_THAN_EQUAL, firstTimestamp).vertices()));
        assertEquals(v1, getOnlyVertex(graph.query().has("instant", Cmp.LESS_THAN, secondTimestamp)));
        assertEquals(Sets.newHashSet(v1, v2), Sets.newHashSet(graph.query().has("instant", Cmp.LESS_THAN_EQUAL, secondTimestamp).vertices()));
        assertEquals(v2, getOnlyVertex(graph.query().has("instant", Cmp.NOT_EQUAL, firstTimestamp)));
    }

    /**
     * Tests indexing boolean
     */
    @Test
    public void testUUIDIndexing() {
        PropertyKey name = makeKey("uid", UUID.class);
        mgmt.buildIndex("uuidIndex", Vertex.class).
                addKey(name).buildMixedIndex(INDEX);
        finishSchema();
        clopen();

        UUID uid1 = UUID.randomUUID();
        UUID uid2 = UUID.randomUUID();

        JanusGraphVertex v1 = graph.addVertex();
        v1.property("uid", uid1);

        JanusGraphVertex v2 = graph.addVertex();
        v2.property("uid", uid2);

        assertCount(2, graph.query().vertices());
        assertEquals(v1, getOnlyVertex(graph.query().has("uid", uid1)));
        assertEquals(v2, getOnlyVertex(graph.query().has("uid", uid2)));

        assertEquals(v2, getOnlyVertex(graph.query().has("uid", Cmp.NOT_EQUAL, uid1)));
        assertEquals(v1, getOnlyVertex(graph.query().has("uid", Cmp.NOT_EQUAL, uid2)));

        clopen();//Flush the index
        assertCount(2, graph.query().vertices());
        assertEquals(v1, getOnlyVertex(graph.query().has("uid", uid1)));
        assertEquals(v2, getOnlyVertex(graph.query().has("uid", uid2)));

        assertEquals(v2, getOnlyVertex(graph.query().has("uid", Cmp.NOT_EQUAL, uid1)));
        assertEquals(v1, getOnlyVertex(graph.query().has("uid", Cmp.NOT_EQUAL, uid2)));

    }


    /**
     * Tests conditional indexing and the different management features
     */
    @Test
    public void testConditionalIndexing() {
        PropertyKey name = makeKey("name", String.class);
        PropertyKey weight = makeKey("weight", Double.class);
        PropertyKey text = makeKey("text", String.class);

        VertexLabel person = mgmt.makeVertexLabel("person").make();
        VertexLabel org = mgmt.makeVertexLabel("org").make();

        JanusGraphIndex index1 = mgmt.buildIndex("index1", Vertex.class).
                addKey(name, getStringMapping()).buildMixedIndex(INDEX);
        JanusGraphIndex index2 = mgmt.buildIndex("index2", Vertex.class).indexOnly(person).
                addKey(text, getTextMapping()).addKey(weight).buildMixedIndex(INDEX);
        JanusGraphIndex index3 = mgmt.buildIndex("index3", Vertex.class).indexOnly(org).
                addKey(text, getTextMapping()).addKey(weight).buildMixedIndex(INDEX);

        // ########### INSPECTION & FAILURE ##############
        assertTrue(mgmt.containsGraphIndex("index1"));
        assertFalse(mgmt.containsGraphIndex("index"));
        assertCount(3, mgmt.getGraphIndexes(Vertex.class));
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
        assertEquals("index2", index2.name());
        assertEquals(INDEX, index3.getBackingIndex());
        assertFalse(index2.isUnique());
        assertEquals(2, index3.getFieldKeys().length);
        assertEquals(1, index1.getFieldKeys().length);
        assertEquals(3, index3.getParametersFor(text).length);
        assertEquals(2, index3.getParametersFor(weight).length);

        try {
            //Already exists
            mgmt.buildIndex("index2", Vertex.class).addKey(weight).buildMixedIndex(INDEX);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //Already exists
            mgmt.buildIndex("index2", Vertex.class).addKey(weight).buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //Key is already added
            mgmt.addIndexKey(index2, weight);
            fail();
        } catch (IllegalArgumentException e) {
        }

        finishSchema();
        clopen();

        // ########### INSPECTION & FAILURE (copied from above) ##############
        assertTrue(mgmt.containsGraphIndex("index1"));
        assertFalse(mgmt.containsGraphIndex("index"));
        assertCount(3, mgmt.getGraphIndexes(Vertex.class));
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
        assertEquals("index2", index2.name());
        assertEquals(INDEX, index3.getBackingIndex());
        assertFalse(index2.isUnique());
        assertEquals(2, index3.getFieldKeys().length);
        assertEquals(1, index1.getFieldKeys().length);
        assertEquals(3, index3.getParametersFor(text).length);
        assertEquals(2, index3.getParametersFor(weight).length);

        try {
            //Already exists
            mgmt.buildIndex("index2", Vertex.class).addKey(weight).buildMixedIndex(INDEX);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //Already exists
            mgmt.buildIndex("index2", Vertex.class).addKey(weight).buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //Key is already added
            mgmt.addIndexKey(index2, weight);
            fail();
        } catch (IllegalArgumentException e) {
        }


        // ########### TRANSACTIONAL ##############
        weight = tx.getPropertyKey("weight");


        final int numV = 200;
        String[] strs = {"houseboat", "humanoid", "differential", "extraordinary"};
        String[] strs2 = new String[strs.length];
        for (int i = 0; i < strs.length; i++) strs2[i] = strs[i] + " " + strs[i];
        final int modulo = 5;
        assert numV % (modulo * strs.length * 2) == 0;

        for (int i = 0; i < numV; i++) {
            JanusGraphVertex v = tx.addVertex(i % 2 == 0 ? "person" : "org");
            v.property("name", strs[i % strs.length]);
            v.property("text", strs[i % strs.length]);
            v.property("weight", (i % modulo) + 0.5);
        }

        //########## QUERIES ################
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[0]).has(LABEL_NAME, Cmp.EQUAL, "person"), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{true, true}, index2.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[0]).has(LABEL_NAME, Cmp.EQUAL, "person").orderBy("weight", decr), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{true, true}, weight, Order.DESC, index2.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[3]).has(LABEL_NAME, Cmp.EQUAL, "org"), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{true, true}, index3.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[1]).has(LABEL_NAME, Cmp.EQUAL, "org").orderBy("weight", decr), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{true, true}, weight, Order.DESC, index3.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[0]).has("weight", Cmp.EQUAL, 2.5).has(LABEL_NAME, Cmp.EQUAL, "person"), ElementCategory.VERTEX,
                numV / (modulo * strs.length), new boolean[]{true, true}, index2.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[2]).has(LABEL_NAME, Cmp.EQUAL, "person"), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{false, true}, index1.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[3]).has(LABEL_NAME, Cmp.EQUAL, "person"), ElementCategory.VERTEX,
                0, new boolean[]{false, true}, index1.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[0]), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{true, true}, index1.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[2]).has("text", Text.CONTAINS, strs[2]).has(LABEL_NAME, Cmp.EQUAL, "person"), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{true, true}, index1.name(), index2.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[0]).has("text", Text.CONTAINS, strs[0]).has(LABEL_NAME, Cmp.EQUAL, "person").orderBy("weight", incr), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{true, true}, weight, Order.ASC, index1.name(), index2.name());

        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[0]), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{false, true});
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[0]).orderBy("weight", incr), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{false, false}, weight, Order.ASC);

        clopen();
        weight = tx.getPropertyKey("weight");

        //########## QUERIES (copied from above) ################
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[0]).has(LABEL_NAME, Cmp.EQUAL, "person"), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{true, true}, index2.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[0]).has(LABEL_NAME, Cmp.EQUAL, "person").orderBy("weight", decr), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{true, true}, weight, Order.DESC, index2.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[3]).has(LABEL_NAME, Cmp.EQUAL, "org"), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{true, true}, index3.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[1]).has(LABEL_NAME, Cmp.EQUAL, "org").orderBy("weight", decr), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{true, true}, weight, Order.DESC, index3.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[0]).has("weight", Cmp.EQUAL, 2.5).has(LABEL_NAME, Cmp.EQUAL, "person"), ElementCategory.VERTEX,
                numV / (modulo * strs.length), new boolean[]{true, true}, index2.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[2]).has(LABEL_NAME, Cmp.EQUAL, "person"), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{false, true}, index1.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[3]).has(LABEL_NAME, Cmp.EQUAL, "person"), ElementCategory.VERTEX,
                0, new boolean[]{false, true}, index1.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[0]), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{true, true}, index1.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[2]).has("text", Text.CONTAINS, strs[2]).has(LABEL_NAME, Cmp.EQUAL, "person"), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{true, true}, index1.name(), index2.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[0]).has("text", Text.CONTAINS, strs[0]).has(LABEL_NAME, Cmp.EQUAL, "person").orderBy("weight", incr), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{true, true}, weight, Order.ASC, index1.name(), index2.name());

        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[0]), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{false, true});
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[0]).orderBy("weight", incr), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{false, false}, weight, Order.ASC);
    }

    @Test
    public void testCompositeAndMixedIndexing() {
        PropertyKey name = makeKey("name", String.class);
        PropertyKey weight = makeKey("weight", Double.class);
        PropertyKey text = makeKey("text", String.class);
        makeKey("flag", Boolean.class);

        JanusGraphIndex composite = mgmt.buildIndex("composite", Vertex.class).addKey(name).addKey(weight).buildCompositeIndex();
        JanusGraphIndex mixed = mgmt.buildIndex("mixed", Vertex.class).addKey(weight)
                .addKey(text, getTextMapping()).buildMixedIndex(INDEX);
        mixed.name();
        composite.name();
        finishSchema();

        final int numV = 100;
        String[] strs = {"houseboat", "humanoid", "differential", "extraordinary"};
        String[] strs2 = new String[strs.length];
        for (int i = 0; i < strs.length; i++) strs2[i] = strs[i] + " " + strs[i];
        final int modulo = 5;
        final int divisor = modulo * strs.length;

        for (int i = 0; i < numV; i++) {
            JanusGraphVertex v = tx.addVertex();
            v.property("name", strs[i % strs.length]);
            v.property("text", strs[i % strs.length]);
            v.property("weight", (i % modulo) + 0.5);
            v.property("flag", true);
        }

        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[0]), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{false, true});
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[0]), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{true, true}, mixed.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[0]).has("flag"), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{false, true}, mixed.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[0]).has("weight", Cmp.EQUAL, 1.5), ElementCategory.VERTEX,
                numV / divisor, new boolean[]{true, true}, composite.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[0]).has("weight", Cmp.EQUAL, 1.5).has("flag"), ElementCategory.VERTEX,
                numV / divisor, new boolean[]{false, true}, composite.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[2]).has("weight", Cmp.EQUAL, 2.5), ElementCategory.VERTEX,
                numV / divisor, new boolean[]{true, true}, mixed.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[2]).has("weight", Cmp.EQUAL, 2.5).has("flag"), ElementCategory.VERTEX,
                numV / divisor, new boolean[]{false, true}, mixed.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[3]).has("name", Cmp.EQUAL, strs[3]).has("weight", Cmp.EQUAL, 3.5), ElementCategory.VERTEX,
                numV / divisor, new boolean[]{true, true}, mixed.name(), composite.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[3]).has("name", Cmp.EQUAL, strs[3]).has("weight", Cmp.EQUAL, 3.5).has("flag"), ElementCategory.VERTEX,
                numV / divisor, new boolean[]{false, true}, mixed.name(), composite.name());

        clopen();

        //Same queries as above
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[0]), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{false, true});
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[0]), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{true, true}, mixed.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[0]).has("flag"), ElementCategory.VERTEX,
                numV / strs.length, new boolean[]{false, true}, mixed.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[0]).has("weight", Cmp.EQUAL, 1.5), ElementCategory.VERTEX,
                numV / divisor, new boolean[]{true, true}, composite.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, strs[0]).has("weight", Cmp.EQUAL, 1.5).has("flag"), ElementCategory.VERTEX,
                numV / divisor, new boolean[]{false, true}, composite.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[2]).has("weight", Cmp.EQUAL, 2.5), ElementCategory.VERTEX,
                numV / divisor, new boolean[]{true, true}, mixed.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[2]).has("weight", Cmp.EQUAL, 2.5).has("flag"), ElementCategory.VERTEX,
                numV / divisor, new boolean[]{false, true}, mixed.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[3]).has("name", Cmp.EQUAL, strs[3]).has("weight", Cmp.EQUAL, 3.5), ElementCategory.VERTEX,
                numV / divisor, new boolean[]{true, true}, mixed.name(), composite.name());
        evaluateQuery(tx.query().has("text", Text.CONTAINS, strs[3]).has("name", Cmp.EQUAL, strs[3]).has("weight", Cmp.EQUAL, 3.5).has("flag"), ElementCategory.VERTEX,
                numV / divisor, new boolean[]{false, true}, mixed.name(), composite.name());

    }


    private void setupChainGraph(int numV, String[] strs, boolean sameNameMapping) {
        clopen(option(INDEX_NAME_MAPPING, INDEX), sameNameMapping);
        JanusGraphIndex vindex = getExternalIndex(Vertex.class, INDEX);
        JanusGraphIndex eindex = getExternalIndex(Edge.class, INDEX);
        JanusGraphIndex pindex = getExternalIndex(JanusGraphVertexProperty.class, INDEX);
        PropertyKey name = makeKey("name", String.class);

        mgmt.addIndexKey(vindex, name, getStringMapping());
        mgmt.addIndexKey(eindex, name, getStringMapping());
        mgmt.addIndexKey(pindex, name, getStringMapping(), Parameter.of("mapped-name", "xstr"));
        PropertyKey text = makeKey("text", String.class);
        mgmt.addIndexKey(vindex, text, getTextMapping(), Parameter.of("mapped-name", "xtext"));
        mgmt.addIndexKey(eindex, text, getTextMapping());
        mgmt.addIndexKey(pindex, text, getTextMapping());
        mgmt.makeEdgeLabel("knows").signature(name).make();
        mgmt.makePropertyKey("uid").dataType(String.class).signature(text).make();
        finishSchema();
        JanusGraphVertex previous = null;
        for (int i = 0; i < numV; i++) {
            JanusGraphVertex v = graph.addVertex("name", strs[i % strs.length], "text", strs[i % strs.length]);
            v.addEdge("knows", previous == null ? v : previous,
                    "name", strs[i % strs.length], "text", strs[i % strs.length]);
            v.property("uid", "v" + i,
                    "name", strs[i % strs.length], "text", strs[i % strs.length]);
            previous = v;
        }
    }

    /**
     * Tests index parameters (mapping and names) and various string predicates
     */
    @Test
    public void testIndexParameters() {
        int numV = 1000;
        String[] strs = {"Uncle Berry has a farm", "and on his farm he has five ducks", "ducks are beautiful animals", "the sky is very blue today"};
        setupChainGraph(numV, strs, false);

        evaluateQuery(graph.query().has("text", Text.CONTAINS, "ducks"),
                ElementCategory.VERTEX, numV / strs.length * 2, new boolean[]{true, true}, VINDEX);
        assertCount(numV / strs.length * 2, graph.query().has("text", Text.CONTAINS, "ducks").vertices());
        assertCount(numV / strs.length * 2, graph.query().has("text", Text.CONTAINS, "farm").vertices());
        assertCount(numV / strs.length, graph.query().has("text", Text.CONTAINS, "beautiful").vertices());
        evaluateQuery(graph.query().has("text", Text.CONTAINS_PREFIX, "beauti"),
                ElementCategory.VERTEX, numV / strs.length, new boolean[]{true, true}, VINDEX);
        assertCount(numV / strs.length, graph.query().has("text", Text.CONTAINS_REGEX, "be[r]+y").vertices());
        assertCount(0, graph.query().has("text", Text.CONTAINS, "lolipop").vertices());
        assertCount(numV / strs.length, graph.query().has("name", Cmp.EQUAL, strs[1]).vertices());
        assertCount(numV / strs.length, graph.query().has("name", Cmp.EQUAL, strs[1]).vertices());
        assertCount(numV / strs.length * (strs.length - 1), graph.query().has("name", Cmp.NOT_EQUAL, strs[2]).vertices());
        assertCount(0, graph.query().has("name", Cmp.EQUAL, "farm").vertices());
        assertCount(numV / strs.length, graph.query().has("name", Text.PREFIX, "ducks").vertices());
        assertCount(numV / strs.length * 2, graph.query().has("name", Text.REGEX, "(.*)ducks(.*)").vertices());

        //Same queries for edges
        evaluateQuery(graph.query().has("text", Text.CONTAINS, "ducks"),
                ElementCategory.EDGE, numV / strs.length * 2, new boolean[]{true, true}, EINDEX);
        assertCount(numV / strs.length * 2, graph.query().has("text", Text.CONTAINS, "ducks").edges());
        assertCount(numV / strs.length * 2, graph.query().has("text", Text.CONTAINS, "farm").edges());
        assertCount(numV / strs.length, graph.query().has("text", Text.CONTAINS, "beautiful").edges());
        evaluateQuery(graph.query().has("text", Text.CONTAINS_PREFIX, "beauti"),
                ElementCategory.EDGE, numV / strs.length, new boolean[]{true, true}, EINDEX);
        assertCount(numV / strs.length, graph.query().has("text", Text.CONTAINS_REGEX, "be[r]+y").edges());
        assertCount(0, graph.query().has("text", Text.CONTAINS, "lolipop").edges());
        assertCount(numV / strs.length, graph.query().has("name", Cmp.EQUAL, strs[1]).edges());
        assertCount(numV / strs.length, graph.query().has("name", Cmp.EQUAL, strs[1]).edges());
        assertCount(numV / strs.length * (strs.length - 1), graph.query().has("name", Cmp.NOT_EQUAL, strs[2]).edges());
        assertCount(0, graph.query().has("name", Cmp.EQUAL, "farm").edges());
        assertCount(numV / strs.length, graph.query().has("name", Text.PREFIX, "ducks").edges());
        assertCount(numV / strs.length * 2, graph.query().has("name", Text.REGEX, "(.*)ducks(.*)").edges());

        //Same queries for properties
        evaluateQuery(graph.query().has("text", Text.CONTAINS, "ducks"),
                ElementCategory.PROPERTY, numV / strs.length * 2, new boolean[]{true, true}, PINDEX);
        assertCount(numV / strs.length * 2, graph.query().has("text", Text.CONTAINS, "ducks").properties());
        assertCount(numV / strs.length * 2, graph.query().has("text", Text.CONTAINS, "farm").properties());
        assertCount(numV / strs.length, graph.query().has("text", Text.CONTAINS, "beautiful").properties());
        evaluateQuery(graph.query().has("text", Text.CONTAINS_PREFIX, "beauti"),
                ElementCategory.PROPERTY, numV / strs.length, new boolean[]{true, true}, PINDEX);
        assertCount(numV / strs.length, graph.query().has("text", Text.CONTAINS_REGEX, "be[r]+y").properties());
        assertCount(0, graph.query().has("text", Text.CONTAINS, "lolipop").properties());
        assertCount(numV / strs.length, graph.query().has("name", Cmp.EQUAL, strs[1]).properties());
        assertCount(numV / strs.length, graph.query().has("name", Cmp.EQUAL, strs[1]).properties());
        assertCount(numV / strs.length * (strs.length - 1), graph.query().has(LABEL_NAME, "uid").has("name", Cmp.NOT_EQUAL, strs[2]).properties());
        assertCount(0, graph.query().has("name", Cmp.EQUAL, "farm").properties());
        assertCount(numV / strs.length, graph.query().has("name", Text.PREFIX, "ducks").properties());
        assertCount(numV / strs.length * 2, graph.query().has("name", Text.REGEX, "(.*)ducks(.*)").properties());


        clopen();
        /* =======================================
        Same queries as above but against backend */

        evaluateQuery(graph.query().has("text", Text.CONTAINS, "ducks"),
                ElementCategory.VERTEX, numV / strs.length * 2, new boolean[]{true, true}, VINDEX);
        assertCount(numV / strs.length * 2, graph.query().has("text", Text.CONTAINS, "ducks").vertices());
        assertCount(numV / strs.length * 2, graph.query().has("text", Text.CONTAINS, "farm").vertices());
        assertCount(numV / strs.length, graph.query().has("text", Text.CONTAINS, "beautiful").vertices());
        evaluateQuery(graph.query().has("text", Text.CONTAINS_PREFIX, "beauti"),
                ElementCategory.VERTEX, numV / strs.length, new boolean[]{true, true}, VINDEX);
        assertCount(numV / strs.length, graph.query().has("text", Text.CONTAINS_REGEX, "be[r]+y").vertices());
        assertCount(0, graph.query().has("text", Text.CONTAINS, "lolipop").vertices());
        assertCount(numV / strs.length, graph.query().has("name", Cmp.EQUAL, strs[1]).vertices());
        assertCount(numV / strs.length, graph.query().has("name", Cmp.EQUAL, strs[1]).vertices());
        assertCount(numV / strs.length * (strs.length - 1), graph.query().has("name", Cmp.NOT_EQUAL, strs[2]).vertices());
        assertCount(0, graph.query().has("name", Cmp.EQUAL, "farm").vertices());
        assertCount(numV / strs.length, graph.query().has("name", Text.PREFIX, "ducks").vertices());
        assertCount(numV / strs.length * 2, graph.query().has("name", Text.REGEX, "(.*)ducks(.*)").vertices());

        //Same queries for edges
        evaluateQuery(graph.query().has("text", Text.CONTAINS, "ducks"),
                ElementCategory.EDGE, numV / strs.length * 2, new boolean[]{true, true}, EINDEX);
        assertCount(numV / strs.length * 2, graph.query().has("text", Text.CONTAINS, "ducks").edges());
        assertCount(numV / strs.length * 2, graph.query().has("text", Text.CONTAINS, "farm").edges());
        assertCount(numV / strs.length, graph.query().has("text", Text.CONTAINS, "beautiful").edges());
        evaluateQuery(graph.query().has("text", Text.CONTAINS_PREFIX, "beauti"),
                ElementCategory.EDGE, numV / strs.length, new boolean[]{true, true}, EINDEX);
        assertCount(numV / strs.length, graph.query().has("text", Text.CONTAINS_REGEX, "be[r]+y").edges());
        assertCount(0, graph.query().has("text", Text.CONTAINS, "lolipop").edges());
        assertCount(numV / strs.length, graph.query().has("name", Cmp.EQUAL, strs[1]).edges());
        assertCount(numV / strs.length, graph.query().has("name", Cmp.EQUAL, strs[1]).edges());
        assertCount(numV / strs.length * (strs.length - 1), graph.query().has("name", Cmp.NOT_EQUAL, strs[2]).edges());
        assertCount(0, graph.query().has("name", Cmp.EQUAL, "farm").edges());
        assertCount(numV / strs.length, graph.query().has("name", Text.PREFIX, "ducks").edges());
        assertCount(numV / strs.length * 2, graph.query().has("name", Text.REGEX, "(.*)ducks(.*)").edges());

        //Same queries for properties
        evaluateQuery(graph.query().has("text", Text.CONTAINS, "ducks"),
                ElementCategory.PROPERTY, numV / strs.length * 2, new boolean[]{true, true}, PINDEX);
        assertCount(numV / strs.length * 2, graph.query().has("text", Text.CONTAINS, "ducks").properties());
        assertCount(numV / strs.length * 2, graph.query().has("text", Text.CONTAINS, "farm").properties());
        assertCount(numV / strs.length, graph.query().has("text", Text.CONTAINS, "beautiful").properties());
        evaluateQuery(graph.query().has("text", Text.CONTAINS_PREFIX, "beauti"),
                ElementCategory.PROPERTY, numV / strs.length, new boolean[]{true, true}, PINDEX);
        assertCount(numV / strs.length, graph.query().has("text", Text.CONTAINS_REGEX, "be[r]+y").properties());
        assertCount(0, graph.query().has("text", Text.CONTAINS, "lolipop").properties());
        assertCount(numV / strs.length, graph.query().has("name", Cmp.EQUAL, strs[1]).properties());
        assertCount(numV / strs.length, graph.query().has("name", Cmp.EQUAL, strs[1]).properties());
        assertCount(numV / strs.length * (strs.length - 1), graph.query().has(LABEL_NAME, "uid").has("name", Cmp.NOT_EQUAL, strs[2]).properties());
        assertCount(0, graph.query().has("name", Cmp.EQUAL, "farm").properties());
        assertCount(numV / strs.length, graph.query().has("name", Text.PREFIX, "ducks").properties());
        assertCount(numV / strs.length * 2, graph.query().has("name", Text.REGEX, "(.*)ducks(.*)").properties());

        //Test name mapping
        if (supportsLuceneStyleQueries()) {
            assertCount(numV / strs.length * 2, graph.indexQuery(VINDEX, "xtext:ducks").vertices());
            assertCount(0, graph.indexQuery(EINDEX, "xtext:ducks").edges());
        }
    }

    /**
     * Tests index parameters (mapping and names) with raw indexQuery
     */
    @Test
    public void testRawQueries() {
        if (!supportsLuceneStyleQueries()) return;

        int numV = 1000;
        String[] strs = {"Uncle Berry has a farm", "and on his farm he has five ducks", "ducks are beautiful animals", "the sky is very blue today"};
        setupChainGraph(numV, strs, true);
        clopen();

        assertCount(numV / strs.length * 2, graph.indexQuery(VINDEX, "v.text:ducks").vertices());
        assertCount(numV / strs.length * 2, graph.indexQuery(VINDEX, "v.text:(farm uncle berry)").vertices());
        assertCount(numV / strs.length, graph.indexQuery(VINDEX, "v.text:(farm uncle berry) AND v.name:\"Uncle Berry has a farm\"").vertices());
        assertCount(numV / strs.length * 2, graph.indexQuery(VINDEX, "v.text:(beautiful are ducks)").vertices());
        assertCount(numV / strs.length * 2 - 10, graph.indexQuery(VINDEX, "v.text:(beautiful are ducks)").offset(10).vertices());
        long total = size(graph.indexQuery(VINDEX, "v.\"text\":(beautiful are ducks)").limit(Integer.MAX_VALUE).vertices());
        assertCount(10, graph.indexQuery(VINDEX, "v.\"text\":(beautiful are ducks)").limit(10).vertices());
        assertEquals(total, (long) graph.indexQuery(VINDEX, "v.\"text\":(beautiful are ducks)").limit(10).vertexTotals());
        assertCount(10, graph.indexQuery(VINDEX, "v.\"text\":(beautiful are ducks)").limit(10).offset(10).vertices());
        assertEquals(total, (long) graph.indexQuery(VINDEX, "v.\"text\":(beautiful are ducks)").limit(10).offset(10).vertexTotals());
        assertCount(0, graph.indexQuery(VINDEX, "v.\"text\":(beautiful are ducks)").limit(10).offset(numV).vertices());
        assertEquals(total, (long) graph.indexQuery(VINDEX, "v.\"text\":(beautiful are ducks)").limit(10).offset(numV).vertexTotals());
        //Test name mapping
        assertCount(numV / strs.length * 2, graph.indexQuery(VINDEX, "xtext:ducks").vertices());
        assertCount(0, graph.indexQuery(VINDEX, "text:ducks").vertices());
        //Test custom element identifier
        assertCount(numV / strs.length * 2, graph.indexQuery(VINDEX, "$v$text:ducks").setElementIdentifier("$v$").vertices());
        //assertCount(0, graph.indexQuery(VINDEX, "v.\"text\":ducks").setElementIdentifier("$v$").vertices()));

        //Same queries for edges
        assertCount(numV / strs.length * 2, graph.indexQuery(EINDEX, "e.text:ducks").edges());
        total = size(graph.indexQuery(EINDEX, "e.text:ducks").limit(Integer.MAX_VALUE).edges());
        assertEquals(total, (long) numV / strs.length * 2, graph.indexQuery(EINDEX, "e.text:ducks").edgeTotals());
        assertCount(numV / strs.length * 2, graph.indexQuery(EINDEX, "e.text:(farm uncle berry)").edges());
        assertCount(numV / strs.length, graph.indexQuery(EINDEX, "e.text:(farm uncle berry) AND e.name:\"Uncle Berry has a farm\"").edges());
        assertCount(numV / strs.length * 2, graph.indexQuery(EINDEX, "e.text:(beautiful are ducks)").edges());
        assertCount(numV / strs.length * 2 - 10, graph.indexQuery(EINDEX, "e.text:(beautiful are ducks)").offset(10).edges());
        total = size(graph.indexQuery(EINDEX, "e.\"text\":(beautiful are ducks)").limit(Integer.MAX_VALUE).edges());
        assertCount(10, graph.indexQuery(EINDEX, "e.\"text\":(beautiful are ducks)").limit(10).edges());
        assertEquals(total, (long) graph.indexQuery(EINDEX, "e.\"text\":(beautiful are ducks)").limit(10).edgeTotals());
        assertCount(10, graph.indexQuery(EINDEX, "e.\"text\":(beautiful are ducks)").limit(10).offset(10).edges());
        assertEquals(total, (long) graph.indexQuery(EINDEX, "e.\"text\":(beautiful are ducks)").limit(10).offset(10).edgeTotals());
        assertCount(0, graph.indexQuery(EINDEX, "e.\"text\":(beautiful are ducks)").limit(10).offset(numV).edges());
        assertEquals(total, (long) graph.indexQuery(EINDEX, "e.\"text\":(beautiful are ducks)").limit(10).offset(numV).edgeTotals());
        //Test name mapping
        assertCount(numV / strs.length * 2, graph.indexQuery(EINDEX, "text:ducks").edges());

        //Same queries for properties
        assertCount(numV / strs.length * 2, graph.indexQuery(PINDEX, "p.text:ducks").properties());
        total = size(graph.indexQuery(PINDEX, "p.text:ducks").limit(Integer.MAX_VALUE).properties());
        assertEquals(total, (long) numV / strs.length * 2, graph.indexQuery(PINDEX, "p.text:ducks").propertyTotals());
        assertCount(numV / strs.length * 2, graph.indexQuery(PINDEX, "p.text:(farm uncle berry)").properties());
        assertCount(numV / strs.length, graph.indexQuery(PINDEX, "p.text:(farm uncle berry) AND p.name:\"Uncle Berry has a farm\"").properties());
        assertCount(numV / strs.length * 2, graph.indexQuery(PINDEX, "p.text:(beautiful are ducks)").properties());
        assertCount(numV / strs.length * 2 - 10, graph.indexQuery(PINDEX, "p.text:(beautiful are ducks)").offset(10).properties());
        total = size(graph.indexQuery(PINDEX, "p.\"text\":(beautiful are ducks)").limit(Integer.MAX_VALUE).properties());
        assertCount(10, graph.indexQuery(PINDEX, "p.\"text\":(beautiful are ducks)").limit(10).properties());
        assertEquals(total, (long) graph.indexQuery(PINDEX, "p.\"text\":(beautiful are ducks)").limit(10).propertyTotals());
        assertCount(10, graph.indexQuery(PINDEX, "p.\"text\":(beautiful are ducks)").limit(10).offset(10).properties());
        assertEquals(total, (long) graph.indexQuery(PINDEX, "p.\"text\":(beautiful are ducks)").limit(10).offset(10).propertyTotals());
        assertCount(0, graph.indexQuery(PINDEX, "p.\"text\":(beautiful are ducks)").limit(10).offset(numV).properties());
        assertEquals(total, (long) graph.indexQuery(PINDEX, "p.\"text\":(beautiful are ducks)").limit(10).offset(numV).propertyTotals());
        //Test name mapping
        assertCount(numV / strs.length * 2, graph.indexQuery(PINDEX, "text:ducks").properties());
    }

    /**
     * Tests query parameters with raw indexQuery
     */
    @Test
    public void testRawQueriesWithParameters() throws Exception {
        if (!supportsLuceneStyleQueries()) return;
        Parameter asc_sort_p = null;
        Parameter desc_sort_p = null;

        // ElasticSearch and Solr have different formats for sort parameters
        String backend = readConfig.get(INDEX_BACKEND, INDEX);
        if ("elasticsearch".equals(backend)) {
            Map<String,String> sortAsc = new HashMap<String,String>();
            sortAsc.put("_score", "asc");
            asc_sort_p = new Parameter("sort",Arrays.asList(sortAsc));
            Map<String,String> sortDesc = new HashMap<String,String>();
            sortDesc.put("_score", "desc");
            desc_sort_p = new Parameter("sort",Arrays.asList(sortDesc));
        } else if ("solr".equals(backend)) {
            asc_sort_p = new Parameter("sort",new String[]{"score asc"});
            desc_sort_p = new Parameter("sort",new String[]{"score desc"});
        } else if ("lucene".equals(backend)) {
            return; // Ignore for lucene
        } else {
            Assert.fail("Unknown index backend:" + backend);
        }

        final PropertyKey field1Key = mgmt.makePropertyKey("field1").dataType(String.class).make();
        mgmt.buildIndex("store1", Vertex.class).addKey(field1Key).buildMixedIndex(INDEX);
        mgmt.commit();

        JanusGraphVertex v1 = tx.addVertex();
        JanusGraphVertex v2 = tx.addVertex();
        JanusGraphVertex v3 = tx.addVertex();

        v1.property("field1", "Hello Hello Hello Hello Hello Hello Hello Hello world");
        v2.property("field1", "Hello blue and yellow meet green");
        v3.property("field1", "Hello Hello world world");

        tx.commit();

        List<JanusGraphVertex> vertices = new ArrayList<JanusGraphVertex>();
        for (JanusGraphIndexQuery.Result<JanusGraphVertex> r : graph.indexQuery("store1", "v.field1:(Hello)").addParameter(asc_sort_p).vertices()) {
            vertices.add(r.getElement());
        }
        assertNotEmpty(vertices);
        int idx=vertices.size()-1;
        // Verify this query returns the items in reverse order.
        for (JanusGraphIndexQuery.Result<JanusGraphVertex> r : graph.indexQuery("store1", "v.field1:(Hello)").addParameter(desc_sort_p).vertices()) {
            assertEquals(vertices.get(idx--), r.getElement());
        }
    }

    @Test
    public void testDualMapping() {
        if (!indexFeatures.supportsStringMapping(Mapping.TEXTSTRING)) return;

        PropertyKey name = makeKey("name", String.class);
        JanusGraphIndex mixed = mgmt.buildIndex("mixed", Vertex.class).addKey(name, Mapping.TEXTSTRING.asParameter()).buildMixedIndex(INDEX);
        mixed.name();
        finishSchema();


        tx.addVertex("name", "Long John Don");
        tx.addVertex("name", "Long Little Lewis");
        tx.addVertex("name", "Middle Sister Mabel");

        clopen();
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, "Long John Don"), ElementCategory.VERTEX,
                1, new boolean[]{true, true}, "mixed");
        evaluateQuery(tx.query().has("name", Text.CONTAINS, "Long"), ElementCategory.VERTEX,
                2, new boolean[]{true, true}, "mixed");
        evaluateQuery(tx.query().has("name", Text.CONTAINS, "Long Don"), ElementCategory.VERTEX,
                1, new boolean[]{true, true}, "mixed");
        evaluateQuery(tx.query().has("name", Text.CONTAINS_PREFIX, "Lon"), ElementCategory.VERTEX,
                2, new boolean[]{true, true}, "mixed");
        evaluateQuery(tx.query().has("name", Text.CONTAINS_REGEX, "Lit*le"), ElementCategory.VERTEX,
                1, new boolean[]{true, true}, "mixed");
        evaluateQuery(tx.query().has("name", Text.REGEX, "Long.*"), ElementCategory.VERTEX,
                2, new boolean[]{true, true}, "mixed");
        evaluateQuery(tx.query().has("name", Text.PREFIX, "Middle"), ElementCategory.VERTEX,
                1, new boolean[]{true, true}, "mixed");
        evaluateQuery(tx.query().has("name", Text.FUZZY, "Long john Don"), ElementCategory.VERTEX,
                1, new boolean[]{true, true}, "mixed");
        evaluateQuery(tx.query().has("name", Text.CONTAINS_FUZZY, "Midle"), ElementCategory.VERTEX,
                1, new boolean[]{true, true}, "mixed");
        for (Vertex u : tx.getVertices()) {
            String n = u.<String>value("name");
            if (n.endsWith("Don")) {
                u.remove();
            } else if (n.endsWith("Lewis")) {
                u.property(VertexProperty.Cardinality.single, "name", "Big Brother Bob");
            } else if (n.endsWith("Mabel")) {
                u.property("name").remove();
            }
        }

        clopen();

        evaluateQuery(tx.query().has("name", Text.CONTAINS, "Long"), ElementCategory.VERTEX,
                0, new boolean[]{true, true}, "mixed");
        evaluateQuery(tx.query().has("name", Text.CONTAINS, "Big"), ElementCategory.VERTEX,
                1, new boolean[]{true, true}, "mixed");
        evaluateQuery(tx.query().has("name", Text.PREFIX, "Big"), ElementCategory.VERTEX,
                1, new boolean[]{true, true}, "mixed");
        evaluateQuery(tx.query().has("name", Text.PREFIX, "Middle"), ElementCategory.VERTEX,
                0, new boolean[]{true, true}, "mixed");

    }

    @Category({BrittleTests.class})
    @Test
    public void testIndexReplay() throws Exception {
        final TimestampProvider times = graph.getConfiguration().getTimestampProvider();
        final Instant startTime = times.getTime();
        clopen(option(SYSTEM_LOG_TRANSACTIONS), true
                , option(KCVSLog.LOG_READ_LAG_TIME, TRANSACTION_LOG), Duration.ofMillis(50)
                , option(LOG_READ_INTERVAL, TRANSACTION_LOG), Duration.ofMillis(250)
                , option(MAX_COMMIT_TIME), Duration.ofSeconds(1)
                , option(STORAGE_WRITE_WAITTIME), Duration.ofMillis(300)
                , option(TestMockIndexProvider.INDEX_BACKEND_PROXY, INDEX), readConfig.get(INDEX_BACKEND, INDEX)
                , option(INDEX_BACKEND, INDEX), TestMockIndexProvider.class.getName()
                , option(TestMockIndexProvider.INDEX_MOCK_FAILADD, INDEX), true
        );

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).make();
        mgmt.buildIndex("mi", Vertex.class).addKey(name, getTextMapping()).addKey(age).buildMixedIndex(INDEX);
        finishSchema();
        Vertex vs[] = new JanusGraphVertex[4];

        vs[0] = tx.addVertex("name", "Big Boy Bobson", "age", 55);
        newTx();
        vs[1] = tx.addVertex("name", "Long Little Lewis", "age", 35);
        vs[2] = tx.addVertex("name", "Tall Long Tiger", "age", 75);
        vs[3] = tx.addVertex("name", "Long John Don", "age", 15);
        newTx();
        vs[2] = getV(tx, vs[2]);
        vs[2].remove();
        vs[3] = getV(tx, vs[3]);
        vs[3].property(VertexProperty.Cardinality.single, "name", "Bad Boy Badsy");
        vs[3].property("age").remove();
        newTx();
        vs[0] = getV(tx, vs[0]);
        vs[0].property(VertexProperty.Cardinality.single, "age", 66);
        newTx();

        clopen();
        //Just to make sure nothing has been persisted to index
        evaluateQuery(tx.query().has("name", Text.CONTAINS, "boy"),
                ElementCategory.VERTEX, 0, new boolean[]{true, true}, "mi");
        /*
        Transaction Recovery
         */
        TransactionRecovery recovery = JanusGraphFactory.startTransactionRecovery(graph, startTime);
        //wait
        Thread.sleep(12000L);

        recovery.shutdown();
        long[] recoveryStats = ((StandardTransactionLogProcessor) recovery).getStatistics();

        clopen();

        evaluateQuery(tx.query().has("name", Text.CONTAINS, "boy"),
                ElementCategory.VERTEX, 2, new boolean[]{true, true}, "mi");
        evaluateQuery(tx.query().has("name", Text.CONTAINS, "long"),
                ElementCategory.VERTEX, 1, new boolean[]{true, true}, "mi");
//        JanusGraphVertex v = Iterables.getOnlyElement(tx.query().has("name",Text.CONTAINS,"long").vertices());
//        System.out.println(v.getProperty("age"));
        evaluateQuery(tx.query().has("name", Text.CONTAINS, "long").interval("age", 30, 40),
                ElementCategory.VERTEX, 1, new boolean[]{true, true}, "mi");
        evaluateQuery(tx.query().has("age", 75),
                ElementCategory.VERTEX, 0, new boolean[]{true, true}, "mi");
        evaluateQuery(tx.query().has("name", Text.CONTAINS, "boy").interval("age", 60, 70),
                ElementCategory.VERTEX, 1, new boolean[]{true, true}, "mi");
        evaluateQuery(tx.query().interval("age", 0, 100),
                ElementCategory.VERTEX, 2, new boolean[]{true, true}, "mi");


        assertEquals(1, recoveryStats[0]); //schema transaction was successful
        assertEquals(4, recoveryStats[1]); //all 4 index transaction had provoked errors in the indexing backend
    }

    @Test
    public void testIndexUpdatesWithoutReindex() throws InterruptedException, ExecutionException {
        Object[] settings = new Object[]{option(LOG_SEND_DELAY, MANAGEMENT_LOG), Duration.ofMillis(0),
                option(KCVSLog.LOG_READ_LAG_TIME, MANAGEMENT_LOG), Duration.ofMillis(50),
                option(LOG_READ_INTERVAL, MANAGEMENT_LOG), Duration.ofMillis(250)
        };

        clopen(settings);
        final String defText = "Mountain rocks are great friends";
        final int defTime = 5;
        final double defHeight = 101.1;
        final String[] defPhones = new String[]{"1234", "5678"};

        //Creates types and index only two keys key
        mgmt.makePropertyKey("time").dataType(Integer.class).make();
        PropertyKey text = mgmt.makePropertyKey("text").dataType(String.class).make();

        mgmt.makePropertyKey("height").dataType(Double.class).make();
        if (indexFeatures.supportsCardinality(Cardinality.LIST)) {
            mgmt.makePropertyKey("phone").dataType(String.class).cardinality(Cardinality.LIST).make();
        }
        mgmt.buildIndex("theIndex", Vertex.class).addKey(text, getTextMapping(), getFieldMap(text)).buildMixedIndex(INDEX);
        finishSchema();

        //Add initial data
        addVertex(defTime, defText, defHeight, defPhones);

        //Indexes should not yet be in use
        clopen(settings);
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "rocks"),
                ElementCategory.VERTEX, 1, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().has("time", 5),
                ElementCategory.VERTEX, 1, new boolean[]{false, true});
        evaluateQuery(tx.query().interval("height", 100, 200),
                ElementCategory.VERTEX, 1, new boolean[]{false, true});
        evaluateQuery(tx.query().interval("height", 100, 200).has("time", 5),
                ElementCategory.VERTEX, 1, new boolean[]{false, true});
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "rocks").has("time", 5).interval("height", 100, 200),
                ElementCategory.VERTEX, 1, new boolean[]{false, true}, "theIndex");
        if (indexFeatures.supportsCardinality(Cardinality.LIST)) {
            evaluateQuery(tx.query().has("phone", Cmp.EQUAL, "1234"),
                    ElementCategory.VERTEX, 1, new boolean[]{false, true});
            evaluateQuery(tx.query().has("phone", Cmp.EQUAL, "5678"),
                    ElementCategory.VERTEX, 1, new boolean[]{false, true});
        }
        newTx();

        //Add another key to index ------------------------------------------------------
        finishSchema();
        PropertyKey time = mgmt.getPropertyKey("time");
        mgmt.addIndexKey(mgmt.getGraphIndex("theIndex"), time, getFieldMap(time));
        finishSchema();
        newTx();

        //Add more data
        addVertex(defTime, defText, defHeight, defPhones);
        tx.commit();
        //Should not yet be able to enable since not yet registered
        assertNull(mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.ENABLE_INDEX));
        //This call is redundant and just here to make sure it doesn't mess anything up
        mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.REGISTER_INDEX).get();
        mgmt.commit();

        ManagementSystem.awaitGraphIndexStatus(graph, "theIndex").timeout(10L, ChronoUnit.SECONDS).call();

        finishSchema();
        mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.ENABLE_INDEX).get();
        finishSchema();

        //Add more data
        addVertex(defTime, defText, defHeight, defPhones);

        //One more key should be indexed but only sees partial data
        clopen(settings);
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "rocks"),
                ElementCategory.VERTEX, 3, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().has("time", 5),
                ElementCategory.VERTEX, 2, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().interval("height", 100, 200),
                ElementCategory.VERTEX, 3, new boolean[]{false, true});
        evaluateQuery(tx.query().interval("height", 100, 200).has("time", 5),
                ElementCategory.VERTEX, 2, new boolean[]{false, true}, "theIndex");
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "rocks").has("time", 5).interval("height", 100, 200),
                ElementCategory.VERTEX, 2, new boolean[]{false, true}, "theIndex");
        if (indexFeatures.supportsCardinality(Cardinality.LIST)) {
            evaluateQuery(tx.query().has("phone", Cmp.EQUAL, "1234"),
                    ElementCategory.VERTEX, 3, new boolean[]{false, true});
            evaluateQuery(tx.query().has("phone", Cmp.EQUAL, "5678"),
                    ElementCategory.VERTEX, 3, new boolean[]{false, true});
        }
        newTx();

        //Add another key to index ------------------------------------------------------
        finishSchema();
        PropertyKey height = mgmt.getPropertyKey("height");
        mgmt.addIndexKey(mgmt.getGraphIndex("theIndex"), height);
        if (indexFeatures.supportsCardinality(Cardinality.LIST)) {
            PropertyKey phone = mgmt.getPropertyKey("phone");
            mgmt.addIndexKey(mgmt.getGraphIndex("theIndex"), phone, new Parameter("mapping", Mapping.STRING));
        }
        finishSchema();

        //Add more data
        addVertex(defTime, defText, defHeight, defPhones);
        tx.commit();
        mgmt.commit();

        ManagementUtil.awaitGraphIndexUpdate(graph, "theIndex", 10, ChronoUnit.SECONDS);

        finishSchema();
        mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.ENABLE_INDEX);
        finishSchema();

        JanusGraphIndex index = mgmt.getGraphIndex("theIndex");
        for (PropertyKey key : index.getFieldKeys()) {
            assertEquals(SchemaStatus.ENABLED, index.getIndexStatus(key));
        }

        //Add more data
        addVertex(defTime, defText, defHeight, defPhones);

        //One more key should be indexed but only sees partial data
        clopen(settings);
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "rocks"),
                ElementCategory.VERTEX, 5, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().has("time", 5),
                ElementCategory.VERTEX, 4, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().interval("height", 100, 200),
                ElementCategory.VERTEX, 2, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().interval("height", 100, 200).has("time", 5),
                ElementCategory.VERTEX, 2, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "rocks").has("time", 5).interval("height", 100, 200),
                ElementCategory.VERTEX, 2, new boolean[]{true, true}, "theIndex");
        if (indexFeatures.supportsCardinality(Cardinality.LIST)) {
            evaluateQuery(tx.query().has("phone", Cmp.EQUAL, "1234"),
                    ElementCategory.VERTEX, 2, new boolean[]{true, true}, "theIndex");
            evaluateQuery(tx.query().has("phone", Cmp.EQUAL, "5678"),
                    ElementCategory.VERTEX, 2, new boolean[]{true, true}, "theIndex");
        }
        newTx();
        finishSchema();

        mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.REINDEX).get();
        mgmt.commit();

        finishSchema();

        //All the data should now be in the index
        clopen(settings);
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "rocks"),
                ElementCategory.VERTEX, 5, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().has("time", 5),
                ElementCategory.VERTEX, 5, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().interval("height", 100, 200),
                ElementCategory.VERTEX, 5, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().interval("height", 100, 200).has("time", 5),
                ElementCategory.VERTEX, 5, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "rocks").has("time", 5).interval("height", 100, 200),
                ElementCategory.VERTEX, 5, new boolean[]{true, true}, "theIndex");
        if (indexFeatures.supportsCardinality(Cardinality.LIST)) {
            evaluateQuery(tx.query().has("phone", Cmp.EQUAL, "1234"),
                    ElementCategory.VERTEX, 5, new boolean[]{true, true}, "theIndex");
            evaluateQuery(tx.query().has("phone", Cmp.EQUAL, "5678"),
                    ElementCategory.VERTEX, 5, new boolean[]{true, true}, "theIndex");
        }

        mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.DISABLE_INDEX).get();
        tx.commit();
        mgmt.commit();

        ManagementUtil.awaitGraphIndexUpdate(graph, "theIndex", 10, ChronoUnit.SECONDS);
        finishSchema();

        index = mgmt.getGraphIndex("theIndex");
        for (PropertyKey key : index.getFieldKeys()) {
            assertEquals(SchemaStatus.DISABLED, index.getIndexStatus(key));
        }

        newTx();
        //This now requires a full graph scan
        evaluateQuery(tx.query().has("time", 5),
                ElementCategory.VERTEX, 5, new boolean[]{false, true});

    }

    private void addVertex(int time, String text, double height, String[] phones) {
        newTx();
        JanusGraphVertex v = tx.addVertex("text", text, "time", time, "height", height);
        for (String phone : phones) {
            v.property("phone", phone);
        }

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
        final int eventTTLSeconds = (int) TestGraphConfigs.getTTL(TimeUnit.SECONDS);
        mgmt.setTTL(event, Duration.ofSeconds(eventTTLSeconds));

        mgmt.buildIndex("index1", Vertex.class).
                addKey(name, getStringMapping()).addKey(time).buildMixedIndex(INDEX);
        mgmt.buildIndex("index2", Vertex.class).indexOnly(event).
                addKey(text, getTextMapping()).buildMixedIndex(INDEX);

        assertEquals(Duration.ZERO, mgmt.getTTL(name));
        assertEquals(Duration.ZERO, mgmt.getTTL(time));
        assertEquals(Duration.ofSeconds(eventTTLSeconds), mgmt.getTTL(event));
        finishSchema();

        JanusGraphVertex v1 = tx.addVertex("event");
        v1.property(VertexProperty.Cardinality.single, "name", "first event");
        v1.property(VertexProperty.Cardinality.single, "text", "this text will help to identify the first event");
        long time1 = System.currentTimeMillis();
        v1.property(VertexProperty.Cardinality.single, "time", time1);
        JanusGraphVertex v2 = tx.addVertex("event");
        v2.property(VertexProperty.Cardinality.single, "name", "second event");
        v2.property(VertexProperty.Cardinality.single, "text", "this text won't match");
        long time2 = time1 + 1;
        v2.property(VertexProperty.Cardinality.single, "time", time2);

        evaluateQuery(tx.query().has("name", "first event").orderBy("time", decr),
                ElementCategory.VERTEX, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC, "index1");
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "help").has(LABEL_NAME, "event"),
                ElementCategory.VERTEX, 1, new boolean[]{true, true}, "index2");

        clopen();

        Object v1Id = v1.id();
        Object v2Id = v2.id();

        evaluateQuery(tx.query().has("name", "first event").orderBy("time", decr),
                ElementCategory.VERTEX, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC, "index1");
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "help").has(LABEL_NAME, "event"),
                ElementCategory.VERTEX, 1, new boolean[]{true, true}, "index2");

        v1 = getV(tx, v1Id);
        v2 = getV(tx, v1Id);
        assertNotNull(v1);
        assertNotNull(v2);

        Thread.sleep(TimeUnit.MILLISECONDS.convert((long) Math.ceil(eventTTLSeconds * 1.25), TimeUnit.SECONDS));

        clopen();

        Thread.sleep(TimeUnit.MILLISECONDS.convert((long) Math.ceil(eventTTLSeconds * 1.25), TimeUnit.SECONDS));

        evaluateQuery(tx.query().has("text", Text.CONTAINS, "help").has(LABEL_NAME, "event"),
                ElementCategory.VERTEX, 0, new boolean[]{true, true}, "index2");
        evaluateQuery(tx.query().has("name", "first event").orderBy("time", decr),
                ElementCategory.VERTEX, 0, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC, "index1");


        v1 = getV(tx, v1Id);
        v2 = getV(tx, v2Id);
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
        final int likesTTLSeconds = (int) TestGraphConfigs.getTTL(TimeUnit.SECONDS);
        mgmt.setTTL(label, Duration.ofSeconds(likesTTLSeconds));

        mgmt.buildIndex("index1", Edge.class).
                addKey(name, getStringMapping()).addKey(time).buildMixedIndex(INDEX);
        mgmt.buildIndex("index2", Edge.class).indexOnly(label).
                addKey(text, getTextMapping()).buildMixedIndex(INDEX);

        assertEquals(Duration.ZERO, mgmt.getTTL(name));
        assertEquals(Duration.ofSeconds(likesTTLSeconds), mgmt.getTTL(label));
        finishSchema();

        JanusGraphVertex v1 = tx.addVertex(), v2 = tx.addVertex(), v3 = tx.addVertex();

        Edge e1 = v1.addEdge("likes", v2, "name", "v1 likes v2", "text", "this will help to identify the edge");
        long time1 = System.currentTimeMillis();
        e1.property("time", time1);
        Edge e2 = v2.addEdge("likes", v3, "name", "v2 likes v3", "text", "this won't match anything");
        long time2 = time1 + 1;
        e2.property("time", time2);

        tx.commit();

        clopen();
        Object e1Id = e1.id();
        e2.id();

        evaluateQuery(tx.query().has("text", Text.CONTAINS, "help").has(LABEL_NAME, "likes"),
                ElementCategory.EDGE, 1, new boolean[]{true, true}, "index2");
        evaluateQuery(tx.query().has("name", "v2 likes v3").orderBy("time", decr),
                ElementCategory.EDGE, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC, "index1");
        v1 = getV(tx, v1.id());
        v2 = getV(tx, v2.id());
        v3 = getV(tx, v3.id());
        e1 = getE(tx, e1Id);
        e2 = getE(tx, e1Id);
        assertNotNull(v1);
        assertNotNull(v2);
        assertNotNull(v3);
        assertNotNull(e1);
        assertNotNull(e2);
        assertNotEmpty(v1.query().direction(Direction.OUT).edges());
        assertNotEmpty(v2.query().direction(Direction.OUT).edges());


        Thread.sleep(TimeUnit.MILLISECONDS.convert((long) Math.ceil(likesTTLSeconds * 1.25), TimeUnit.SECONDS));
        clopen();

        // ...indexes have expired
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "help").has(LABEL_NAME, "likes"),
                ElementCategory.EDGE, 0, new boolean[]{true, true}, "index2");
        evaluateQuery(tx.query().has("name", "v2 likes v3").orderBy("time", decr),
                ElementCategory.EDGE, 0, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC, "index1");

        v1 = getV(tx, v1.id());
        v2 = getV(tx, v2.id());
        v3 = getV(tx, v3.id());
        e1 = getE(tx, e1Id);
        e2 = getE(tx, e1Id);
        assertNotNull(v1);
        assertNotNull(v2);
        assertNotNull(v3);
        // edges have expired from the graph...
        assertNull(e1);
        assertNull(e2);
        assertEmpty(v1.query().direction(Direction.OUT).edges());
        assertEmpty(v2.query().direction(Direction.OUT).edges());
    }

   /* ==================================================================================
                            SPECIAL CONCURRENT UPDATE CASES
     ==================================================================================*/

    /**
     * Create a vertex with an indexed property and commit. Open two new
     * transactions; delete vertex in one and delete just the property in the
     * other, then commit in the same order. Neither commit throws an exception.
     * @throws BackendException
     */
    @Test
    public void testDeleteVertexThenDeleteProperty() throws BackendException {
        testNestedWrites("x", null);
    }

    /**
     * Create a vertex and commit. Open two new transactions; delete vertex in
     * one and add an indexed property in the other, then commit in the same
     * order. Neither commit throws an exception.
     * @throws BackendException
     */
    @Test
    public void testDeleteVertexThenAddProperty() throws BackendException {
        testNestedWrites(null, "y");
    }

    /**
     * Create a vertex with an indexed property and commit. Open two new
     * transactions; delete vertex in one and modify the property in the other,
     * then commit in the same order. Neither commit throws an exception.
     * @throws BackendException
     */
    @Test
    public void testDeleteVertexThenModifyProperty() throws BackendException {
        testNestedWrites("x", "y");
    }

    @Test
    public void testIndexQueryWithScore() throws InterruptedException {
        PropertyKey textKey = mgmt.makePropertyKey("text").dataType(String.class).make();
        mgmt.buildIndex("store1", Vertex.class).addKey(textKey).buildMixedIndex(INDEX);
        mgmt.commit();

        JanusGraphVertex v1 = tx.addVertex();
        JanusGraphVertex v2 = tx.addVertex();
        JanusGraphVertex v3 = tx.addVertex();

        v1.property("text", "Hello Hello Hello Hello Hello Hello Hello Hello world");
        v2.property("text", "Hello abab abab fsdfsd sfdfsd sdffs fsdsdf fdf fsdfsd aera fsad abab abab fsdfsd sfdf");
        v3.property("text", "Hello Hello world world");

        tx.commit();

        Set<Double> scores = new HashSet<Double>();
        for (JanusGraphIndexQuery.Result<JanusGraphVertex> r : graph.indexQuery("store1", "v.text:(Hello)").vertices()) {
            scores.add(r.getScore());
        }

        Assert.assertEquals(3, scores.size());
    }

    @Test
    // this tests a case when there as AND with a single CONTAINS condition inside AND(name:(was here))
    // which (in case of Solr) spans multiple conditions such as AND(AND(name:was, name:here))
    // so we need to make sure that we don't apply AND twice.
    public void testContainsWithMultipleValues() throws Exception {
        PropertyKey name = makeKey("name", String.class);

        mgmt.buildIndex("store1", Vertex.class).addKey(name).buildMixedIndex(INDEX);
        mgmt.commit();

        JanusGraphVertex v1 = tx.addVertex();
        v1.property("name", "hercules was here");

        tx.commit();

        JanusGraphVertex r = Iterables.<JanusGraphVertex>get(graph.query().has("name", Text.CONTAINS, "hercules here").vertices(), 0);
        Assert.assertEquals(r.property("name").value(), "hercules was here");
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
                log.info("Skipping " + getClass().getSimpleName() + "." + methodName.getMethodName());
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

        JanusGraphVertex v = graph.addVertex();
        if (null != initialValue)
            v.property(VertexProperty.Cardinality.single, propName, initialValue);
        graph.tx().commit();

        Object id = v.id();

        // Open two transactions and modify the same vertex
        JanusGraphTransaction vertexDeleter = graph.newTransaction();
        JanusGraphTransaction propDeleter = graph.newTransaction();

        getV(vertexDeleter, id).remove();
        if (null == updatedValue)
            getV(propDeleter, id).property(propName).remove();
        else
            getV(propDeleter, id).property(VertexProperty.Cardinality.single, propName, updatedValue);

        vertexDeleter.commit();
        propDeleter.commit();

        // The vertex must not exist after deletion
        graph.tx().rollback();
        assertEquals(null, getV(graph, id));
        assertEmpty(graph.query().has(propName).vertices());
        if (null != updatedValue)
            assertEmpty(graph.query().has(propName, updatedValue).vertices());
        graph.tx().rollback();
    }

    /**
     * Tests indexing using _all virtual field
     */
    @Test
    public void testWidcardQuery() {
        if (supportsWildcardQuery()) {
            PropertyKey p1 = makeKey("p1", String.class);
            PropertyKey p2 = makeKey("p2", String.class);
            mgmt.buildIndex("mixedIndex", Vertex.class).addKey(p1).addKey(p2).buildMixedIndex(INDEX);

            finishSchema();
            clopen();

            JanusGraphVertex v1 = graph.addVertex();
            v1.property("p1", "test1");
            v1.property("p2", "test2");

            clopen();//Flush the index
            assertEquals(v1, graph.indexQuery("mixedIndex", "v.*:\"test1\"").vertices().iterator().next().getElement());
            assertEquals(v1, graph.indexQuery("mixedIndex", "v.*:\"test2\"").vertices().iterator().next().getElement());
        }

    }


    /**
     * Tests indexing lists
     */
    @Test
    public void testListIndexing() {
        testIndexing(Cardinality.LIST);
    }

    protected abstract boolean supportsCollections();

    /**
     * Tests indexing sets
     */
    @Test
    public void testSetIndexing() {
        testIndexing(Cardinality.SET);
    }


    private void testIndexing(Cardinality cardinality) {
        if (supportsCollections()) {
            PropertyKey stringProperty = mgmt.makePropertyKey("name").dataType(String.class).cardinality(cardinality).make();
            PropertyKey intProperty = mgmt.makePropertyKey("age").dataType(Integer.class).cardinality(cardinality).make();
            PropertyKey longProperty = mgmt.makePropertyKey("long").dataType(Long.class).cardinality(cardinality).make();
            PropertyKey uuidProperty = mgmt.makePropertyKey("uuid").dataType(UUID.class).cardinality(cardinality).make();
            PropertyKey geopointProperty = mgmt.makePropertyKey("geopoint").dataType(Geoshape.class).cardinality(cardinality).make();
            mgmt.buildIndex("collectionIndex", Vertex.class).addKey(stringProperty, getStringMapping()).addKey(intProperty).addKey(longProperty).addKey(uuidProperty).addKey(geopointProperty).buildMixedIndex(INDEX);

            finishSchema();
            testCollection(cardinality, "name", "Totoro", "Hiro");
            testCollection(cardinality, "age", 1, 2);
            testCollection(cardinality, "long", 1L, 2L);
            testCollection(cardinality, "geopoint", Geoshape.point(1.0, 1.0), Geoshape.point(2.0, 2.0));
            String backend = readConfig.get(INDEX_BACKEND, INDEX);
            // Solr 6 has issues processing UUIDs with Multivalues 
            // https://issues.apache.org/jira/browse/SOLR-11264
            if (!"solr".equals(backend)) {
                testCollection(cardinality, "uuid", UUID.randomUUID(), UUID.randomUUID());
            }
        } else {
            try {
                PropertyKey stringProperty = mgmt.makePropertyKey("name").dataType(String.class).cardinality(cardinality).make();
                //This should throw an exception
                mgmt.buildIndex("collectionIndex", Vertex.class).addKey(stringProperty, getStringMapping()).buildMixedIndex(INDEX);
                Assert.fail("Should have thrown an exception");
            } catch (JanusGraphException e) {

            }
        }
    }

    private void testCollection(Cardinality cardinality, String property, Object value1, Object value2) {
        clopen();

        Vertex v1 = graph.addVertex();

        //Adding properties one at a time
        v1.property(property, value1);
        clopen();//Flush the index
        assertEquals(v1, getOnlyElement(graph.query().has(property, value1).vertices()));

        v1 = getV(graph, v1.id());
        v1.property(property, value2);

        assertEquals(v1, getOnlyElement(graph.query().has(property, value1).vertices()));
        assertEquals(v1, getOnlyElement(graph.query().has(property, value2).vertices()));
        clopen();//Flush the index
        assertEquals(v1, getOnlyElement(graph.query().has(property, value1).vertices()));
        assertEquals(v1, getOnlyElement(graph.query().has(property, value2).vertices()));

        //Remove the properties
        v1 = getV(graph, v1.id());
        v1.properties(property).forEachRemaining(p -> {
            if (p.value().equals(value1)) {
                p.remove();
            }
        });

        assertFalse(graph.query().has(property, value1).vertices().iterator().hasNext());
        assertEquals(v1, getOnlyElement(graph.query().has(property, value2).vertices()));
        clopen();//Flush the index
        assertEquals(v1, getOnlyElement(graph.query().has(property, value2).vertices()));
        assertFalse(graph.query().has(property, value1).vertices().iterator().hasNext());

        //Re add the properties
        v1 = getV(graph, v1.id());
        v1.property(property, value1);
        assertEquals(v1, getOnlyElement(graph.query().has(property, value1).vertices()));
        assertEquals(v1, getOnlyElement(graph.query().has(property, value2).vertices()));
        clopen();//Flush the index
        assertEquals(v1, getOnlyElement(graph.query().has(property, value1).vertices()));
        assertEquals(v1, getOnlyElement(graph.query().has(property, value2).vertices()));

        //Add a duplicate property
        v1 = getV(graph, v1.id());
        v1.property(property, value1);


        assertEquals(Cardinality.SET.equals(cardinality) ? 2 : 3, Iterators.size(getOnlyVertex(graph.query().has(property, value1)).properties(property)));
        clopen();//Flush the index
        assertEquals(Cardinality.SET.equals(cardinality) ? 2 : 3, Iterators.size(getOnlyVertex(graph.query().has(property, value1)).properties(property)));


        //Add two properties at once to a fresh vertex
        graph.vertices().forEachRemaining(v -> v.remove());
        v1 = graph.addVertex();
        v1.property(property, value1);
        v1.property(property, value2);

        assertEquals(v1, getOnlyElement(graph.query().has(property, value1).vertices()));
        assertEquals(v1, getOnlyElement(graph.query().has(property, value2).vertices()));
        clopen();//Flush the index
        assertEquals(v1, getOnlyElement(graph.query().has(property, value1).vertices()));
        assertEquals(v1, getOnlyElement(graph.query().has(property, value2).vertices()));

        //If this is a geo test then try a within query
        if (value1 instanceof Geoshape) {
            assertEquals(v1, getOnlyElement(graph.query().has(property, Geo.WITHIN, Geoshape.circle(1.0, 1.0, 0.1)).vertices()));
            assertEquals(v1, getOnlyElement(graph.query().has(property, Geo.WITHIN, Geoshape.circle(2.0, 2.0, 0.1)).vertices()));
        }

        // Test traversal property drop Issue #408
        GraphTraversalSource g = graph.traversal();
        g.V().drop().iterate();
        clopen(); // Flush the index
        g = graph.traversal();
        v1 = g.addV().property(property, value1).property(property, value2).next();
        Vertex v2 = g.addV().property(property, value1).property(property, value2).next();
        clopen(); // Flush the index
        g = graph.traversal();
        assertEquals(2, g.V().has(property, value1).toList().size());
        g.V().properties().drop().iterate();
        clopen(); // Flush the index
        g = graph.traversal();
        assertFalse(g.V().has(property, value1).hasNext());
        assertFalse(g.V().has(property, value2).hasNext());
    }

    private void testGeo(int i, int origNumV, int numV, String geoPointProperty, String geoShapeProperty) {
        double offset = (i * 50.0 / origNumV);
        double bufferKm = 20;
        double distance = Geoshape.point(0.0, 0.0).getPoint().distance(Geoshape.point(offset, offset).getPoint()) + bufferKm;

        assertCount(i + 1, tx.query().has(geoPointProperty, Geo.WITHIN, Geoshape.circle(0.0, 0.0, distance)).vertices());
        assertCount(i + 1, tx.query().has(geoPointProperty, Geo.WITHIN, Geoshape.circle(0.0, 0.0, distance)).edges());
        assertCount(i + 1, tx.query().has(geoPointProperty, Geo.INTERSECT, Geoshape.circle(0.0, 0.0, distance)).vertices());
        assertCount(i + 1, tx.query().has(geoPointProperty, Geo.INTERSECT, Geoshape.circle(0.0, 0.0, distance)).edges());
        assertCount(numV-(i + 1), tx.query().has(geoPointProperty, Geo.DISJOINT, Geoshape.circle(0.0, 0.0, distance)).vertices());
        assertCount(numV-(i + 1), tx.query().has(geoPointProperty, Geo.DISJOINT, Geoshape.circle(0.0, 0.0, distance)).edges());
        assertCount(i + 1, tx.query().has(geoShapeProperty, Geo.INTERSECT, Geoshape.circle(0.0, 0.0, distance)).vertices());
        assertCount(i + 1, tx.query().has(geoShapeProperty, Geo.INTERSECT, Geoshape.circle(0.0, 0.0, distance)).edges());
        if (i > 0) {
            assertCount(i, tx.query().has(geoShapeProperty, Geo.WITHIN, Geoshape.circle(0.0, 0.0, distance-bufferKm)).vertices());
            assertCount(i, tx.query().has(geoShapeProperty, Geo.WITHIN, Geoshape.circle(0.0, 0.0, distance-bufferKm)).edges());
        }
        assertCount(numV-(i + 1), tx.query().has(geoShapeProperty, Geo.DISJOINT, Geoshape.circle(0.0, 0.0, distance)).vertices());
        assertCount(numV-(i + 1), tx.query().has(geoShapeProperty, Geo.DISJOINT, Geoshape.circle(0.0, 0.0, distance)).edges());

        if (indexFeatures.supportsGeoContains()) {
            assertCount(i % 2, tx.query().has(geoShapeProperty, Geo.CONTAINS, Geoshape.point(-offset, -offset)).vertices());
            assertCount(i % 2, tx.query().has(geoShapeProperty, Geo.CONTAINS, Geoshape.point(-offset, -offset)).edges());
        }

        double buffer = bufferKm/111.;
        double min = -Math.abs(offset);
        double max = Math.abs(offset);
        Geoshape bufferedBox = Geoshape.box(min-buffer, min-buffer, max+buffer, max+buffer);
        assertCount(i + 1, tx.query().has(geoPointProperty, Geo.WITHIN, bufferedBox).vertices());
        assertCount(i + 1, tx.query().has(geoPointProperty, Geo.WITHIN, bufferedBox).edges());
        assertCount(i + 1, tx.query().has(geoPointProperty, Geo.INTERSECT, bufferedBox).vertices());
        assertCount(i + 1, tx.query().has(geoPointProperty, Geo.INTERSECT, bufferedBox).edges());
        assertCount(numV-(i + 1), tx.query().has(geoPointProperty, Geo.DISJOINT, bufferedBox).vertices());
        assertCount(numV-(i + 1), tx.query().has(geoPointProperty, Geo.DISJOINT, bufferedBox).edges());
        if (i > 0) {
            Geoshape exactBox = Geoshape.box(min, min, max, max);
            assertCount(i, tx.query().has(geoShapeProperty, Geo.WITHIN, exactBox).vertices());
            assertCount(i, tx.query().has(geoShapeProperty, Geo.WITHIN, exactBox).edges());
        }
        assertCount(i + 1, tx.query().has(geoShapeProperty, Geo.INTERSECT, bufferedBox).vertices());
        assertCount(i + 1, tx.query().has(geoShapeProperty, Geo.INTERSECT, bufferedBox).edges());
        assertCount(numV-(i + 1), tx.query().has(geoShapeProperty, Geo.DISJOINT, bufferedBox).vertices());
        assertCount(numV-(i + 1), tx.query().has(geoShapeProperty, Geo.DISJOINT, bufferedBox).edges());

        Geoshape bufferedPoly = Geoshape.polygon(Arrays.asList(new double[][]
                {{min-buffer,min-buffer},{max+buffer,min-buffer},{max+buffer,max+buffer},{min-buffer,max+buffer},{min-buffer,min-buffer}}));
        if (i > 0) {
            Geoshape exactPoly = Geoshape.polygon(Arrays.asList(new double[][]
                    {{min,min},{max,min},{max,max},{min,max},{min,min}}));
            assertCount(i, tx.query().has(geoShapeProperty, Geo.WITHIN, exactPoly).vertices());
            assertCount(i, tx.query().has(geoShapeProperty, Geo.WITHIN, exactPoly).edges());
        }
        assertCount(i + 1, tx.query().has(geoShapeProperty, Geo.INTERSECT, bufferedPoly).vertices());
        assertCount(i + 1, tx.query().has(geoShapeProperty, Geo.INTERSECT, bufferedPoly).edges());
        assertCount(numV-(i + 1), tx.query().has(geoShapeProperty, Geo.DISJOINT, bufferedPoly).vertices());
        assertCount(numV-(i + 1), tx.query().has(geoShapeProperty, Geo.DISJOINT, bufferedPoly).edges());
    }

    @Test
    public void shouldAwaitMultipleStatuses() throws InterruptedException, ExecutionException {
        PropertyKey key1 = makeKey("key1", String.class);
        JanusGraphIndex index = mgmt.buildIndex("randomMixedIndex", Vertex.class).addKey(key1).buildMixedIndex(INDEX);
        if (index.getIndexStatus(key1) == SchemaStatus.INSTALLED) {
            mgmt.updateIndex(mgmt.getGraphIndex("randomMixedIndex"), SchemaAction.REGISTER_INDEX).get();
            mgmt.updateIndex(mgmt.getGraphIndex("randomMixedIndex"), SchemaAction.ENABLE_INDEX).get();
        } else if (index.getIndexStatus(key1) == SchemaStatus.REGISTERED) {
            mgmt.updateIndex(mgmt.getGraphIndex("randomMixedIndex"), SchemaAction.ENABLE_INDEX).get();
        }
        PropertyKey key2 = makeKey("key2", String.class);
        mgmt.addIndexKey(index, key2);
        mgmt.commit();
        //key1 now has status ENABLED, let's ensure we can watch for REGISTERED and ENABLED
        try {
            ManagementSystem.awaitGraphIndexStatus(graph, "randomMixedIndex").status(SchemaStatus.REGISTERED, SchemaStatus.ENABLED).call();
        } catch (Exception e) {
            Assert.fail("Failed to awaitGraphIndexStatus on multiple statuses.");
        }
    }
}
