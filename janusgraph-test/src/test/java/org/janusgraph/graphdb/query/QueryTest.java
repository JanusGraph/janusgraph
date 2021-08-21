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
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.attribute.Contain;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.inmemory.InMemoryStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.P.not;
import static org.janusgraph.testutil.JanusGraphAssert.assertBackendHit;
import static org.janusgraph.testutil.JanusGraphAssert.assertCount;
import static org.janusgraph.testutil.JanusGraphAssert.assertNoBackendHit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class QueryTest {

    private JanusGraph graph;
    private JanusGraphTransaction tx;

    @BeforeEach
    public void setup() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, InMemoryStoreManager.class.getCanonicalName());
        graph = JanusGraphFactory.open(config);
        tx = graph.newTransaction();
    }

    @AfterEach
    public void shutdown() {
        if (tx!=null && tx.isOpen()) tx.commit();
        if (graph!=null && graph.isOpen()) graph.close();
    }

    private String getQueryAnnotation(TraversalMetrics metrics) {
        Object[] nestedArr = metrics.getMetrics(0).getNested().toArray();
        // the last nested group is the actual backend query
        return (String) ((Metrics) nestedArr[nestedArr.length - 1]).getAnnotation(QueryProfiler.QUERY_ANNOTATION);
    }

    @Test
    public void testQueryLimitAdjustment() {
        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey prop1Key = mgmt.makePropertyKey("prop1").dataType(String.class).make();
        PropertyKey prop2Key = mgmt.makePropertyKey("prop2").dataType(String.class).make();
        PropertyKey prop3Key = mgmt.makePropertyKey("prop3").dataType(String.class).make();
        PropertyKey prop4Key = mgmt.makePropertyKey("prop4").dataType(String.class).make();

        mgmt.buildIndex("prop1_idx", Vertex.class).addKey(prop1Key).buildCompositeIndex();
        mgmt.buildIndex("props_idx", Vertex.class).addKey(prop1Key).addKey(prop2Key).buildCompositeIndex();

        mgmt.commit();

        for (int i = 0; i < 20; i++) {
            tx.addVertex().property("prop1", "prop1val").element().property("prop2", "prop2val")
                .element().property("prop3", "prop3val").element().property("prop4", "prop4val");
        }
        tx.commit();

        // Single condition, fully met by index prop1_idx. No need to adjust backend query limit.
        assertCount(5, graph.traversal().V().has("prop1", "prop1val").limit(5));
        assertEquals("multiKSQ[1]@5", getQueryAnnotation(graph.traversal().V().has("prop1", "prop1val").limit(5)
                .profile().next()));

        // Two conditions, fully met by index props_idx. No need to adjust backend query limit.
        assertCount(5, graph.traversal().V().has("prop1", "prop1val").has("prop2", "prop2val").limit(5));
        assertEquals("multiKSQ[1]@5", getQueryAnnotation(graph.traversal().V().has("prop1", "prop1val").has("prop2", "prop2val").limit(5)
            .profile().next()));

        // Two conditions, one of which met by index prop1_idx. Multiply original limit by two for sake of in-memory filtering.
        assertCount(5, graph.traversal().V().has("prop1", "prop1val").has("prop3", "prop3val").limit(5));
        assertEquals("multiKSQ[1]@10", getQueryAnnotation(graph.traversal().V().has("prop1", "prop1val").has("prop3", "prop3val").limit(5)
            .profile().next()));

        // Three conditions, one of which met by index prop1_idx. Multiply original limit by four for sake of in-memory filtering.
        assertCount(5, graph.traversal().V().has("prop1", "prop1val").has("prop3", "prop3val").has("prop4", "prop4val").limit(5));
        assertEquals("multiKSQ[1]@20", getQueryAnnotation(graph.traversal().V().has("prop1", "prop1val").has("prop3", "prop3val").has("prop4", "prop4val").limit(5)
            .profile().next()));
    }

    @Test
    public void testMultipleKeysQuery() {
        tx.makePropertyKey("name").dataType(String.class).make();
        tx.addVertex("name","vertex1");
        tx.addVertex("name","vertex2");
        tx.addVertex("name","vertex3");

        int found = Iterators.size(tx.query().has("name", Contain.IN, Collections.singletonList("vertex1")).vertices().iterator());
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
        } catch (IllegalArgumentException ignored) {}
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

        assertEquals(0, ol3.compare(v1, v2));
        assertTrue(ol3.compare(v2,v3)>0);
        assertTrue(ol3.compare(v1,v3)>0);

    }

    @Test
    public void testIndexQueryCache() throws Exception {
        JanusGraphManagement mgmt = graph.openManagement();
        final PropertyKey prop = mgmt.makePropertyKey("prop").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        final JanusGraphIndex index = mgmt.buildIndex("index", Vertex.class).addKey(prop).buildCompositeIndex();
        mgmt.commit();

        // cache is used when there is no result for given query
        assertBackendHit(graph.traversal().V().has("prop", "value").profile().next());
        assertNoBackendHit(graph.traversal().V().has("prop", "value").profile().next());
        assertEquals(0, graph.traversal().V().has("prop", "value").toList().size());
        graph.tx().rollback();

        for (int i = 0; i < 100; i++) {
            tx.addVertex("prop", "value");
        }
        tx.commit();

        // cache is used when there are results for given query
        assertBackendHit(graph.traversal().V().has("prop", "value").profile().next());
        assertNoBackendHit(graph.traversal().V().has("prop", "value").profile().next());
        assertEquals(100, graph.traversal().V().has("prop", "value").toList().size());
        graph.tx().rollback();

        // cache is used with limit
        assertBackendHit(graph.traversal().V().has("prop", "value").limit(10).profile().next());
        assertNoBackendHit(graph.traversal().V().has("prop", "value").limit(10).profile().next());
        assertEquals(10, graph.traversal().V().has("prop", "value").limit(10).toList().size());
        graph.tx().rollback();

        // result is cached and cache is used with limit larger than number of possible results
        assertBackendHit(graph.traversal().V().has("prop", "value").limit(1000).profile().next());
        assertNoBackendHit(graph.traversal().V().has("prop", "value").limit(1000).profile().next());
        assertEquals(100, graph.traversal().V().has("prop", "value").limit(1000).toList().size());
        graph.tx().rollback();

        // cache is not used when second query has higher limit
        assertBackendHit(graph.traversal().V().has("prop", "value").limit(10).profile().next());
        assertBackendHit(graph.traversal().V().has("prop", "value").limit(11).profile().next());
        assertEquals(11, graph.traversal().V().has("prop", "value").limit(11).toList().size());
        graph.tx().rollback();

        // cache is used when first query exhausts all results
        assertBackendHit(graph.traversal().V().has("prop", "value").limit(200).profile().next());
        assertNoBackendHit(graph.traversal().V().has("prop", "value").limit(1000).profile().next());
        assertEquals(100, graph.traversal().V().has("prop", "value").limit(1000).toList().size());
        graph.tx().rollback();

        assertBackendHit(graph.traversal().V().has("prop", "value").profile().next());
        assertNoBackendHit(graph.traversal().V().has("prop", "value").limit(1000).profile().next());
        assertEquals(100, graph.traversal().V().has("prop", "value").limit(1000).toList().size());
        graph.tx().rollback();

        // cache is used when second query has lower limit
        assertBackendHit(graph.traversal().V().has("prop", "value").limit(10).profile().next());
        assertNoBackendHit(graph.traversal().V().has("prop", "value").limit(9).profile().next());
        assertEquals(9, graph.traversal().V().has("prop", "value").limit(9).toList().size());
        graph.tx().rollback();

        // incomplete results are not put in cache if iterator is not exhausted
        GraphTraversal<Vertex, Vertex> iter = graph.traversal().V().has("prop", "value");
        for (int i = 0; i < 10; i++) {
            iter.next();
        }
        iter.close();
        assertBackendHit(graph.traversal().V().has("prop", "value").limit(1).profile().next());
        graph.tx().rollback();

        try (GraphTraversal<Vertex, Vertex> it = graph.traversal().V().has("prop", "value")) {
            for (int i = 0; i < 10; i++) {
                it.next();
            }
        }
        assertBackendHit(graph.traversal().V().has("prop", "value").limit(1).profile().next());
        graph.tx().rollback();

        // complete results are put in cache if iterator is exhausted
        iter = graph.traversal().V().has("prop", "value");
        while (iter.hasNext()) {
            iter.next();
        }
        assertNoBackendHit(graph.traversal().V().has("prop", "value").limit(1).profile().next());
        graph.tx().rollback();

        iter = graph.traversal().V().has("prop", "value");
        for (int i = 0; i < 100; i++) {
            iter.next();
        }
        assertNoBackendHit(graph.traversal().V().has("prop", "value").limit(1).profile().next());
        graph.tx().rollback();
    }

    @Test
    public void testMultipleIndexQueryWithLimits() {
        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey prop1Key = mgmt.makePropertyKey("prop1").dataType(String.class).make();
        PropertyKey prop2Key = mgmt.makePropertyKey("prop2").dataType(String.class).make();

        mgmt.buildIndex("prop1_idx", Vertex.class).addKey(prop1Key).buildCompositeIndex();
        mgmt.buildIndex("prop2_idx", Vertex.class).addKey(prop2Key).buildCompositeIndex();

        mgmt.commit();

        // Creates 20 vertices with prop1=prop1val1, prop2=prop2val1
        for(int i=0; i<20; i++)
        {
            tx.addVertex().property("prop1", "prop1val1").element().property("prop2", "prop2val1");
        }
        // Creates an additional vertex with prop1=prop1val1, prop2=prop2val2
        tx.addVertex().property("prop1", "prop1val1").element().property("prop2", "prop2val2");

        tx.commit();

        List<Vertex> res;

        // Tests that queries for the single vertex containing prop1=prop1val1, prop2=prop2val2, are returned when limit(1) is applied

        // Tests that single vertex containing prop1=prop1val1, prop2=prop2val2 is returned when indices are not used
        res = graph.traversal().V().map(x -> x.get()).has("prop2", "prop2val2").has("prop1", "prop1val1").limit(1).toList();
        assertEquals(1, res.size());

        // Tests that single vertex containing prop1=prop1val1, prop2=prop2val2 is returned when only prop1 index is used
        res = graph.traversal().V().has("prop1", "prop1val1").map(x -> x.get()).has("prop2", "prop2val2").limit(1).toList();
        assertEquals(1, res.size());

        // Tests that single vertex containing prop1=prop1val1, prop2=prop2val2 is returned when only prop2 index is used
        res = graph.traversal().V().has("prop2", "prop2val2").map(x -> x.get()).has("prop1", "prop1val1").limit(1).toList();
        assertEquals(1, res.size());

        // Tests that JanusGraphStep strategy properly combines has() steps to use both indices
        // Tests without limits
        res = graph.traversal().V().has("prop1", "prop1val1").has("prop2", "prop2val2").toList();
        assertEquals(1, res.size());
        // Tests with limit
        res = graph.traversal().V().has("prop1", "prop1val1").has("prop2", "prop2val2").limit(1).toList();
        assertEquals(1, res.size());
        res = graph.traversal().V().has("prop2", "prop2val2").has("prop1", "prop1val1").limit(1).toList();
        assertEquals(1, res.size());
    }

    @Test
    public void testFuzzyMatchWithoutIndex() {
        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.commit();

        tx.addVertex().property("name", "some value");
        tx.commit();

        // Exact match
        assertEquals(1, graph.traversal().V().has("name", Text.textFuzzy("some value")).count().next());
        assertEquals(1, graph.traversal().V().has("name", Text.textContainsFuzzy("value")).count().next());

        // One character different
        assertEquals(1, graph.traversal().V().has("name", Text.textFuzzy("some values")).count().next());
        assertEquals(1, graph.traversal().V().has("name", Text.textContainsFuzzy("values")).count().next());

        // Two characters different
        assertEquals(1, graph.traversal().V().has("name", Text.textFuzzy("some val")).count().next());
        assertEquals(1, graph.traversal().V().has("name", Text.textContainsFuzzy("values!")).count().next());

        // Three characters different
        assertEquals(0, graph.traversal().V().has("name", Text.textFuzzy("some Val")).count().next());
        assertEquals(0, graph.traversal().V().has("name", Text.textContainsFuzzy("valuable")).count().next());
    }

    @Test
    public void testTextContainsPhraseWithoutIndex() {
        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.commit();

        tx.addVertex().property("name", "some value");
        tx.addVertex().property("name", "other value");
        tx.commit();

        assertEquals(2, graph.traversal().V().has("name", Text.textContainsPhrase("value")).count().next());
        assertEquals(1, graph.traversal().V().has("name", Text.textContainsPhrase("other value")).count().next());
        assertEquals(0, graph.traversal().V().has("name", Text.textContainsPhrase("final value")).count().next());
    }

    @Test
    public void testTextNegatedWithoutIndex() {
        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.commit();

        tx.addVertex().property("name", "some value");
        tx.addVertex().property("name", "other value");
        tx.commit();

        // Text.textNotFuzzy
        assertEquals(1, graph.traversal().V().has("name", Text.textNotFuzzy("other values")).count().next());
        assertEquals(2, graph.traversal().V().has("name", Text.textNotFuzzy("final values")).count().next());

        // Text.textNotRegex
        assertEquals(0, graph.traversal().V().has("name", Text.textNotRegex(".*value")).count().next());
        assertEquals(1, graph.traversal().V().has("name", Text.textNotRegex("other.*")).count().next());
        assertEquals(2, graph.traversal().V().has("name", Text.textNotRegex("final.*")).count().next());

        // Text.textNotPrefix
        assertEquals(1, graph.traversal().V().has("name", Text.textNotPrefix("other")).count().next());
        assertEquals(2, graph.traversal().V().has("name", Text.textNotPrefix("final")).count().next());

        // Text.textNotContains
        assertEquals(0, graph.traversal().V().has("name", Text.textNotContains("value")).count().next());
        assertEquals(1, graph.traversal().V().has("name", Text.textNotContains("other")).count().next());
        assertEquals(2, graph.traversal().V().has("name", Text.textNotContains("final")).count().next());

        // Text.textNotContainsFuzzy
        assertEquals(0, graph.traversal().V().has("name", Text.textNotContainsFuzzy("values")).count().next());
        assertEquals(1, graph.traversal().V().has("name", Text.textNotContainsFuzzy("others")).count().next());
        assertEquals(2, graph.traversal().V().has("name", Text.textNotContainsFuzzy("final")).count().next());

        // Text.textNotContainsRegex
        assertEquals(0, graph.traversal().V().has("name", Text.textNotContainsRegex("val.*")).count().next());
        assertEquals(1, graph.traversal().V().has("name", Text.textNotContainsRegex("oth.*")).count().next());
        assertEquals(2, graph.traversal().V().has("name", Text.textNotContainsRegex("fin.*")).count().next());

        // Text.textNotContainsPrefix
        assertEquals(0, graph.traversal().V().has("name", Text.textNotContainsPrefix("val")).count().next());
        assertEquals(1, graph.traversal().V().has("name", Text.textNotContainsPrefix("oth")).count().next());
        assertEquals(2, graph.traversal().V().has("name", Text.textNotContainsPrefix("final")).count().next());

        // Text.textNotContainsPhrase
        assertEquals(0, graph.traversal().V().has("name", Text.textNotContainsPhrase("value")).count().next());
        assertEquals(1, graph.traversal().V().has("name", Text.textNotContainsPhrase("other value")).count().next());
        assertEquals(2, graph.traversal().V().has("name", Text.textNotContainsPhrase("final value")).count().next());
    }

    @Test
    public void testComplexConditions() {
        GraphTraversalSource g = graph.traversal();
        Vertex bob = g.addV("person").property("name", "Bob").next();
        Vertex alice = g.addV("person").property("name", "Alice").next();
        Vertex book = g.addV("book").property("name", "book1").next();

        g.addE("knows").from(bob).to(alice).next();
        Edge edge2 = g.addE("write").from(alice).to(book).next();
        g.E(edge2).property("duration", new Double(0.2d)).iterate();

        // vertex centric queries
        assertFalse(g.E().inV().outE("write").has("duration", P.eq(0d).or(P.outside(0.1d, 0.3d))).hasNext());
        assertTrue(g.E().inV().outE("write").has("duration", P.eq(0.2d).or(P.outside(0.1d, 0.3d))).hasNext());
        assertTrue(g.E().inV().outE("write").or(__.has("duration", P.eq(0d)), __.has("duration", not(P.outside(0.1d, 0.3d)))).hasNext());
        assertTrue(g.E().inV().outE("write").has("duration", P.eq(0d).or(not(P.outside(0.1d, 0.3d)))).hasNext());
        assertTrue(g.E().inV().outE("write").has("duration", P.eq(0.2d).or(not(P.outside(0.3d, 0.4d)))).hasNext());
        assertFalse(g.E().inV().outE("write").has("duration", P.eq(0d).or(not(P.outside(0.3d, 0.4d)))).hasNext());

        // graph centric queries
        assertFalse(g.E().has("duration", P.eq(0d).or(P.outside(0.1d, 0.3d))).hasNext());
        assertTrue(g.E().has("duration", P.eq(0.2d).or(P.outside(0.1d, 0.3d))).hasNext());
        assertTrue(g.E().or(__.has("duration", P.eq(0d)), __.has("duration", not(P.outside(0.1d, 0.3d)))).hasNext());
        assertTrue(g.E().has("duration", P.eq(0d).or(not(P.outside(0.1d, 0.3d)))).hasNext());
        assertTrue(g.E().has("duration", P.eq(0.2d).or(not(P.outside(0.3d, 0.4d)))).hasNext());
        assertFalse(g.E().has("duration", P.eq(0d).or(not(P.outside(0.3d, 0.4d)))).hasNext());
    }
}
