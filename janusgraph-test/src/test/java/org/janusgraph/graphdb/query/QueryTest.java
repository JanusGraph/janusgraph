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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ProductiveByStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphQuery;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Contain;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.inmemory.InMemoryStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.process.traversal.Order.asc;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.desc;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.shuffle;
import static org.apache.tinkerpop.gremlin.process.traversal.P.not;
import static org.janusgraph.testutil.JanusGraphAssert.assertBackendHit;
import static org.janusgraph.testutil.JanusGraphAssert.assertCount;
import static org.janusgraph.testutil.JanusGraphAssert.assertNoBackendHit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class QueryTest {

    //Strings in the order of their code points, in which the index backends sort them: U+E000 to U+FFFF come before
    //the characters from U+10000, which String.compareTo puts first, as their first UTF-16 unit is a surrogate
    private static final String[] BY_CODE_POINT = {"a", "z", "\uD7FF", "\uE000", "\uE000a", "\uFFFF", "\uD800\uDC00",
        "\uD83D\uDE00", "\uD83D\uDE00a", "\uD83D\uDE01", "\uDBFF\uDC00", "\uDBFF\uDFFF"};

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
        assertEquals("multiKSQ[1]@5{KeySliceQuery(0x0A89A070726F70317661EC)[0x00,0xFF)}",
            getQueryAnnotation(graph.traversal().V().has("prop1", "prop1val").limit(5).profile().next()));

        // Two conditions, fully met by index props_idx. No need to adjust backend query limit.
        assertCount(5, graph.traversal().V().has("prop1", "prop1val").has("prop2", "prop2val").limit(5));
        assertEquals("multiKSQ[1]@5{KeySliceQuery(0x0C89A070726F70317661ECA070726F70327661EC)[0x00,0xFF)}", getQueryAnnotation(graph.traversal().V().has("prop1", "prop1val").has("prop2", "prop2val").limit(5)
            .profile().next()));

        // Two conditions, one of which met by index prop1_idx. Multiply original limit by two for sake of in-memory filtering.
        assertCount(5, graph.traversal().V().has("prop1", "prop1val").has("prop3", "prop3val").limit(5));
        assertEquals("multiKSQ[1]@10{KeySliceQuery(0x0A89A070726F70317661EC)[0x00,0xFF)}", getQueryAnnotation(graph.traversal().V().has("prop1", "prop1val").has("prop3", "prop3val").limit(5)
            .profile().next()));

        // Three conditions, one of which met by index prop1_idx. Multiply original limit by four for sake of in-memory filtering.
        assertCount(5, graph.traversal().V().has("prop1", "prop1val").has("prop3", "prop3val").has("prop4", "prop4val").limit(5));
        assertEquals("multiKSQ[1]@20{KeySliceQuery(0x0A89A070726F70317661EC)[0x00,0xFF)}", getQueryAnnotation(graph.traversal().V().has("prop1", "prop1val").has("prop3", "prop3val").has("prop4", "prop4val").limit(5)
            .profile().next()));
    }

    @Test
    public void testIdLookUpQuery() {
        Vertex v1 = tx.addVertex("pos", 0, "age", 30);
        Vertex v2 = tx.addVertex("age", 20);
        Edge e = v1.addEdge("connects", v2, "prop", "val");
        Object vId = v1.id();
        Object v2Id = v2.id();
        Object eId = e.id();
        tx.commit();

        tx = graph.newTransaction();

        // edges
        assertTrue(tx.traversal().E(eId).hasNext());
        assertFalse(tx.traversal().E(eId).hasId("wrong-id").hasNext());
        assertFalse(tx.traversal().E("wrong-id").hasId(eId).hasNext());
        assertFalse(tx.traversal().E(eId).has("prop", "non").hasNext());
        assertTrue(tx.traversal().E(eId).has("prop", "val").hasNext());
        assertTrue(tx.traversal().E().hasId(eId).hasNext());
        assertFalse(tx.traversal().E().hasId(eId).has("prop", "non").hasNext());
        assertTrue(tx.traversal().E().hasId(eId).has("prop", "val").hasNext());
        assertEquals(tx.traversal().E(eId).next(), tx.traversal().E().hasId(eId).next());
        assertEquals(tx.traversal().E(eId).next(), tx.traversal().E(eId).hasId(eId).next());

        // vertices
        assertTrue(tx.traversal().V(vId).hasNext());
        assertFalse(tx.traversal().V(vId).hasId(123).hasNext());
        assertFalse(tx.traversal().V(123).hasId(vId).hasNext());
        assertFalse(tx.traversal().V(vId).has("pos", 1).hasNext());
        assertTrue(tx.traversal().V(vId).has("pos", 0).hasNext());
        assertTrue(tx.traversal().V().hasId(vId).hasNext());
        assertFalse(tx.traversal().V().hasId(vId).has("pos", 1).hasNext());
        assertTrue(tx.traversal().V().hasId(vId).has("pos", 0).hasNext());
        assertEquals(tx.traversal().V(vId).next(), tx.traversal().V().hasId(vId).next());
        assertEquals(tx.traversal().V(vId).next(), tx.traversal().V(vId).hasId(vId).next());

        // vertices lookup followed by limit step
        assertEquals(1, tx.traversal().V(vId, v2Id).limit(1).count().next());
        assertEquals(1, tx.traversal().V(vId, v2Id).limit(1).toList().size());
        assertEquals(1, tx.traversal().V().hasId(vId, v2Id).limit(1).count().next());
        assertEquals(0, tx.traversal().V().hasId(vId, v2Id).limit(0).count().next());
        assertEquals(1, tx.traversal().V().has(T.id, P.within(vId, v2Id)).limit(1).count().next());
        assertEquals(0, tx.traversal().V().has(T.id, P.within(vId, v2Id)).limit(0).count().next());

        // vertices lookup followed by order step
        assertEquals(Arrays.asList(20, 30), tx.traversal().V().order().by("age").values("age").toList());
        assertEquals(Arrays.asList(20, 30), tx.traversal().V().has(T.id, P.within(vId, v2Id)).order().by("age").values("age").toList());
        assertEquals(Arrays.asList(20, 30), tx.traversal().V().has(T.id, P.within(vId, v2Id)).order().by("age").values("age").toList());
        assertEquals(Arrays.asList(20, 30), tx.traversal().V().hasId(vId, v2Id).order().by("age").values("age").toList());
        assertEquals(Arrays.asList(20, 30), tx.traversal().V().hasId(vId, v2Id).order().by("age").values("age").toList());

        // FIXME: vertices lookup in Or query
        // assertEquals(1, tx.traversal().V().or(__.hasId(vId), __.has("pos", 100)).count().next());
        // assertEquals(2, tx.traversal().V().or(__.has(T.id, vId), __.has(T.id, v2Id)).count().next());
        // assertEquals(2, tx.traversal().V().or(__.hasId(vId, v2Id), __.has("pos", 100)).count().next());
        // assertEquals(1, tx.traversal().V().or(__.hasId(vId, v2Id).limit(1), __.has("pos", 100)).count().next());
        // assertEquals(1, tx.traversal().V().or(__.hasId(vId, v2Id).limit(0), __.has("pos")).count().next());
        // assertEquals(1, tx.traversal().V().or(__.hasId(vId, v2Id).limit(1), __.has("pos")).count().next());
        // assertEquals(2, tx.traversal().V().or(__.hasId(vId, v2Id).limit(1), __.has("age")).count().next());
    }

    /**
     * TinkerPop runs each branch of an or() for each element on its own, so a limit() or range() in a branch lets the
     * element through, unless it skips it or its limit is 0, and an order() where each of its by(key) modulators has a
     * value. No index answers these or() steps, so JanusGraph queries each branch, or scans the vertices where the
     * traversal has several or() steps, and returns what a TinkerGraph which holds the same vertices does
     */
    @Test
    public void testOrBranchesCheckEachElementOnItsOwnAsTinkerGraphDoes() {
        tx.makePropertyKey("uid").dataType(Integer.class).make();
        tx.makePropertyKey("kind").dataType(String.class).make();
        tx.makePropertyKey("name").dataType(String.class).make();
        tx.makePropertyKey("age").dataType(Integer.class).make();
        final GraphTraversalSource reference = TinkerGraph.open().traversal();
        //uid, kind, name and age; every vertex has a name, and some have no age
        final Object[][] vertices = {{1, "p", "c", 5}, {2, "p", "a", 3}, {3, "p", "f", null}, {4, "p", "e", null},
            {5, "p", "g", 1}, {6, "q", "d", 7}, {7, "q", "h", null}, {8, "r", "j", 9}};
        for (final GraphTraversalSource g : Arrays.asList(tx.traversal(), reference)) {
            for (final Object[] vertex : vertices) {
                final GraphTraversal<Vertex, Vertex> added = g.addV().property("uid", vertex[0])
                    .property("kind", vertex[1]).property("name", vertex[2]);
                (vertex[3] == null ? added : added.property("age", vertex[3])).iterate();
            }
        }
        tx.commit();
        tx = graph.newTransaction();
        final List<Function<GraphTraversalSource, GraphTraversal<?, ?>>> unordered = Arrays.asList(
            g -> g.V().or(__.has("kind", "p").limit(1), __.has("kind", "q")),
            g -> g.V().or(__.has("kind", "q"), __.has("kind", "p").limit(1)),
            g -> g.V().or(__.has("kind", "p").range(1, 3), __.has("kind", "q")),
            g -> g.V().or(__.has("kind", "p").order().by("age", desc).limit(2), __.has("kind", "q")),
            g -> g.V().or(__.has("kind", "p").limit(1).has("age", 3), __.has("kind", "q").range(0, 1)),
            g -> g.V().or(__.has("kind", "p").has("age"), __.has("kind", "q")),
            g -> g.V().or(__.has("kind", "p").range(1, 3), __.has("kind", "q"))
                .or(__.has("age", 1), __.has("name", "d")),
            g -> g.V().or(__.has("kind", "p").order().by("age").limit(1), __.has("kind", "q"))
                .or(__.has("age", 1), __.has("name", "d")),
            g -> g.V().or(__.has("nickname", "x"), __.has("kind", "p")).or(__.has("age", 1), __.has("name", "d")),
            //The step's has() is copied into each branch, so the second branch of the first or() repeats the within()
            g -> g.V().has("kind", P.within("p", "q")).or(__.has("name", "a"), __.has("kind", P.within("p", "q")))
                .or(__.has("age", 1), __.has("name", "b")),
            g -> g.V().or(__.has("kind", "p").range(1, 3), __.has("kind", "q").limit(0)),
            //The vertices of given ids, each checked against the branches
            g -> g.V().hasId(withUids(g, 1, 3, 6, 8)).or(__.has("kind", "p"), __.has("name", "d")),
            g -> g.V().hasId(withUids(g, 1, 3, 6, 8)).or(__.has("kind", "p").order().by("age"), __.has("name", "d")),
            g -> g.V().hasId(withUids(g, 1, 3, 6, 8)).or(__.has("kind", "p").range(1, 2), __.has("name", "j")),
            g -> g.V().hasId(withUids(g, 1, 3, 6, 8)).has("age").or(__.has("kind", "p"), __.has("name", "j")));
        final List<Function<GraphTraversalSource, GraphTraversal<?, ?>>> ordered = Arrays.asList(
            g -> g.V().or(__.has("kind", "p").limit(1), __.has("kind", "q")).order().by("name").limit(3),
            g -> g.V().or(__.has("kind", "p").limit(1), __.has("kind", "q")).count(),
            //An or() of which no branch lets an element through, ordered
            g -> g.V().or(__.has("kind", "p").range(1, 3), __.has("kind", "q").limit(0)).order().by("name"),
            g -> g.V().or(__.has("kind", "p").range(1, 3), __.has("kind", "q").limit(0)).order().by("name", desc)
                .limit(2));
        for (final List<Function<GraphTraversalSource, GraphTraversal<?, ?>>> traversals
                : Arrays.asList(unordered, ordered)) {
            for (final Function<GraphTraversalSource, GraphTraversal<?, ?>> traversal : traversals) {
                final List<Object> expected = new ArrayList<>(), actual = new ArrayList<>();
                traversal.apply(reference).forEachRemaining(
                    result -> expected.add(result instanceof Vertex ? ((Vertex) result).value("uid") : result));
                traversal.apply(tx.traversal()).forEachRemaining(
                    result -> actual.add(result instanceof Vertex ? ((Vertex) result).value("uid") : result));
                //Where the traversal doesn't order the vertices, they come in any order
                if (traversals == unordered) {
                    expected.sort(Comparator.comparing(Object::toString));
                    actual.sort(Comparator.comparing(Object::toString));
                }
                assertEquals(expected, actual, traversal.apply(reference).toString());
            }
        }
        //A branch of the API's or() which no element can hold, as one on a key which doesn't exist, leaves the others
        final JanusGraphQuery<?> query = tx.query();
        ((JanusGraphQuery) query).or(Arrays.asList(tx.query().has("nickname", "x"), tx.query().has("kind", "p")));
        final List<Object> ofKindP = new ArrayList<>();
        query.vertices().forEach(vertex -> ofKindP.add(vertex.value("uid")));
        ofKindP.sort(Comparator.comparing(Object::toString));
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), ofKindP);
        //The query's own conditions hold along with its or(), and an or() of no branches holds for no element
        final JanusGraphQuery<?> withOr = tx.query().has("kind", "p");
        ((JanusGraphQuery) withOr).or(Arrays.asList(tx.query().has("name", "d"), tx.query().has("age", 1)));
        final List<Object> ofKindPWithOr = new ArrayList<>();
        withOr.vertices().forEach(vertex -> ofKindPWithOr.add(vertex.value("uid")));
        assertEquals(Collections.singletonList(5), ofKindPWithOr);
        assertFalse(tx.query().or(Collections.emptyList()).vertices().iterator().hasNext());
        assertFalse(tx.query().has("kind", "p").or(Collections.emptyList()).vertices().iterator().hasNext());
        //The or() calls of a sub-query hold along with its own conditions
        final JanusGraphQuery<?> ofKindPOrQ = tx.query();
        ((JanusGraphQuery) ofKindPOrQ).or(Arrays.asList(tx.query().has("kind", "p"), tx.query().has("kind", "q")));
        final JanusGraphQuery<?> withNestedOr = tx.query();
        ((JanusGraphQuery) withNestedOr).or(Arrays.asList(ofKindPOrQ, tx.query().has("name", "a")));
        final List<Object> ofKindPOrQOrNamedA = new ArrayList<>();
        withNestedOr.vertices().forEach(vertex -> ofKindPOrQOrNamedA.add(vertex.value("uid")));
        ofKindPOrQOrNamedA.sort(Comparator.comparing(Object::toString));
        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7), ofKindPOrQOrNamedA);
        final JanusGraphQuery<?> ofKindPAndAge = tx.query().has("kind", "p");
        ((JanusGraphQuery) ofKindPAndAge).or(Arrays.asList(tx.query().has("age", 1), tx.query().has("age", 3)));
        final JanusGraphQuery<?> withOwnAndNestedOr = tx.query();
        ((JanusGraphQuery) withOwnAndNestedOr).or(Arrays.asList(ofKindPAndAge, tx.query().has("name", "h")));
        final List<Object> ofKindPAndAgeOrNamedH = new ArrayList<>();
        withOwnAndNestedOr.vertices().forEach(vertex -> ofKindPAndAgeOrNamedH.add(vertex.value("uid")));
        ofKindPAndAgeOrNamedH.sort(Comparator.comparing(Object::toString));
        assertEquals(Arrays.asList(2, 5, 7), ofKindPAndAgeOrNamedH);
        //A condition which the query and an or() call of a single sub-query both hold is held once, and so is one which
        //two sub-queries of an or() call hold
        final JanusGraphQuery<?> twice = tx.query().has("kind", "p");
        ((JanusGraphQuery) twice).or(Collections.singletonList(tx.query().has("kind", "p")));
        assertEquals("(kind = p)", conditionOf(twice));
        final JanusGraphQuery<?> twiceInOr = tx.query().has("age", 1);
        ((JanusGraphQuery) twiceInOr).or(Arrays.asList(tx.query().has("kind", "p"), tx.query().has("kind", "p"),
            tx.query().has("name", "a")));
        assertEquals("(age = 1 AND (kind = p OR name = a))", conditionOf(twiceInOr));
        //The sub-queries are taken as they are at the call: a query among the sub-queries of its own or() call holds
        //the conditions it had then, and a condition added to a sub-query afterwards doesn't count
        final JanusGraphQuery<?> itself = tx.query().has("kind", "p");
        ((JanusGraphQuery) itself).or(Arrays.asList(itself, tx.query().has("name", "d")));
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), sortedUids(itself));
        final JanusGraphQuery<?> later = tx.query().has("kind", "q");
        final JanusGraphQuery<?> withLater = tx.query();
        ((JanusGraphQuery) withLater).or(Arrays.asList(later, tx.query().has("name", "a")));
        later.has("name", "h");
        assertEquals(Arrays.asList(2, 6, 7), sortedUids(withLater));
        //The step of an or() which lets no element through prints it as an or() of no branches, and is profiled
        final GraphTraversal<Vertex, Vertex> none = tx.traversal().V()
            .or(__.has("kind", "p").range(1, 3), __.has("kind", "q").limit(0));
        assertFalse(none.hasNext());
        assertTrue(none.toString().contains("Or()"), none.toString());
        tx.traversal().V().or(__.has("kind", "p").range(1, 3), __.has("kind", "q").limit(0)).order().by("name")
            .profile().next();
    }

    //The condition of the query's vertices
    private static String conditionOf(JanusGraphQuery<?> query) {
        return ((org.janusgraph.graphdb.query.graph.GraphCentricQueryBuilder) query)
            .constructQuery(org.janusgraph.graphdb.internal.ElementCategory.VERTEX).getCondition().toString();
    }

    //The uids of the query's vertices, in the order of the uids
    private static List<Object> sortedUids(JanusGraphQuery<?> query) {
        final List<Object> uids = new ArrayList<>();
        query.vertices().forEach(vertex -> uids.add(vertex.value("uid")));
        uids.sort(Comparator.comparing(uid -> (Integer) uid));
        return uids;
    }

    //The ids of the vertices of the given uids
    private static P<Object> withUids(GraphTraversalSource g, Object... uids) {
        return P.within(g.V().has("uid", P.within(uids)).id().toList());
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

    //An element with several values of an order key, LIST or SET, is compared by the least of them in an ascending
    //order and by the greatest in a descending one, as Elasticsearch, OpenSearch and Solr sort such a key, its values
    //compared as any two values of the key are, strings by code point; an element without a value comes last
    @Test
    public void testOrderListComparesAKeyOfSeveralValuesByItsLeastOrGreatestValue() {
        final PropertyKey names = tx.makePropertyKey("names").dataType(String.class).cardinality(Cardinality.LIST)
            .make();
        final PropertyKey scores = tx.makePropertyKey("scores").dataType(Integer.class).cardinality(Cardinality.SET)
            .make();
        final OrderList namesAscending = orderBy(names, Order.ASC);
        final OrderList namesDescending = orderBy(names, Order.DESC);
        final OrderList scoresAscending = orderBy(scores, Order.ASC);
        final OrderList scoresDescending = orderBy(scores, Order.DESC);

        //U+FFFF comes before U+1F600 by code point, but after it by UTF-16 unit, and U+FFFF U+0001 comes between them
        final JanusGraphVertex several = tx.addVertex();
        several.property("names", "\uFFFF");
        several.property("names", "\uD83D\uDE00");
        several.property("scores", 7);
        several.property("scores", 1);
        final JanusGraphVertex one = tx.addVertex("names", "\uFFFF\u0001", "scores", 3);
        final JanusGraphVertex none = tx.addVertex();

        assertEquals("\uFFFF", namesAscending.value(several, 0));
        assertEquals("\uD83D\uDE00", namesDescending.value(several, 0));
        assertEquals(1, scoresAscending.value(several, 0));
        assertEquals(7, scoresDescending.value(several, 0));
        assertNull(namesAscending.value(none, 0));
        //several comes first in either direction: by U+FFFF before U+FFFF U+0001, and by U+1F600 after it; by 1 before
        //3, and by 7 after it
        for (final OrderList order
                : Arrays.asList(namesAscending, namesDescending, scoresAscending, scoresDescending)) {
            final List<JanusGraphVertex> vertices = new ArrayList<>(Arrays.asList(none, one, several));
            vertices.sort(order);
            assertEquals(Arrays.asList(several, one, none), vertices, order.toString());
            //and so by the values read once, as a query sorts the elements which its transaction added
            assertTrue(order.compareValues(order.values(several), order.values(one)) < 0, order.toString());
            assertTrue(order.compareValues(order.values(one), order.values(none)) < 0, order.toString());
        }

        //A value which the element gains counts, also next to a key of one value
        one.property("names", "\uD83D\uDE01");
        assertTrue(namesDescending.compare(one, several) < 0);
        final PropertyKey name = tx.makePropertyKey("name").dataType(String.class).make();
        final OrderList byScoresThenName = new OrderList();
        byScoresThenName.add(scores, Order.DESC);
        byScoresThenName.add(name, Order.ASC);
        byScoresThenName.makeImmutable();
        several.property("name", "b");
        final JanusGraphVertex tied = tx.addVertex("name", "a", "scores", 7);
        tied.property("scores", 2);
        assertTrue(byScoresThenName.compare(tied, several) < 0);
        assertTrue(byScoresThenName.compare(several, one) < 0);
    }

    private static OrderList orderBy(PropertyKey key, Order order) {
        final OrderList orders = new OrderList();
        orders.add(key, order);
        orders.makeImmutable();
        return orders;
    }

    @Test
    public void testOrderListComparesStringsByCodePoint() {
        for (int i = 0; i < BY_CODE_POINT.length; i++) {
            for (int j = 0; j < BY_CODE_POINT.length; j++) {
                assertEquals(Integer.signum(i - j), Integer.signum(
                    OrderList.compareOrderValues(BY_CODE_POINT[i], BY_CODE_POINT[j], Order.ASC)), i + " vs " + j);
                assertEquals(Integer.signum(j - i), Integer.signum(
                    OrderList.compareOrderValues(BY_CODE_POINT[i], BY_CODE_POINT[j], Order.DESC)), i + " vs " + j);
            }
        }
        //Strings of such characters in any combination compare as their code points do. A string with a surrogate
        //which isn't part of a pair isn't valid UTF-16, and has no code point order to agree on
        final int[] codePoints = {'a', 0xD7FF, 0xE000, 0xFFFF, 0x10000, 0x1F600, 0x10FC00, 0x10FFFF};
        final Random random = new Random(7);
        for (int n = 0; n < 20000; n++) {
            final String s1 = randomString(random, codePoints), s2 = randomString(random, codePoints);
            assertEquals(Integer.signum(Arrays.compare(s1.codePoints().toArray(), s2.codePoints().toArray())),
                Integer.signum(OrderList.compareOrderValues(s1, s2, Order.ASC)), s1 + " vs " + s2);
        }
        //A missing value comes last in either direction, and other values compare by their natural order
        for (final Order order : Order.values()) {
            assertTrue(OrderList.compareOrderValues(null, "a", order) > 0);
            assertTrue(OrderList.compareOrderValues("a", null, order) < 0);
            assertEquals(0, OrderList.compareOrderValues(null, null, order));
        }
        assertTrue(OrderList.compareOrderValues(1L, 2L, Order.ASC) < 0);
        assertTrue(OrderList.compareOrderValues(1L, 2L, Order.DESC) > 0);
        assertTrue(OrderList.compareOrderValues('\uFFFF', '\uD83D', Order.ASC) > 0);

        final PropertyKey name = tx.makePropertyKey("name").dataType(String.class).make();
        final OrderList byName = new OrderList();
        byName.add(name, Order.ASC);
        byName.makeImmutable();
        final List<JanusGraphVertex> vertices = new ArrayList<>();
        for (final String s : BY_CODE_POINT) {
            vertices.add(tx.addVertex("name", s));
        }
        Collections.shuffle(vertices, random);
        vertices.sort(byName);
        final List<String> names = new ArrayList<>();
        vertices.forEach(v -> names.add(v.value("name")));
        assertEquals(Arrays.asList(BY_CODE_POINT), names);
    }

    private static String randomString(Random random, int[] codePoints) {
        final StringBuilder s = new StringBuilder();
        for (int i = random.nextInt(4); i > 0; i--) {
            s.appendCodePoint(codePoints[random.nextInt(codePoints.length)]);
        }
        return s.toString();
    }

    @Test
    public void testGraphQueryConditionsCompareStringsByCodePoint() {
        //The conditions of a graph query compare two strings by code point, as the mixed indexes and the order which
        //JanusGraph gives the query do, and those of a vertex-centric query by UTF-16 unit, as the storage backend
        //holds sort keys
        final PropertyKey name = tx.makePropertyKey("name").dataType(String.class).make();
        final List<JanusGraphVertex> vertices = new ArrayList<>();
        for (final String s : BY_CODE_POINT) {
            vertices.add(tx.addVertex("name", s));
        }
        for (int i = 0; i < BY_CODE_POINT.length; i++) {
            final JanusGraphVertex v = vertices.get(i);
            for (int j = 0; j < BY_CODE_POINT.length; j++) {
                final String s = BY_CODE_POINT[j], pair = i + " vs " + j;
                assertEquals(i < j, holds(v, name, Cmp.LESS_THAN, s, true), pair);
                assertEquals(i <= j, holds(v, name, Cmp.LESS_THAN_EQUAL, s, true), pair);
                assertEquals(i > j, holds(v, name, Cmp.GREATER_THAN, s, true), pair);
                assertEquals(i >= j, holds(v, name, Cmp.GREATER_THAN_EQUAL, s, true), pair);
                assertEquals(i == j, holds(v, name, Cmp.EQUAL, s, true), pair);
                assertEquals(i != j, holds(v, name, Cmp.NOT_EQUAL, s, true), pair);
                final int byUnit = BY_CODE_POINT[i].compareTo(s);
                assertEquals(byUnit < 0, holds(v, name, Cmp.LESS_THAN, s, false), pair);
                assertEquals(byUnit >= 0, holds(v, name, Cmp.GREATER_THAN_EQUAL, s, false), pair);
            }
        }
        final JanusGraphVertex emoji = vertices.get(Arrays.asList(BY_CODE_POINT).indexOf("\uD83D\uDE00"));
        assertTrue(holds(emoji, name, Cmp.GREATER_THAN, "\uE000", true));
        assertFalse(holds(emoji, name, Cmp.GREATER_THAN, "\uE000", false));
        //Other predicates, and the ranges of other values, test as they do on their own
        assertTrue(holds(emoji, name, Text.PREFIX, "\uD83D", true));
        assertTrue(holds(emoji, name, Contain.IN, Arrays.asList("a", "\uD83D\uDE00"), true));
        tx.makePropertyKey("age").dataType(Integer.class).make();
        final JanusGraphVertex aged = tx.addVertex("age", 2);
        assertTrue(holds(aged, tx.getPropertyKey("age"), Cmp.GREATER_THAN, 1, true));
        assertFalse(holds(aged, tx.getPropertyKey("age"), Cmp.GREATER_THAN, 2, true));
    }

    private static boolean holds(JanusGraphVertex vertex, PropertyKey key, JanusGraphPredicate predicate, Object value,
                                 boolean stringsByCodePoint) {
        return new PredicateCondition<>(key, predicate, value, stringsByCodePoint).evaluate(vertex);
    }

    /**
     * Only a range of Cmp on strings depends on the order of strings, so any other condition of a graph query equals
     * the same condition of a vertex-centric query, as a query which leaves out a repeated condition expects.
     */
    @Test
    public void testConditionsWhichDontCompareStringsEqualTheirTwins() {
        final PropertyKey name = tx.makePropertyKey("name").dataType(String.class).make();
        final PropertyKey age = tx.makePropertyKey("age").dataType(Integer.class).make();
        assertTwins(true, name, Cmp.NOT_EQUAL, null);
        assertTwins(true, name, Cmp.EQUAL, "a");
        assertTwins(true, name, Text.PREFIX, "a");
        assertTwins(true, name, Contain.IN, Arrays.asList("a", "b"));
        assertTwins(true, age, Cmp.GREATER_THAN, 1);
        assertTwins(false, name, Cmp.GREATER_THAN, "a");
        assertTwins(false, name, Cmp.LESS_THAN_EQUAL, "a");
    }

    private static void assertTwins(boolean equal, PropertyKey key, JanusGraphPredicate predicate, Object value) {
        final PredicateCondition<PropertyKey, JanusGraphVertex> byCodePoint =
            new PredicateCondition<>(key, predicate, value, true);
        final PredicateCondition<PropertyKey, JanusGraphVertex> byUnit =
            new PredicateCondition<>(key, predicate, value, false);
        assertEquals(equal, byCodePoint.equals(byUnit), predicate + " " + value);
        if (equal) {
            assertEquals(byCodePoint.hashCode(), byUnit.hashCode(), predicate + " " + value);
        }
    }

    /**
     * A graph query which no mixed index answers compares strings by code point in its conditions, as in its order, so
     * that keyset paging with gt() from the last string of each page comes through all of them, the ones which the
     * transaction added included: by UTF-16 unit, U+1F600 comes before U+E000 and U+FFFF.
     */
    @Test
    public void testGraphQueryPagesThroughStringsByCodePoint() {
        final JanusGraphManagement mgmt = graph.openManagement();
        final PropertyKey kind = mgmt.makePropertyKey("kind").dataType(String.class).make();
        mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.buildIndex("byKind", Vertex.class).addKey(kind).buildCompositeIndex();
        mgmt.commit();
        for (final String name : Arrays.asList("a", "\uFFFF")) {
            tx.addVertex("kind", "k", "name", name);
        }
        tx.commit();
        tx = graph.newTransaction();
        for (final String name : Arrays.asList("\uE000", "\uD83D\uDE00")) {
            tx.addVertex("kind", "k", "name", name);
        }
        final List<String> byCodePoint = Arrays.asList("a", "\uE000", "\uFFFF", "\uD83D\uDE00");
        //Through the composite index on the kind, and by a scan
        assertEquals(byCodePoint, pageThroughNames("k"));
        assertEquals(byCodePoint, pageThroughNames(null));
        //Ranges of two predicates, which a graph query checks one by one
        final GraphTraversalSource g = tx.traversal();
        assertEquals(byCodePoint.subList(0, 2), g.V().has("kind", "k").has("name", P.lt("\uFFFF"))
            .order().by("name", asc).values("name").toList());
        assertEquals(byCodePoint.subList(1, 4), g.V().has("kind", "k").has("name", P.between("\uE000", "\uD83D\uDE01"))
            .order().by("name", asc).values("name").toList());
        assertEquals(byCodePoint.subList(2, 4), g.V().has("kind", "k").has("name", P.inside("\uE000", "\uD83D\uDE01"))
            .order().by("name", asc).values("name").toList());
        assertEquals(byCodePoint.subList(3, 4), g.V().has("kind", "k").has("name", P.outside("a", "\uFFFF"))
            .order().by("name", asc).values("name").toList());
    }

    //The names which keyset paging finds, two at a time, each page from the last name of the page before: of the
    //vertices of the kind, through its index, or of all vertices with a name, by a scan
    private List<String> pageThroughNames(String kind) {
        final GraphTraversalSource g = tx.traversal();
        final List<String> names = new ArrayList<>();
        List<String> page = withKind(g, kind).order().by("name", asc).limit(2).<String>values("name").toList();
        for (int i = 0; !page.isEmpty(); i++) {
            assertTrue(i < 10, "pages without end: " + names);
            names.addAll(page);
            page = withKind(g, kind).has("name", P.gt(page.get(page.size() - 1))).order().by("name", asc).limit(2)
                .<String>values("name").toList();
        }
        return names;
    }

    private static GraphTraversal<Vertex, Vertex> withKind(GraphTraversalSource g, String kind) {
        return kind == null ? g.V().has("name") : g.V().has("kind", kind);
    }

    @Test
    public void testInMemoryOrderComparesStringsByCodePoint() {
        tx.makePropertyKey("name").dataType(String.class).make();
        final List<String> byCodePoint = Arrays.asList(BY_CODE_POINT);
        final List<String> shuffled = new ArrayList<>(byCodePoint);
        Collections.shuffle(shuffled, new Random(11));
        for (final String s : shuffled.subList(0, 6)) {
            tx.addVertex("name", s);
        }
        tx.commit();
        //Sorted in memory, by a query which no index answers, with the vertices the transaction adds among the others
        tx = graph.newTransaction();
        for (final String s : shuffled.subList(6, shuffled.size())) {
            tx.addVertex("name", s);
        }
        final GraphTraversalSource g = tx.traversal();
        assertEquals(byCodePoint, g.V().has("name").order().by("name", asc).values("name").toList());
        assertEquals(byCodePoint.subList(0, 4),
            g.V().has("name").order().by("name", asc).limit(4).values("name").toList());
        final List<String> reversed = new ArrayList<>(byCodePoint);
        Collections.reverse(reversed);
        assertEquals(reversed.subList(0, 4),
            g.V().has("name").order().by("name", desc).limit(4).values("name").toList());
    }

    @Test
    public void testOrBranchesAreMergedInTheOrderOfTheirQueries() {
        tx.makePropertyKey("kind").dataType(String.class).make();
        tx.makePropertyKey("name").dataType(String.class).make();
        tx.makePropertyKey("w").dataType(Integer.class).make();
        for (final String name : Arrays.asList("a", "b", "\uE000", "\uD83D\uDE00")) {
            tx.addVertex("kind", "p", "name", name);
        }
        for (final String name : Arrays.asList("z", "\uFFFF", "\uD83D\uDE01")) {
            tx.addVertex("kind", "q", "name", name);
        }
        tx.addVertex("kind", "q");
        final JanusGraphVertex from = tx.addVertex(), to = tx.addVertex();
        for (final int w : new int[]{1, 1, 2, 2}) from.addEdge("e", to, "w", w);
        tx.commit();
        tx = graph.newTransaction();
        final GraphTraversalSource g = tx.traversal();
        //No index answers the or(), and the branches have ranges of their own, so JanusGraph answers each of them by a
        //query of its own, whose results it merges in their order: strings by code point. An element without the key
        //is left out, as TinkerPop leaves it out
        final List<String> byName = Arrays.asList("a", "b", "z", "\uE000", "\uFFFF", "\uD83D\uDE00", "\uD83D\uDE01");
        assertEquals(byName, names(g.V().or(__.has("kind", "p").range(0, 10), __.has("kind", "q").range(0, 9))
            .order().by("name", asc).toList()));
        final List<String> byNameDescending = new ArrayList<>(byName);
        Collections.reverse(byNameDescending);
        assertEquals(byNameDescending, names(g.V().or(__.has("kind", "p").range(0, 10), __.has("kind", "q").range(0, 9))
            .order().by("name", desc).toList()));
        //A first branch which can't return anything, of a key which doesn't exist or of a limit of 0, is a query
        //without an order, while the others are in the order of the step
        assertEquals(byName, names(g.V().or(__.has("nickname", "x"), __.has("kind", "p"), __.has("kind", "q"))
            .order().by("name", asc).toList()));
        assertEquals(byName, names(g.V().or(__.has("kind", "x").limit(0), __.has("kind", "p"), __.has("kind", "q"))
            .order().by("name", asc).toList()));
        //An implicit key, whose value each query computes, here the id
        final List<Edge> byId = g.E().order().by("~nid", asc).toList();
        assertEquals(4, byId.size());
        assertEquals(byId,
            g.E().or(__.has("w", 1).range(0, 5), __.has("w", 2).range(0, 4)).order().by("~nid", asc).toList());
    }

    private static List<String> names(List<Vertex> vertices) {
        final List<String> names = new ArrayList<>();
        for (final Vertex vertex : vertices) {
            names.add(vertex.<String>property("name").orElse(null));
        }
        return names;
    }

    /**
     * TinkerPop leaves out an element for which a by() modulator of an order() has no value. JanusGraph sorts these
     * queries itself, as no index answers them, and agrees with a TinkerGraph which holds the same vertices: where it
     * folds the order into its query, where several order() steps follow each other, and where the order is TinkerPop's
     */
    @Test
    @SuppressWarnings({"deprecation", "unchecked", "rawtypes"})
    public void testOrderLeavesOutTheElementsWithoutAKeyItOrdersByAsTinkerGraphDoes() {
        tx.makePropertyKey("uid").dataType(Integer.class).make();
        tx.makePropertyKey("kind").dataType(String.class).make();
        tx.makePropertyKey("name").dataType(String.class).make();
        tx.makePropertyKey("age").dataType(Integer.class).make();
        final GraphTraversalSource reference = TinkerGraph.open().traversal();
        //uid, name and age, some without a name or an age
        final Object[][] vertices = {{1, "c", 5}, {2, "a", 3}, {3, null, 4}, {4, "e", null}, {5, null, 1}, {6, "b", 2}};
        for (final GraphTraversalSource g : Arrays.asList(tx.traversal(), reference)) {
            for (final Object[] vertex : vertices) {
                GraphTraversal<Vertex, Vertex> added = g.addV().property("uid", vertex[0]).property("kind", "p");
                if (vertex[1] != null) added = added.property("name", vertex[1]);
                if (vertex[2] != null) added = added.property("age", vertex[2]);
                added.iterate();
            }
        }
        tx.commit();
        tx = graph.newTransaction();
        final List<Function<GraphTraversalSource, GraphTraversal<?, Vertex>>> traversals = Arrays.asList(
            g -> g.V().has("kind", "p").order().by("name"),
            g -> g.V().has("kind", "p").order().by("name", desc).limit(2),
            g -> g.V().has("kind", "p").order().by("age", desc).by("name"),
            //The second by() of a key, which only orders the ties of the first, is left out
            g -> g.V().has("kind", "p").order().by("name").by("name", desc),
            //Both orders are folded in, by the name and then by the age
            g -> g.V().has("kind", "p").order().by("age").order().by("name"),
            //An order by a traversal is TinkerPop's, and so are the orders next to it
            g -> g.V().has("kind", "p").order().by(__.values("age").is(P.gt(2))).order().by("name", desc),
            g -> g.V().has("kind", "p").order().by("name").order().by(__.values("age").is(P.gt(2))));
        for (final Function<GraphTraversalSource, GraphTraversal<?, Vertex>> traversal : traversals) {
            assertEquals(uids(traversal.apply(reference).toList()), uids(traversal.apply(tx.traversal()).toList()),
                traversal.apply(reference).toString());
        }
        //A random order, which TinkerPop gives, of the vertices which have a name
        assertEquals(Arrays.asList(1, 2, 4, 6), sorted(uids(tx.traversal().V().has("kind", "p")
            .order().by("name", shuffle).toList())));
        assertEquals(2, tx.traversal().V().has("kind", "p").order().by("name", shuffle).limit(2).toList()
            .stream().filter(v -> v.property("name").isPresent()).count());
        //ProductiveByStrategy makes a by() modulator give null where it has no value, which TinkerPop orders first
        final Function<GraphTraversalSource, GraphTraversal<?, Vertex>> productive = g -> g
            .withStrategies(ProductiveByStrategy.instance()).V().has("kind", "p").order().by("age").by("uid");
        assertEquals(uids(productive.apply(reference).toList()), uids(productive.apply(tx.traversal()).toList()));
        assertEquals(Arrays.asList(4, 5, 6, 2, 3, 1), uids(productive.apply(tx.traversal()).toList()));
        //An order by an ElementValueComparator, on which TinkerPop fails where a vertex has no value, leaves out such
        //a vertex too
        final ElementValueComparator byName = new ElementValueComparator("name", asc);
        assertEquals(Arrays.asList(2, 6, 1, 4), uids(tx.traversal().V().has("kind", "p").order().by(byName).toList()));
        //An order in an or() branch, a random one too, lets a vertex through only where it has a name, as TinkerPop
        //runs the branch for each vertex on its own: the branch is folded in with a has(name)
        final Function<GraphTraversalSource, GraphTraversal<?, Vertex>> shuffledBranch = g -> g.V()
            .or(__.has("kind", "p").order().by("name", shuffle), __.has("uid", 3));
        assertEquals(sorted(uids(shuffledBranch.apply(reference).toList())),
            sorted(uids(shuffledBranch.apply(tx.traversal()).toList())));
        assertEquals(Arrays.asList(1, 2, 3, 4, 6), sorted(uids(shuffledBranch.apply(tx.traversal()).toList())));
        //while an order which leaves out no element, of the element itself, its label, or a key which
        //ProductiveByStrategy makes yield null where it has no value, needs no value
        final List<Function<GraphTraversalSource, GraphTraversal<?, Vertex>>> branchOrders = Arrays.asList(
            g -> g.V().or(__.has("kind", "p").order(), __.has("uid", 3)),
            g -> g.V().or(__.has("kind", "p").order().by(T.label), __.has("uid", 3)),
            g -> g.withStrategies(ProductiveByStrategy.instance()).V()
                .or(__.has("kind", "p").order().by("age"), __.has("uid", 3)));
        for (final Function<GraphTraversalSource, GraphTraversal<?, Vertex>> branchOrder : branchOrders) {
            assertEquals(sorted(uids(branchOrder.apply(reference).toList())),
                sorted(uids(branchOrder.apply(tx.traversal()).toList())), branchOrder.apply(reference).toString());
        }

        //An edge holds one value of a key at most, also of one declared to hold several, so an order by such a key
        //leaves out the edges without it as well
        tx.makePropertyKey("lw").dataType(Integer.class).cardinality(Cardinality.LIST).make();
        final Integer[] weights = {3, null, 1};
        for (final GraphTraversalSource g : Arrays.asList(tx.traversal(), reference)) {
            for (int i = 0; i < weights.length; i++) {
                GraphTraversal<Vertex, Edge> added = g.V().has("uid", 1).addE("link").to(__.V().has("uid", i + 2))
                    .property("uid", 11 + i);
                if (weights[i] != null) added = added.property("lw", weights[i]);
                added.iterate();
            }
        }
        tx.commit();
        tx = graph.newTransaction();
        for (final org.apache.tinkerpop.gremlin.process.traversal.Order order : Arrays.asList(asc, desc)) {
            final Function<GraphTraversalSource, List<Object>> edges = g -> g.E().order().by("lw", order).values("uid")
                .toList();
            assertEquals(edges.apply(reference), edges.apply(tx.traversal()), order.toString());
        }
    }

    private static List<Integer> uids(List<Vertex> vertices) {
        final List<Integer> uids = new ArrayList<>();
        vertices.forEach(vertex -> uids.add(vertex.value("uid")));
        return uids;
    }

    private static List<Integer> sorted(List<Integer> values) {
        final List<Integer> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        return sorted;
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
