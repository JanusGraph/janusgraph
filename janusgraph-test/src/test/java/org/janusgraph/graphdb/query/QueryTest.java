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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.process.traversal.Order.asc;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.desc;
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
        //query of its own, whose results it merges in their order: strings by code point, and an element without the
        //key last in either direction
        final List<String> byName = Arrays.asList("a", "b", "z", "\uE000", "\uFFFF", "\uD83D\uDE00", "\uD83D\uDE01",
            null);
        assertEquals(byName, names(g.V().or(__.has("kind", "p").range(0, 10), __.has("kind", "q").range(0, 9))
            .order().by("name", asc).toList()));
        final List<String> byNameDescending = new ArrayList<>(byName.subList(0, 7));
        Collections.reverse(byNameDescending);
        byNameDescending.add(null);
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
