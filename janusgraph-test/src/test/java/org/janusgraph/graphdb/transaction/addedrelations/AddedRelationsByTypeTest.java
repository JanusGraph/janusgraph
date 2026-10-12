// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.graphdb.transaction.addedrelations;

import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.StorageSetup;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.StandardTransactionBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A vertex which gains relations in a transaction keeps its edges by label once it has more than
 * {@link SimpleAddedRelations#MAX_EDGES_WITHOUT_LABEL_INDEX}, and a query of some types reads only the relations of
 * those: it answers as the committed graph does, below and above that bound, in a transaction of one thread
 * ({@link SimpleAddedRelations}) and in one which threads share ({@link ConcurrentAddedRelations}).
 */
public class AddedRelationsByTypeTest {

    private static final int MANY = SimpleAddedRelations.MAX_EDGES_WITHOUT_LABEL_INDEX + 8;

    /** The two kinds of transaction, which keep the relations of a vertex in different containers. */
    enum Transaction {
        THREAD_BOUND, SHARED;

        JanusGraphTransaction open(StandardJanusGraph graph) {
            return this == THREAD_BOUND ? graph.newThreadBoundTransaction() : graph.newTransaction();
        }

        StandardTransactionBuilder configure(StandardTransactionBuilder builder) {
            return this == THREAD_BOUND ? builder.threadBound() : builder;
        }
    }

    private StandardJanusGraph graph;

    @AfterEach
    public void tearDown() {
        if (graph != null) {
            graph.close();
        }
    }

    private void open() {
        open(StorageSetup.getInMemoryConfiguration());
    }

    private void open(ModifiableConfiguration config) {
        graph = (StandardJanusGraph) JanusGraphFactory.open(config);
    }

    private void schema() {
        final JanusGraphManagement management = graph.openManagement();
        management.makeEdgeLabel("member").make();
        management.makeEdgeLabel("owner").make();
        management.makeEdgeLabel("unused").make();
        management.makePropertyKey("name").dataType(String.class).make();
        management.makePropertyKey("since").dataType(Integer.class).make();
        management.commit();
    }

    //Each query, by the names of the vertices at the other ends of the edges or the values of the properties, sorted
    private static Map<String, List<String>> answers(GraphTraversalSource g, Object hubId) {
        final Map<String, List<String>> answers = new LinkedHashMap<>();
        final Function<Edge, String> otherEnd = e -> e.label() + ":" + e.outVertex().value("name") + "->"
            + e.inVertex().value("name");
        answers.put("outE(member)", sorted(g.V(hubId).outE("member").toList(), otherEnd));
        answers.put("outE(owner)", sorted(g.V(hubId).outE("owner").toList(), otherEnd));
        answers.put("inE(owner)", sorted(g.V(hubId).inE("owner").toList(), otherEnd));
        answers.put("bothE(owner)", sorted(g.V(hubId).bothE("owner").toList(), otherEnd));
        answers.put("bothE(member,owner)", sorted(g.V(hubId).bothE("member", "owner").toList(), otherEnd));
        answers.put("bothE(unused)", sorted(g.V(hubId).bothE("unused").toList(), otherEnd));
        answers.put("bothE()", sorted(g.V(hubId).bothE().toList(), otherEnd));
        answers.put("outE(member).has(since>30)",
            sorted(g.V(hubId).outE("member").has("since", P.gt(30)).toList(), otherEnd));
        answers.put("out(member).has(name,m7)",
            sorted(g.V(hubId).out("member").has("name", "m7").values("name").toList(), Object::toString));
        answers.put("out(member).where(in(owner))",
            sorted(g.V(hubId).out("member").where(__.in("owner")).values("name").toList(), Object::toString));
        answers.put("values(name)", sorted(g.V(hubId).values("name").toList(), Object::toString));
        answers.put("properties()", sorted(g.V(hubId).properties().toList(), p -> p.key() + "=" + p.value()));
        return answers;
    }

    private static <T> List<String> sorted(List<T> results, Function<? super T, String> describe) {
        final List<String> described = new ArrayList<>(results.size());
        for (T result : results) {
            described.add(describe.apply(result));
        }
        Collections.sort(described);
        return described;
    }

    @ParameterizedTest
    @EnumSource(Transaction.class)
    public void shouldAnswerTheQueriesOfAVertexWithManyNewEdgesAsTheCommittedGraphDoes(Transaction kind) {
        open();
        schema();
        final JanusGraphTransaction tx = kind.open(graph);
        final JanusGraphVertex hub = tx.addVertex("name", "hub");
        final List<JanusGraphEdge> members = new ArrayList<>();
        for (int i = 0; i < MANY; i++) {
            members.add(hub.addEdge("member", tx.addVertex("name", "m" + i), "since", i));
        }
        for (int i = 0; i < 3; i++) {
            tx.addVertex("name", "o" + i).addEdge("owner", hub);
        }
        hub.addEdge("owner", hub);
        //One member owns another one, which the where() asks about
        members.get(1).inVertex().addEdge("owner", members.get(2).inVertex());
        //New edges removed again leave the vertex
        members.get(0).remove();
        hub.addEdge("member", tx.addVertex("name", "gone")).remove();
        final Object hubId = hub.id();

        final Map<String, List<String>> inTransaction = answers(tx.traversal(), hubId);
        assertEquals(MANY - 1, inTransaction.get("outE(member)").size());
        assertEquals(Collections.singletonList("owner:hub->hub"), inTransaction.get("outE(owner)"));
        assertTrue(inTransaction.get("bothE(unused)").isEmpty());
        tx.commit();

        final JanusGraphTransaction committed = graph.newTransaction();
        try {
            assertEquals(answers(committed.traversal(), hubId), inTransaction);
        } finally {
            committed.rollback();
        }
    }

    //A loaded vertex which gains the edges in the transaction answers the same as well
    @ParameterizedTest
    @EnumSource(Transaction.class)
    public void shouldAnswerTheQueriesOfALoadedVertexWithManyNewEdgesAsTheCommittedGraphDoes(Transaction kind) {
        open();
        schema();
        JanusGraphTransaction tx = graph.newTransaction();
        final JanusGraphVertex created = tx.addVertex("name", "hub");
        created.addEdge("member", tx.addVertex("name", "stored"), "since", 100);
        tx.addVertex("name", "stored owner").addEdge("owner", created);
        tx.commit();
        final Object hubId = created.id();

        tx = kind.open(graph);
        final JanusGraphVertex hub = tx.getVertex(hubId);
        for (int i = 0; i < MANY; i++) {
            hub.addEdge("member", tx.addVertex("name", "m" + i), "since", i);
        }
        tx.addVertex("name", "o").addEdge("owner", hub);
        hub.addEdge("owner", hub);
        Iterables.getOnlyElement(hub.query().labels("member").has("since", 100).edges()).remove();
        final Map<String, List<String>> inTransaction = answers(tx.traversal(), hubId);
        assertEquals(MANY, inTransaction.get("outE(member)").size());
        tx.commit();

        final JanusGraphTransaction committed = graph.newTransaction();
        try {
            assertEquals(answers(committed.traversal(), hubId), inTransaction);
        } finally {
            committed.rollback();
        }
    }

    @ParameterizedTest
    @CsvSource({"3,THREAD_BOUND", "40,THREAD_BOUND", "3,SHARED", "40,SHARED"})
    public void shouldReturnEachNewEdgeOnceToAQueryOfSeveralLabels(int members, Transaction kind) {
        open();
        schema();
        final JanusGraphTransaction tx = kind.open(graph);
        try {
            final JanusGraphVertex hub = tx.addVertex();
            for (int i = 0; i < members; i++) {
                hub.addEdge("member", tx.addVertex());
            }
            for (int i = 0; i < 3; i++) {
                hub.addEdge("owner", tx.addVertex());
            }
            assertEquals(members + 3, Iterables.size(hub.query().labels("member", "owner").edges()));
            assertEquals(members + 3L, (long) tx.traversal().V(hub).outE("member", "owner").count().next());
            hub.addEdge("owner", hub);
            assertEquals(members + 5, Iterables.size(hub.query().labels("member", "owner").edges()));
            assertEquals(members + 4,
                Iterables.size(hub.query().labels("member", "owner").direction(Direction.OUT).edges()));
            assertEquals(1, Iterables.size(hub.query().labels("owner").direction(Direction.IN).edges()));
            assertEquals(members + 4L, (long) tx.traversal().V(hub).outE().hasLabel("member", "owner").count().next());
            assertEquals(members + 5L, (long) tx.traversal().V(hub).bothE().count().next());
        } finally {
            tx.rollback();
        }
    }

    //A new edge of a label of constrained multiplicity replaces the stored one, which queries no longer return
    @ParameterizedTest
    @CsvSource({"3,THREAD_BOUND", "40,THREAD_BOUND", "3,SHARED", "40,SHARED"})
    public void shouldHideTheStoredEdgesThatNewEdgesOfConstrainedLabelsReplace(int members, Transaction kind) {
        open();
        final JanusGraphManagement management = graph.openManagement();
        management.makeEdgeLabel("member").make();
        management.makeEdgeLabel("boss").multiplicity(Multiplicity.MANY2ONE).make();
        management.makeEdgeLabel("friend").multiplicity(Multiplicity.SIMPLE).make();
        management.makeEdgeLabel("parent").multiplicity(Multiplicity.ONE2MANY).make();
        management.makePropertyKey("name").dataType(String.class).make();
        management.commit();
        final JanusGraphTransaction stored = graph.newTransaction();
        final JanusGraphVertex storedVertex = stored.addVertex("name", "v");
        final JanusGraphVertex storedBoss = stored.addVertex("name", "a");
        storedVertex.addEdge("boss", storedBoss);
        storedVertex.addEdge("friend", storedBoss);
        storedVertex.addEdge("parent", stored.addVertex("name", "c0"));
        stored.commit();

        final StandardTransactionBuilder builder = graph.buildTransaction();
        builder.consistencyChecks(false);
        final JanusGraphTransaction tx = kind.configure(builder).start();
        try {
            final JanusGraphVertex vertex = tx.getVertex(storedVertex.id());
            for (int i = 0; i < members; i++) {
                vertex.addEdge("member", tx.addVertex());
            }
            vertex.addEdge("boss", tx.addVertex("name", "b"));
            vertex.addEdge("friend", tx.getVertex(storedBoss.id()));
            for (int i = 1; i <= 3; i++) {
                vertex.addEdge("parent", tx.addVertex("name", "c" + i));
            }
            final List<String> bosses = new ArrayList<>();
            vertex.query().labels("boss").direction(Direction.OUT).vertices().forEach(boss -> bosses.add(boss.value("name")));
            assertEquals(Collections.singletonList("b"), bosses);
            assertEquals(1, Iterables.size(vertex.query().labels("friend").direction(Direction.OUT).edges()));
            assertEquals(4, Iterables.size(vertex.query().labels("parent").direction(Direction.OUT).edges()));
            assertEquals(1L, (long) tx.traversal().V(vertex).out("boss").count().next());
        } finally {
            tx.rollback();
        }
    }

    //The new properties are grouped by the name of their key, which a rename of the key changes under the transaction
    @ParameterizedTest
    @CsvSource({"false,THREAD_BOUND", "true,THREAD_BOUND", "false,SHARED", "true,SHARED"})
    public void shouldFindTheNewPropertiesOfAKeyRenamedWhileTheTransactionIsOpen(boolean loaded, Transaction kind) {
        open();
        JanusGraphManagement management = graph.openManagement();
        management.makePropertyKey("a").dataType(Integer.class).make();
        management.commit();
        Object storedId = null;
        if (loaded) {
            final JanusGraphTransaction stored = graph.newTransaction();
            storedId = stored.addVertex().id();
            stored.commit();
        }
        final JanusGraphTransaction tx = kind.open(graph);
        try {
            final JanusGraphVertex vertex = loaded ? tx.getVertex(storedId) : tx.addVertex();
            vertex.property("a", 1);
            assertEquals(Collections.singletonList(1), tx.traversal().V(vertex.id()).values("a").toList());
            management = graph.openManagement();
            management.changeName(management.getPropertyKey("a"), "b");
            management.commit();
            final List<Object> values = new ArrayList<>();
            vertex.query().keys("b").properties().forEach(property -> values.add(property.value()));
            assertEquals(Collections.singletonList(1), values);
            assertEquals(Collections.singletonList(1), tx.traversal().V(vertex.id()).values("b").toList());
        } finally {
            tx.rollback();
        }
    }

    @Test
    public void shouldAnswerTheQueriesOfAPartitionedVertexWithManyNewEdgesAsTheCommittedGraphDoes() {
        open(StorageSetup.getInMemoryConfiguration().set(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS, 8));
        final JanusGraphManagement management = graph.openManagement();
        management.makeVertexLabel("group").partition().make();
        management.makeEdgeLabel("member").make();
        management.makeEdgeLabel("owner").make();
        management.makePropertyKey("name").dataType(String.class).make();
        management.commit();
        final JanusGraphTransaction tx = graph.newTransaction();
        final JanusGraphVertex hub = tx.addVertex("group");
        hub.property("name", "hub");
        for (int i = 0; i < MANY; i++) {
            tx.addVertex("name", "m" + i).addEdge("member", hub);
        }
        for (int i = 0; i < 5; i++) {
            hub.addEdge("owner", tx.addVertex("name", "o" + i));
        }
        final Object hubId = hub.id();
        final Map<String, Object> inTransaction = partitionedAnswers(tx, hubId);
        tx.commit();
        final JanusGraphTransaction committed = graph.newTransaction();
        try {
            assertEquals(partitionedAnswers(committed, hubId), inTransaction);
        } finally {
            committed.rollback();
        }
    }

    private static Map<String, Object> partitionedAnswers(JanusGraphTransaction tx, Object hubId) {
        final Map<String, Object> answers = new LinkedHashMap<>();
        final GraphTraversalSource g = tx.traversal();
        answers.put("inE(member)", g.V(hubId).inE("member").count().next());
        answers.put("outE(owner)", g.V(hubId).outE("owner").count().next());
        answers.put("bothE(member,owner)", g.V(hubId).bothE("member", "owner").count().next());
        answers.put("bothE()", g.V(hubId).bothE().count().next());
        answers.put("in(member).name", sorted(g.V(hubId).in("member").values("name").toList(), Object::toString));
        return answers;
    }

    @ParameterizedTest
    @EnumSource(Transaction.class)
    public void shouldAnswerAMultiQueryOfVerticesWithNewEdges(Transaction kind) {
        open();
        schema();
        final JanusGraphTransaction tx = kind.open(graph);
        try {
            final JanusGraphVertex many = tx.addVertex();
            final JanusGraphVertex few = tx.addVertex();
            for (int i = 0; i < MANY; i++) {
                many.addEdge("member", tx.addVertex());
            }
            many.addEdge("owner", tx.addVertex());
            many.addEdge("owner", tx.addVertex());
            for (int i = 0; i < 3; i++) {
                few.addEdge("member", tx.addVertex());
            }
            few.addEdge("owner", tx.addVertex());
            Map<JanusGraphVertex, Iterable<JanusGraphEdge>> edges =
                tx.multiQuery(many, few).labels("member").direction(Direction.OUT).edges();
            assertEquals(MANY, Iterables.size(edges.get(many)));
            assertEquals(3, Iterables.size(edges.get(few)));
            edges = tx.multiQuery(many, few).labels("owner", "member").direction(Direction.OUT).edges();
            assertEquals(MANY + 2, Iterables.size(edges.get(many)));
            assertEquals(4, Iterables.size(edges.get(few)));
        } finally {
            tx.rollback();
        }
    }

    @ParameterizedTest
    @EnumSource(Transaction.class)
    public void shouldAnswerTheQueriesOfALoadedVertexWithALabelAndManyNewRelations(Transaction kind) {
        open();
        final JanusGraphManagement management = graph.openManagement();
        management.makeVertexLabel("person").make();
        management.makeEdgeLabel("member").make();
        management.makeEdgeLabel("owner").make();
        management.makePropertyKey("name").dataType(String.class).make();
        management.makePropertyKey("since").dataType(Integer.class).make();
        management.makePropertyKey("tags").dataType(String.class).cardinality(Cardinality.LIST).make();
        management.commit();
        final JanusGraphTransaction stored = graph.newTransaction();
        final JanusGraphVertex storedHub = stored.addVertex(T.label, "person", "name", "hub");
        storedHub.addEdge("member", stored.addVertex("name", "stored"), "since", 100);
        stored.commit();
        final JanusGraphTransaction tx = kind.open(graph);
        try {
            final JanusGraphVertex hub = tx.getVertex(storedHub.id());
            for (int i = 0; i < MANY; i++) {
                hub.addEdge("member", tx.addVertex("name", "m" + i), "since", i);
            }
            for (int i = 0; i < 3; i++) {
                hub.property(VertexProperty.Cardinality.list, "tags", "t" + i);
            }
            final GraphTraversalSource g = tx.traversal();
            assertEquals("person", g.V(hub).label().next());
            assertEquals(1L, (long) g.V(hub).hasLabel("person").count().next());
            assertEquals(3L, (long) g.V(hub).values("tags").count().next());
            assertEquals(MANY + 1L, (long) g.V(hub).outE().hasLabel("member").count().next());
            assertEquals(MANY - 30L + 1, (long) g.V(hub).outE("member").has("since", P.gte(30)).count().next());
            assertEquals(3L, (long) g.V(hub).outE("member").has("since", P.within(1, 2, 3)).count().next());
            g.V(hub).outE("member").has("since", P.lt(20)).drop().iterate();
            assertEquals(MANY - 20 + 1L, (long) g.V(hub).outE("member").count().next());
            assertEquals(0L, (long) g.V(hub).outE("owner").count().next());
        } finally {
            tx.rollback();
        }
    }

    //The schema vertices of a management transaction gain an edge per key of a vertex label and of an index
    @Test
    public void shouldDefineALabelAndAnIndexOfManyKeysInOneManagementTransaction() {
        open();
        JanusGraphManagement management = graph.openManagement();
        final VertexLabel label = management.makeVertexLabel("thing").make();
        final PropertyKey[] keys = new PropertyKey[MANY];
        for (int i = 0; i < MANY; i++) {
            keys[i] = management.makePropertyKey("k" + i).dataType(String.class).make();
        }
        management.addProperties(label, keys);
        assertEquals(MANY, label.mappedProperties().size());
        final JanusGraphManagement.IndexBuilder builder = management.buildIndex("byAll", Vertex.class);
        for (PropertyKey key : keys) {
            builder.addKey(key);
        }
        final JanusGraphIndex index = builder.buildCompositeIndex();
        assertEquals(MANY, index.getFieldKeys().length);
        management.commit();
        management = graph.openManagement();
        try {
            assertEquals(MANY, management.getVertexLabel("thing").mappedProperties().size());
            assertEquals(MANY, management.getGraphIndex("byAll").getFieldKeys().length);
        } finally {
            management.rollback();
        }
    }

    @Test
    public void shouldQueryAVertexWhileThreadsAddItsEdges() throws Exception {
        open();
        schema();
        final JanusGraphTransaction tx = graph.newTransaction();
        final ExecutorService pool = Executors.newFixedThreadPool(9);
        try {
            final JanusGraphVertex hub = tx.addVertex();
            final int threads = 8;
            final int edgesPerThread = 200;
            final CountDownLatch start = new CountDownLatch(1);
            final List<Future<?>> writers = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final String label = t % 2 == 0 ? "member" : "owner";
                writers.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < edgesPerThread; i++) {
                        hub.addEdge(label, tx.addVertex());
                    }
                    return null;
                }));
            }
            final AtomicBoolean done = new AtomicBoolean();
            final Future<?> reader = pool.submit(() -> {
                start.await();
                long previous = 0;
                while (!done.get()) {
                    final long members = Iterables.size(hub.query().labels("member").edges());
                    if (members < previous) {
                        throw new AssertionError("the edges of a label went from " + previous + " to " + members);
                    }
                    previous = members;
                    // Queries as often as it can, but lets the writers run
                    Thread.yield();
                }
                return null;
            });
            start.countDown();
            for (Future<?> writer : writers) {
                writer.get();
            }
            done.set(true);
            reader.get();
            assertEquals(threads / 2 * edgesPerThread, Iterables.size(hub.query().labels("member").edges()));
            assertEquals(threads / 2 * edgesPerThread, Iterables.size(hub.query().labels("owner").edges()));
            assertEquals(threads * edgesPerThread, Iterables.size(hub.query().edges()));
        } finally {
            pool.shutdownNow();
            tx.rollback();
        }
    }

    //The label of the edges is an instance of another transaction's, equal to the transaction's own by its id
    @Test
    public void shouldFindTheEdgesAddedWithALabelOfAnotherTransaction() {
        open();
        schema();
        final JanusGraphManagement management = graph.openManagement();
        final EdgeLabel foreign = management.getEdgeLabel("member");
        final StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
        try {
            final JanusGraphVertex hub = tx.addVertex();
            for (int i = 0; i < MANY; i++) {
                tx.addEdge(hub, tx.addVertex(), foreign);
            }
            hub.addEdge("owner", tx.addVertex());
            assertEquals(MANY, Iterables.size(hub.query().labels("member").edges()));
            assertEquals(MANY, Iterables.size(hub.query().types(foreign).edges()));
            assertEquals(1, Iterables.size(hub.query().labels("owner").edges()));
        } finally {
            tx.rollback();
            management.rollback();
        }
    }

    private static Set<String> edgeLabelsRead(InternalVertex vertex, RelationType... types) {
        final Set<String> labels = new HashSet<>();
        for (InternalRelation relation : vertex.getAddedRelations(new HashSet<>(List.of(types)), r -> true)) {
            if (relation.isEdge()) {
                labels.add(relation.getType().name());
            }
        }
        return labels;
    }

    //Up to the bound the vertex reads all of its new edges, beyond it only those of the labels asked for, and no edge for
    //a key; nothing for no types at all
    @ParameterizedTest
    @EnumSource(Transaction.class)
    public void shouldReadOnlyTheNewEdgesOfTheLabelsAskedForOnceThereAreMoreThanTheBound(Transaction kind) {
        open();
        schema();
        final JanusGraphTransaction tx = kind.open(graph);
        try {
            final EdgeLabel member = tx.getEdgeLabel("member");
            final EdgeLabel owner = tx.getEdgeLabel("owner");
            final PropertyKey name = tx.getPropertyKey("name");
            final InternalVertex hub = (InternalVertex) tx.addVertex("name", "hub");
            hub.addEdge("owner", tx.addVertex());
            for (int i = 1; i < SimpleAddedRelations.MAX_EDGES_WITHOUT_LABEL_INDEX; i++) {
                hub.addEdge("member", tx.addVertex());
            }
            assertEquals(Set.of("member", "owner"), edgeLabelsRead(hub, owner));

            hub.addEdge("member", tx.addVertex());
            assertEquals(Set.of("owner"), edgeLabelsRead(hub, owner));
            assertEquals(Set.of("member"), edgeLabelsRead(hub, member));
            assertEquals(Set.of("member", "owner"), edgeLabelsRead(hub, member, owner, name));
            assertTrue(edgeLabelsRead(hub, name).isEmpty());
            assertTrue(Iterables.any(hub.getAddedRelations(Set.of(name), r -> true), r -> r.getType().equals(name)));
            assertTrue(Iterables.isEmpty(hub.getAddedRelations(Collections.emptySet(), r -> true)));
            //Without types it reads all of them, the hidden property which marks a new vertex as existing too
            assertEquals(Iterables.size(hub.getAddedRelations(r -> true)), Iterables.size(hub.getAddedRelations(null, r -> true)));
            assertEquals(SimpleAddedRelations.MAX_EDGES_WITHOUT_LABEL_INDEX + 3,
                Iterables.size(hub.getAddedRelations(null, r -> true)));

            //An edge added after the label index was built, and one removed again, count in it as well
            final JanusGraphEdge late = hub.addEdge("owner", tx.addVertex());
            assertEquals(2, Iterables.size(hub.getAddedRelations(Set.of(owner), r -> true)));
            late.remove();
            assertEquals(1, Iterables.size(hub.getAddedRelations(Set.of(owner), r -> true)));
            assertEquals(1, Iterables.size(hub.query().labels("owner").direction(Direction.OUT).edges()));
        } finally {
            tx.rollback();
        }
    }

    //The container of a transaction keeps all of its new relations without an index by type
    @Test
    public void shouldReadAllRelationsOfAContainerWhichDoesNotGroupThemByType() {
        open();
        schema();
        final JanusGraphTransaction tx = graph.newTransaction();
        try {
            final EdgeLabel owner = tx.getEdgeLabel("owner");
            final JanusGraphVertex hub = tx.addVertex();
            final SimpleAddedRelations ungrouped = new SimpleAddedRelations(false);
            for (int i = 0; i <= SimpleAddedRelations.MAX_EDGES_WITHOUT_LABEL_INDEX; i++) {
                ungrouped.add((InternalRelation) hub.addEdge("member", tx.addVertex()));
            }
            ungrouped.add((InternalRelation) hub.addEdge("owner", tx.addVertex()));
            assertEquals(SimpleAddedRelations.MAX_EDGES_WITHOUT_LABEL_INDEX + 2,
                Iterables.size(ungrouped.getView(Set.of(owner), r -> true)));
            assertEquals(1, Iterables.size(ungrouped.getView(Set.of(owner), r -> r.getType().equals(owner))));
        } finally {
            tx.rollback();
        }
    }
}
