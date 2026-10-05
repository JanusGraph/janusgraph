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

package org.janusgraph.graphdb.transaction;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A graph query in a transaction which changed vertices finds those which match now, whatever the index it reads still
 * holds for them, and whatever the order of its conditions.
 */
class StandardJanusGraphTxGraphQueryTest {

    private JanusGraph graph;

    @BeforeEach
    void setUp() {
        graph = JanusGraphFactory.open("inmemory");
        final JanusGraphManagement mgmt = graph.openManagement();
        final PropertyKey uid = mgmt.makePropertyKey("uid").dataType(String.class).make();
        final PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        final PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).make();
        final PropertyKey kind = mgmt.makePropertyKey("kind").dataType(String.class).make();
        final PropertyKey tag = mgmt.makePropertyKey("tag").dataType(String.class).cardinality(Cardinality.LIST).make();
        mgmt.buildIndex("byUid", Vertex.class).addKey(uid).buildCompositeIndex();
        mgmt.buildIndex("byNameAndAge", Vertex.class).addKey(name).addKey(age).buildCompositeIndex();
        mgmt.buildIndex("byTagAndKind", Vertex.class).addKey(tag).addKey(kind).buildCompositeIndex();
        mgmt.commit();
    }

    @AfterEach
    void tearDown() {
        graph.close();
    }

    private static Set<Object> ids(List<Vertex> vertices) {
        return vertices.stream().map(Vertex::id).collect(Collectors.toSet());
    }

    //The index on both keys still holds the old age: the vertex matches through the key which the transaction changed
    @Test
    void findsAVertexWhoseSecondKeyOfAnIndexChanged() {
        final Object alice = graph.addVertex("name", "alice", "age", 1).id();
        graph.tx().commit();

        final GraphTraversalSource g = graph.traversal();
        g.V(alice).property("age", 2).iterate();
        assertEquals(Collections.singleton(alice), ids(g.V().has("name", "alice").has("age", 2).toList()));
        assertEquals(Collections.singleton(alice), ids(g.V().has("age", 2).has("name", "alice").toList()));
        assertEquals(0, g.V().has("name", "alice").has("age", 1).count().next());
        graph.tx().rollback();
    }

    @Test
    void findsNewAndChangedVerticesByAnEqualityAndByWithin() {
        final Object loaded = graph.addVertex("uid", "u1").id();
        graph.tx().commit();

        final GraphTraversalSource g = graph.traversal();
        final Object added = g.addV().property("uid", "u2").next().id();
        g.V(loaded).property("uid", "u3").iterate();
        assertEquals(Collections.singleton(added), ids(g.V().has("uid", "u2").toList()));
        assertEquals(Collections.singleton(loaded), ids(g.V().has("uid", "u3").toList()));
        assertEquals(0, g.V().has("uid", "u1").count().next());
        assertEquals(2, g.V().has("uid", P.within("u2", "u3", "u4")).count().next());
        graph.tx().rollback();
    }

    //A vertex which another value of a list joins: the index holds the vertex under its old values only
    @Test
    void findsAVertexWhichAListValueJoined() {
        final Object tagged = graph.addVertex("tag", "red", "kind", "car").id();
        graph.tx().commit();

        final GraphTraversalSource g = graph.traversal();
        g.V(tagged).property(VertexProperty.Cardinality.list, "tag", "blue").iterate();
        assertEquals(Collections.singleton(tagged), ids(g.V().has("tag", "blue").has("kind", "car").toList()));
        assertEquals(Collections.singleton(tagged), ids(g.V().has("kind", "car").has("tag", "blue").toList()));
        assertEquals(Collections.singleton(tagged), ids(g.V().has("kind", "car").has("tag", "red").toList()));
        graph.tx().rollback();
    }

    //A query of keys with values which most new vertices share, and one key which tells them apart, finds each vertex
    @Test
    void findsNewVerticesOfAKindByTheirUniqueKey() {
        final GraphTraversalSource g = graph.traversal();
        for (int i = 0; i < 50; i++) {
            g.addV().property("name", "same").property("age", i).iterate();
        }
        for (int i = 0; i < 50; i++) {
            assertEquals(1, g.V().has("name", "same").has("age", i).count().next());
            assertEquals(1, g.V().has("age", i).has("name", "same").count().next());
        }
        graph.tx().rollback();
    }

    //A changed vertex which the index returns too, under its old age, comes once from an ordered query
    @Test
    void returnsAChangedVertexOnceInAnOrderedQuery() {
        final Object alice = graph.addVertex("name", "alice", "age", 2).id();
        graph.tx().commit();

        final GraphTraversalSource g = graph.traversal();
        g.V(alice).property("age", 2).iterate();
        assertEquals(Collections.singletonList(alice), g.V().has("name", "alice").has("age", 2).order().by("age").id().toList());
        graph.tx().rollback();
    }

    //A transaction which isn't bound to a thread uses the cache for several threads, which picks the same vertices
    @Test
    void findsNewVerticesOfAKindByTheirUniqueKeyInATransactionOfItsOwn() {
        final JanusGraphTransaction tx = graph.newTransaction();
        try {
            final GraphTraversalSource g = tx.traversal();
            for (int i = 0; i < 50; i++) {
                g.addV().property("name", "same").property("age", i).iterate();
            }
            for (int i = 0; i < 50; i++) {
                assertEquals(1, g.V().has("name", "same").has("age", i).count().next());
                assertEquals(1, g.V().has("age", i).has("name", "same").count().next());
            }
        } finally {
            tx.rollback();
        }
    }

    @Test
    void forgetsRemovedVerticesAndValues() {
        final Object loaded = graph.addVertex("uid", "u1").id();
        graph.tx().commit();

        final GraphTraversalSource g = graph.traversal();
        final Object added = g.addV().property("uid", "u2").next().id();
        assertEquals(1, g.V().has("uid", "u2").count().next());
        g.V(added).drop().iterate();
        g.V(loaded).drop().iterate();
        assertEquals(0, g.V().has("uid", "u2").count().next());
        assertEquals(0, g.V().has("uid", "u1").count().next());

        final Object renamed = g.addV().property("uid", "u5").next().id();
        assertEquals(1, g.V().has("uid", "u5").count().next());
        g.V(renamed).property("uid", "u6").iterate();
        assertEquals(0, g.V().has("uid", "u5").count().next());
        assertEquals(Collections.singleton(renamed), ids(g.V().has("uid", "u6").toList()));
        graph.tx().rollback();
    }

    //A transaction which several threads use keeps its index of added properties under a lock, and works alike
    @Test
    void findsChangedVerticesInAThreadedTransaction() {
        final Object alice = graph.addVertex("name", "alice", "age", 1).id();
        graph.tx().commit();

        final JanusGraphTransaction tx = graph.tx().createThreadedTx();
        try {
            final GraphTraversalSource g = tx.traversal();
            g.V(alice).property("age", 2).iterate();
            final Object added = g.addV().property("name", "bob").property("age", 2).next().id();
            assertEquals(Collections.singleton(alice), ids(g.V().has("name", "alice").has("age", 2).toList()));
            assertEquals(Collections.singleton(added), ids(g.V().has("name", "bob").has("age", 2).toList()));
        } finally {
            tx.rollback();
        }
    }
}
