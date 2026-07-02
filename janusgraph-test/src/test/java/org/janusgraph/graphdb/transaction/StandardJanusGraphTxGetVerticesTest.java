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

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.transaction.vertexcache.VertexCache;
import org.janusgraph.util.datastructures.Retriever;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;

class StandardJanusGraphTxGetVerticesTest {

    private JanusGraph graph;

    @BeforeEach
    void setUp() {
        graph = JanusGraphFactory.open("inmemory");
    }

    @AfterEach
    void tearDown() {
        graph.close();
    }

    /**
     * Reproduces https://github.com/JanusGraph/janusgraph/issues/4907: {@code getVertices()} calls
     * {@code vertexCache.contains(id)} and then {@code vertexCache.get(id, ...)}. If the transaction is
     * released concurrently (e.g. {@code graph.close()} on another thread) between those two calls, the
     * cache is swapped to an {@code EmptyVertexCache} whose {@code get()} returns null after this method
     * has already passed {@code verifyOpen()}. The null was added to the result list and then dereferenced
     * by {@code result.removeIf(JanusGraphElement::isRemoved)}, throwing a {@link NullPointerException}.
     * After the fix, {@code getVertices()} tolerates null entries just like {@code getVertex()} does.
     */
    @Test
    void getVerticesMustBeNullSafeWhenCacheReturnsNullConcurrently() throws Exception {
        // Commit a vertex so it gets a permanent, valid (positive) id that survives isValidVertexId().
        JanusGraphVertex vertex = graph.addVertex();
        graph.tx().commit();
        Object id = vertex.id();

        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
        try {
            // Simulate the race window: contains() still reports the vertex, but get() now returns null
            // because a concurrent transaction release swapped in an EmptyVertexCache between the two calls.
            setVertexCache(tx, new ContainsButReturnsNullVertexCache());

            Iterable<JanusGraphVertex> result = assertDoesNotThrow(() -> tx.getVertices(id),
                "getVertices() must not throw when the vertex cache returns null concurrently");
            assertFalse(result.iterator().hasNext(), "null vertex must be filtered out of the result");
        } finally {
            tx.rollback();
        }
    }

    private void setVertexCache(StandardJanusGraphTx tx, VertexCache cache) throws Exception {
        Field field = StandardJanusGraphTx.class.getDeclaredField("vertexCache");
        field.setAccessible(true);
        field.set(tx, cache);
    }

    /**
     * Stub cache modelling the moment during a concurrent transaction release when {@code contains()} has
     * already returned true but the backing cache has been swapped so that {@code get()} now returns null.
     */
    private static class ContainsButReturnsNullVertexCache implements VertexCache {
        @Override
        public boolean contains(Object id) {
            return true;
        }

        @Override
        public InternalVertex get(Object id, Retriever<Object, InternalVertex> retriever) {
            return null;
        }

        @Override
        public void add(InternalVertex vertex, Object id) {
        }

        @Override
        public List<InternalVertex> getAllNew() {
            return Collections.emptyList();
        }

        @Override
        public void close() {
        }
    }
}
