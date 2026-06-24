// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.core;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ConsoleMutationListener;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.graphdb.tinkerpop.JanusGraphBlueprintsGraph;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

class ThreadLocalTxLeakTest {
    private ExecutorService executorService = Executors.newFixedThreadPool(1);
    private JanusGraph graph;

    @BeforeEach
    void setUp() {
        graph = JanusGraphFactory.open("inmemory");
    }

    @AfterEach
    void tearDown() {
        graph.close();
        executorService.shutdown();
    }

    /**
     * Before https://github.com/JanusGraph/janusgraph/pull/2472, event listeners are available across thread-bound transactions
     * They now have the same lifecycle of transaction, meaning they have to be registered again after previous transaction ends
     * This test demonstrates the workaround for the breaking change introduced in PR #2472.
     */
    @Test
    void eventListenersCanBeReusedAcrossTxs() {
        final StubMutationListener listener = new StubMutationListener(graph);
        final EventStrategy.Builder builder = EventStrategy.build()
            .eventQueue(new EventStrategy.TransactionalEventQueue(graph))
            .addListener(listener);
        final EventStrategy eventStrategy = builder.create();
        GraphTraversalSource gts = graph.traversal().withStrategies(eventStrategy);

        gts.addV().next();
        gts.addV().next();
        assertEquals(0, listener.addVertexEventRecorded());
        gts.tx().commit();
        assertEquals(2, listener.addVertexEventRecorded());

        // AbstractThreadLocalTransaction's transactionListeners are removed as part of previous commit. To reuse the
        // listeners, we need to register a new EventStrategy as a workaround
        gts = graph.traversal().withStrategies(EventStrategy.build()
            .eventQueue(new EventStrategy.TransactionalEventQueue(graph)).addListener(listener).create());

        gts.addV().next();
        assertEquals(2, listener.addVertexEventRecorded());
        gts.tx().commit();
        assertEquals(3, listener.addVertexEventRecorded());
    }

    @Test
    void threadLocalTxMustBeCleanedWhenClosed() throws Exception {
        CompletableFuture.runAsync(() -> {
            graph.addVertex();
            graph.tx().close();
        }, executorService).get();
        assertThreadLocalClosed();
    }

    @Test
    void threadLocalTxMustBeCleanedWhenRollbacked() throws Exception {
        CompletableFuture.runAsync(() -> {
            graph.tx().rollback();
        }, executorService).get();
        assertThreadLocalClosed();
    }

    @Test
    void threadLocalTxMustBeCleanedWhenCommitted() throws Exception {
        CompletableFuture.runAsync(() -> {
            graph.addVertex();
            graph.tx().commit();

            graph.tx().commit();
        }, executorService).get();

        assertThreadLocalClosed();
    }

    /**
     * Reproduces https://github.com/JanusGraph/janusgraph/issues/4899: when the graph is closed on another
     * thread (nulling the shared {@code txs} field) while a transaction is being closed, {@code doClose()}
     * used to throw a {@link NullPointerException} in its cleanup block, masking the real commit/rollback
     * outcome. After the fix, {@code doClose()} snapshots the {@code txs} reference and tolerates a null,
     * so the cleanup completes without throwing.
     */
    @Test
    void doCloseMustBeNullSafeWhenTxsClearedConcurrently() throws Exception {
        CompletableFuture.runAsync(() -> {
            // Simulate a concurrent close() that has already nulled the shared txs field before this
            // thread reaches the doClose() cleanup that runs in commit()/rollback()'s finally block.
            ThreadLocal<?> original = getThreadLocalTxs(graph);
            setThreadLocalTxs(graph, null);
            try {
                Assertions.assertDoesNotThrow(() -> invokeDoClose(graph),
                    "doClose() must not throw when txs has been nulled concurrently");
            } finally {
                // Restore the field so the @AfterEach close() does not trip over the artificially nulled txs.
                setThreadLocalTxs(graph, original);
            }
        }, executorService).get();
    }

    private void invokeDoClose(JanusGraph graph) {
        try {
            Field container = JanusGraphBlueprintsGraph.class.getDeclaredField("tinkerpopTxContainer");
            container.setAccessible(true);
            Object graphTransaction = container.get(graph);
            java.lang.reflect.Method doClose = graphTransaction.getClass().getDeclaredMethod("doClose");
            doClose.setAccessible(true);
            doClose.invoke(graphTransaction);
        } catch (ReflectiveOperationException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new JanusGraphException(e);
        }
    }

    private void setThreadLocalTxs(JanusGraph graph, ThreadLocal<?> value) {
        try {
            Field txs = JanusGraphBlueprintsGraph.class.getDeclaredField("txs");
            txs.setAccessible(true);
            txs.set(graph, value);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new JanusGraphException(e);
        }
    }

    private void assertThreadLocalClosed() {
        Assertions.assertDoesNotThrow(() -> {
            CompletableFuture.runAsync(
                () -> {
                    Assertions.assertNull(getThreadLocalTxs(graph).get());
                }, executorService).get();
        });
    }

    private ThreadLocal getThreadLocalTxs(JanusGraph graph) {
        try {
            Field txs = JanusGraphBlueprintsGraph.class.getDeclaredField("txs");
            txs.setAccessible(true);
            return (ThreadLocal) txs.get(graph);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new JanusGraphException(e);
        }
    }

    static class StubMutationListener extends ConsoleMutationListener {
        private final AtomicLong addVertexEvent = new AtomicLong(0);

        public StubMutationListener(Graph graph) {
            super(graph);
        }

        @Override
        public void vertexAdded(final Vertex vertex) {
            addVertexEvent.incrementAndGet();
        }

        public long addVertexEventRecorded() {
            return addVertexEvent.get();
        }
    }
}
