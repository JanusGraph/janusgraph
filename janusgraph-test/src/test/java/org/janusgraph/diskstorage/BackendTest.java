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

package org.janusgraph.diskstorage;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.inmemory.InMemoryStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.IndexSerializerTest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BackendTest {
    @Test
    public void testThreadsNamed() {
        ExecutorService executorService = Backend.buildExecutorService(Configuration.EMPTY);
        try {
            executorService.execute(() -> {
                String threadName = Thread.currentThread().getName();
                assertNotNull(threadName);
                assertFalse(threadName.isEmpty());
                assertTrue(threadName.toLowerCase().startsWith("backend"));
            });
        } finally {
            assertDoesNotThrow(executorService::shutdown);
        }
    }

    /**
     * The executor of the parallel backend operations, whose wait for its termination ends in an interrupt, as when
     * the thread which closes the backend is interrupted while it waits.
     */
    public static class InterruptedWaitExecutorService extends ThreadPoolExecutor {
        public InterruptedWaitExecutorService() {
            super(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            throw new InterruptedException();
        }
    }

    /**
     * An index provider which records whether the thread which closes it is interrupted.
     */
    public static class InterruptRecordingIndexProvider extends IndexSerializerTest.RecordingIndexProvider {
        static final AtomicReference<Boolean> CLOSED_INTERRUPTED = new AtomicReference<>();

        public InterruptRecordingIndexProvider(Configuration config) {
            super(config);
        }

        @Override
        public void close() {
            CLOSED_INTERRUPTED.set(Thread.currentThread().isInterrupted());
            super.close();
        }
    }

    /**
     * A store manager whose close leaves the thread interrupted, as the CQL store manager's does when an interrupt
     * ends the wait for its executors. Only while armed: opening a graph opens and closes a store manager of its own
     * to read the stored configuration, and an interrupt left by that close would reach the rest of the open, and
     * whichever step of it consumes the interrupt first, instead of the close under test.
     */
    public static class InterruptingCloseStoreManager extends InMemoryStoreManager {
        static final AtomicBoolean INTERRUPT_ON_CLOSE = new AtomicBoolean();

        public InterruptingCloseStoreManager(Configuration configuration) {
            super(configuration);
        }

        @Override
        public void close() throws BackendException {
            super.close();
            if (INTERRUPT_ON_CLOSE.get()) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    public void anInterruptRestoredByTheStoreManagersCloseReachesTheCallerAndNoIndexProvider() {
        InterruptRecordingIndexProvider.CLOSED_INTERRUPTED.set(null);
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, InterruptingCloseStoreManager.class.getName());
        config.set(GraphDatabaseConfiguration.INDEX_BACKEND, InterruptRecordingIndexProvider.class.getName(), "search");
        final JanusGraph graph = JanusGraphFactory.open(config.getConfiguration());

        final boolean interrupted;
        InterruptingCloseStoreManager.INTERRUPT_ON_CLOSE.set(true);
        try {
            graph.close();
        } finally {
            InterruptingCloseStoreManager.INTERRUPT_ON_CLOSE.set(false);
            interrupted = Thread.interrupted();
        }

        assertTrue(interrupted, "the interrupt was not restored");
        assertEquals(Boolean.FALSE, InterruptRecordingIndexProvider.CLOSED_INTERRUPTED.get(),
            "the index provider was closed on an interrupted thread, or not at all");
    }

    @Test
    public void anInterruptOfTheWaitForTheExecutorReachesTheCallerAndNoStepOfTheClose() {
        InterruptRecordingIndexProvider.CLOSED_INTERRUPTED.set(null);
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        config.set(GraphDatabaseConfiguration.PARALLEL_BACKEND_OPS, true);
        config.set(GraphDatabaseConfiguration.PARALLEL_BACKEND_EXECUTOR_SERVICE_CLASS,
            InterruptedWaitExecutorService.class.getName());
        config.set(GraphDatabaseConfiguration.INDEX_BACKEND, InterruptRecordingIndexProvider.class.getName(), "search");
        final JanusGraph graph = JanusGraphFactory.open(config.getConfiguration());

        final boolean interrupted;
        try {
            graph.close();
        } finally {
            interrupted = Thread.interrupted();
        }

        assertTrue(interrupted, "the interrupt was not restored");
        assertEquals(Boolean.FALSE, InterruptRecordingIndexProvider.CLOSED_INTERRUPTED.get(),
            "the index provider was closed on an interrupted thread, or not at all");
    }
}
