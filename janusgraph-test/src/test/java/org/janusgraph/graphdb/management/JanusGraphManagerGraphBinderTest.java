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

package org.janusgraph.graphdb.management;

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.janusgraph.core.ConfiguredGraphFactory;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.inmemory.InMemoryStoreManager;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.script.SimpleBindings;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The thread which binds the graphs of the {@link org.janusgraph.core.ConfiguredGraphFactory} to the script engine of
 * Gremlin Server ends with the server, instead of running on as a thread which keeps the JVM from exiting.
 */
public class JanusGraphManagerGraphBinderTest {

    private static final String BINDER_THREAD_NAME = "JanusGraphManager-graph-binder";
    private static final long WAIT_MS = TimeUnit.SECONDS.toMillis(10);

    private StandardJanusGraph configurationGraph;
    private ExecutorService serverExecutor;

    @BeforeEach
    public void setUp() {
        serverExecutor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    public void tearDown() throws Exception {
        final JanusGraphManager manager = JanusGraphManager.getInstance();
        if (manager != null) {
            for (final String graphName : manager.getGraphNames()) {
                manager.getGraph(graphName).close();
            }
        }
        JanusGraphManager.shutdownJanusGraphManager();
        ConfigurationManagementGraph.shutdownConfigurationManagementGraph();
        if (configurationGraph != null) {
            configurationGraph.close();
        }
        serverExecutor.shutdownNow();
        assertTrue(serverExecutor.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS), "the server executor did not end");
    }

    //A ConfigurationManagementGraph without graphs, so that binding runs list no graph names, and fail at none
    private void enableConfigurationManagementGraph() {
        configurationGraph = (StandardJanusGraph) JanusGraphFactory.open("inmemory");
        new ConfigurationManagementGraph(configurationGraph);
    }

    //The configuration of an inmemory graph of the ConfiguredGraphFactory, which binding runs then open
    private static void createGraphConfiguration(String graphName) {
        createGraphConfiguration(graphName, "inmemory");
    }

    private static void createGraphConfiguration(String graphName, String storageBackend) {
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), storageBackend);
        map.put(GRAPH_NAME.toStringWithoutRoot(), graphName);
        ConfiguredGraphFactory.createConfiguration(ConfigurationUtil.loadMapConfiguration(map));
    }

    /**
     * A store manager which records that its graph is opening and that it was closed, so that a test can have the
     * server stop while a graph of the binder opens.
     */
    public static class OpeningStoreManager extends InMemoryStoreManager {
        static final AtomicBoolean OPENING = new AtomicBoolean();
        static final AtomicBoolean CLOSED = new AtomicBoolean();

        public OpeningStoreManager(Configuration configuration) {
            super(configuration);
            OPENING.set(true);
        }

        @Override
        public void close() throws BackendException {
            CLOSED.set(true);
            super.close();
        }
    }

    //A Gremlin executor with the given executor service, which records the thread of each binding run in the given
    //queue, as a run starts by asking for the executor service
    private static GremlinExecutor gremlinExecutorOf(ExecutorService executorService, BlockingQueue<Thread> runs) {
        final GremlinExecutor gremlinExecutor = mock(GremlinExecutor.class);
        when(gremlinExecutor.getExecutorService()).thenAnswer(invocation -> {
            runs.add(Thread.currentThread());
            return executorService;
        });
        //With bindings, which binding a graph adds to and unbinding one removes from
        final GremlinScriptEngineManager scriptEngineManager = mock(GremlinScriptEngineManager.class);
        when(scriptEngineManager.getBindings()).thenReturn(new SimpleBindings());
        when(gremlinExecutor.getScriptEngineManager()).thenReturn(scriptEngineManager);
        return gremlinExecutor;
    }

    //The thread of the first binding run, which must be the binder's named daemon thread
    private static Thread firstRunThread(BlockingQueue<Thread> runs) throws InterruptedException {
        final Thread binder = runs.poll(WAIT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(binder, "no binding run");
        assertEquals(BINDER_THREAD_NAME, binder.getName());
        assertTrue(binder.isDaemon(), "the binder is not a daemon thread");
        return binder;
    }

    //Waits until the binder has finished its run and waits for the next one, 20 s later. The observation is what
    //counts: a thread in a timed wait is seen runnable for an instant whenever it wakes to check its time
    private static void awaitIdle(Thread binder) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
        while (System.nanoTime() - deadline < 0) {
            final Thread.State state = binder.getState();
            if (state == Thread.State.TIMED_WAITING || state == Thread.State.WAITING) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("the binder did not come to wait for its next run within " + WAIT_MS + " ms");
    }

    private static void assertEnds(Thread thread) throws InterruptedException {
        thread.join(WAIT_MS);
        assertFalse(thread.isAlive(), thread + " is still alive");
    }

    @Test
    public void bindsOnADaemonThreadWhichEndsWithTheManager() throws InterruptedException {
        enableConfigurationManagementGraph();
        final JanusGraphManager manager = new JanusGraphManager(new Settings());
        final BlockingQueue<Thread> runs = new LinkedBlockingQueue<>();

        manager.configureGremlinExecutor(gremlinExecutorOf(serverExecutor, runs));
        final Thread binder = firstRunThread(runs);
        awaitIdle(binder);

        JanusGraphManager.shutdownJanusGraphManager();
        assertEnds(binder);
    }

    @Test
    public void aBinderEndsAtItsFirstRunAfterTheServerHasStopped() throws InterruptedException {
        enableConfigurationManagementGraph();
        final JanusGraphManager manager = new JanusGraphManager(new Settings());
        final BlockingQueue<Thread> runs = new LinkedBlockingQueue<>();
        serverExecutor.shutdown();

        manager.configureGremlinExecutor(gremlinExecutorOf(serverExecutor, runs));

        assertEnds(firstRunThread(runs));
    }

    @Test
    public void aRunUnderWayOpensNoFurtherGraphOnceTheServerHasStopped() throws InterruptedException {
        enableConfigurationManagementGraph();
        final JanusGraphManager manager = new JanusGraphManager(new Settings());
        createGraphConfiguration("graph1");
        createGraphConfiguration("graph2");
        //The run asks whether the server has stopped as it starts, before it opens a graph and after it has opened one:
        //the server stops, shutting its executor service down, once the run has opened and bound the first of the two
        //graphs, so the fourth check, the one before the second graph, finds it stopped
        final AtomicInteger checks = new AtomicInteger();
        final ExecutorService stoppingExecutor = mock(ExecutorService.class);
        when(stoppingExecutor.isShutdown()).thenAnswer(invocation -> checks.incrementAndGet() >= 4);
        final BlockingQueue<Thread> runs = new LinkedBlockingQueue<>();

        manager.configureGremlinExecutor(gremlinExecutorOf(stoppingExecutor, runs));

        assertEnds(firstRunThread(runs));
        assertEquals(1, manager.getGraphNames().size(), () -> "bound graphs: " + manager.getGraphNames());
    }

    @Test
    public void aGraphWhichOpensAfterTheServerStoppedIsClosed() throws InterruptedException {
        enableConfigurationManagementGraph();
        final JanusGraphManager manager = new JanusGraphManager(new Settings());
        OpeningStoreManager.OPENING.set(false);
        OpeningStoreManager.CLOSED.set(false);
        createGraphConfiguration("graph1", OpeningStoreManager.class.getName());
        //The server stops, shutting its executor service down, while the graph's storage opens
        final ExecutorService stoppingExecutor = mock(ExecutorService.class);
        when(stoppingExecutor.isShutdown()).thenAnswer(invocation -> OpeningStoreManager.OPENING.get());
        final BlockingQueue<Thread> runs = new LinkedBlockingQueue<>();

        manager.configureGremlinExecutor(gremlinExecutorOf(stoppingExecutor, runs));

        assertEnds(firstRunThread(runs));
        assertEquals(Collections.emptySet(), manager.getGraphNames(), "the graph stayed bound");
        assertEquals(Collections.emptySet(), manager.getTraversalSourceNames(), "the traversal source stayed bound");
        assertTrue(OpeningStoreManager.CLOSED.get(), "the graph was not closed");
    }

    @Test
    public void aBinderEndsWhenNoConfigurationManagementGraphIsConfigured() throws InterruptedException {
        //Which a Gremlin Server need not have
        final JanusGraphManager manager = new JanusGraphManager(new Settings());
        final BlockingQueue<Thread> runs = new LinkedBlockingQueue<>();

        manager.configureGremlinExecutor(gremlinExecutorOf(serverExecutor, runs));

        assertEnds(firstRunThread(runs));
    }

    @Test
    public void aBinderTriesAgainAfterARunWhichFailsForAnotherReason() throws InterruptedException {
        enableConfigurationManagementGraph();
        //Listing the graphs fails while the graph which keeps their configurations is closed
        configurationGraph.close();
        final JanusGraphManager manager = new JanusGraphManager(new Settings());
        final BlockingQueue<Thread> runs = new LinkedBlockingQueue<>();

        manager.configureGremlinExecutor(gremlinExecutorOf(serverExecutor, runs));

        final Thread binder = firstRunThread(runs);
        awaitIdle(binder);
        assertTrue(binder.isAlive());
    }

    @Test
    public void configuringAnotherExecutorReplacesTheBinder() throws InterruptedException {
        enableConfigurationManagementGraph();
        final JanusGraphManager manager = new JanusGraphManager(new Settings());
        final BlockingQueue<Thread> firstRuns = new LinkedBlockingQueue<>();
        manager.configureGremlinExecutor(gremlinExecutorOf(serverExecutor, firstRuns));
        final Thread firstBinder = firstRunThread(firstRuns);
        awaitIdle(firstBinder);

        final BlockingQueue<Thread> secondRuns = new LinkedBlockingQueue<>();
        manager.configureGremlinExecutor(gremlinExecutorOf(serverExecutor, secondRuns));

        assertEnds(firstBinder);
        final Thread secondBinder = firstRunThread(secondRuns);
        awaitIdle(secondBinder);
        assertTrue(secondBinder.isAlive());
    }
}
