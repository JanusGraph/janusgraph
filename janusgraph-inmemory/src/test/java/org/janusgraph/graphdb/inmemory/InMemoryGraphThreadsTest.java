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

package org.janusgraph.graphdb.inmemory;

import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.olap.OLAPTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The threads which a graph runs its background work on: how many there are, which pool they belong to, and that
 * none outlives the graph.
 */
public class InMemoryGraphThreadsTest {

    private static final long WAIT_MS = TimeUnit.SECONDS.toMillis(15);
    //Threads which the JVM keeps for all its code, and which outlive a graph by design: the common pool's, and threads
    //the JVM itself may start at any time
    private static final Predicate<Thread> JVM_WIDE = t -> Stream.of("ForkJoinPool.commonPool-worker-",
            "Attach Listener", "process reaper", "JFR ", "CompletableFutureDelayScheduler", "Common-Cleaner")
        .anyMatch(t.getName()::startsWith);

    private Set<Thread> threadsBefore;

    @BeforeEach
    public void recordThreads() {
        threadsBefore = Thread.getAllStackTraces().keySet();
    }

    private List<Thread> newThreads(Predicate<Thread> filter) {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(t -> !threadsBefore.contains(t) && filter.test(t))
            .collect(Collectors.toList());
    }

    private static Predicate<Thread> named(String prefix) {
        return t -> t.getName().startsWith(prefix);
    }

    private static JanusGraph openGraph() {
        return openGraph(Collections.emptyMap());
    }

    private static JanusGraph openGraph(Map<String, Object> settings) {
        final JanusGraphFactory.Builder builder = JanusGraphFactory.build()
            .set("storage.backend", "inmemory")
            .set("tx.log-tx", true)
            //Blocks of 50 ids, so that the writes below renew the blocks of many pools many times
            .set("ids.block-size", 50)
            //Schema changes arrive within a tenth of a second, rather than within the default five seconds
            .set("log.janusgraph.read-interval", 100)
            .set("log.janusgraph.send-delay", 0);
        settings.forEach(builder::set);
        final JanusGraph graph = builder.open();
        //Defined up front: the writers' transactions would contend to define it
        final JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makeEdgeLabel("knows").make();
        mgmt.commit();
        return graph;
    }

    //Adds vertices with an edge each from several threads at once, which take their ids from the pools of several
    //partitions
    private static void addVerticesConcurrently(JanusGraph graph) throws InterruptedException {
        final List<Throwable> failures = new CopyOnWriteArrayList<>();
        final List<Thread> writers = new ArrayList<>();
        for (int w = 0; w < 8; w++) {
            final Thread writer = new Thread(() -> {
                try {
                    for (int t = 0; t < 10; t++) {
                        final JanusGraphTransaction tx = graph.newTransaction();
                        for (int i = 0; i < 100; i++) {
                            final JanusGraphVertex v = tx.addVertex();
                            v.addEdge("knows", v);
                        }
                        tx.commit();
                    }
                } catch (Throwable e) {
                    failures.add(e);
                }
            });
            writers.add(writer);
            writer.start();
        }
        for (Thread writer : writers) {
            writer.join(TimeUnit.MINUTES.toMillis(2));
            assertFalse(writer.isAlive(), "a writer is still running after two minutes");
        }
        assertEquals(new ArrayList<>(), failures);
    }

    private void assertNoNewThreadOutlives(Predicate<Thread> excluded) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
        List<Thread> alive = newThreads(excluded.negate());
        while (!alive.isEmpty() && System.nanoTime() - deadline < 0) {
            Thread.sleep(50);
            alive = newThreads(excluded.negate());
        }
        assertEquals(new ArrayList<>(), alive.stream().map(Thread::getName).collect(Collectors.toList()));
    }

    @Test
    public void theIdBlocksOfAGraphRenewOnSharedThreads() throws InterruptedException {
        final JanusGraph graph = openGraph();
        try {
            addVerticesConcurrently(graph);

            assertEquals(new ArrayList<>(), newThreads(named("JanusGraphID(")), "threads of a pool of its own");
            final List<Thread> renewalThreads = newThreads(named("JanusGraphID-renewal-"));
            assertFalse(renewalThreads.isEmpty());
            //At most a thread per pool the graph can have: three per partition of the default 32, and two more
            assertTrue(renewalThreads.size() <= 3 * 32 + 2, "renewal threads: " + renewalThreads);
        } finally {
            graph.close();
        }
        assertNoNewThreadOutlives(JVM_WIDE);
    }

    @Test
    public void theIdBlocksOfAGraphRenewOnAsManyThreadsAsConfigured() throws InterruptedException {
        final JanusGraph graph = openGraph(Collections.singletonMap("ids.renew-pool-size", 1));
        try {
            addVerticesConcurrently(graph);

            //A thread ends only after a minute without a renewal, so the threads which renewed blocks are still alive
            assertEquals(1, newThreads(named("JanusGraphID-renewal-")).size());
        } finally {
            graph.close();
        }
        assertNoNewThreadOutlives(JVM_WIDE);
    }

    @Test
    public void theIdRenewalThreadsOfAGraphEndOnceIdleForTheConfiguredTime() throws InterruptedException {
        //Long enough for the threads which renewed blocks during the last commits to be alive after the writes
        final JanusGraph graph = openGraph(Collections.singletonMap("ids.renew-keep-alive-time", 3000));
        try {
            addVerticesConcurrently(graph);
            final List<Thread> renewalThreads = newThreads(named("JanusGraphID-renewal-"));
            assertFalse(renewalThreads.isEmpty());

            //While the graph is open, rather than after the default minute
            for (Thread renewalThread : renewalThreads) {
                renewalThread.join(WAIT_MS);
                assertFalse(renewalThread.isAlive(), renewalThread.getName());
            }
        } finally {
            graph.close();
        }
    }

    //A job which records the thread which runs it: setup() runs on that thread
    private static OLAPTest.DegreeCounter recordingThreadOf(List<Thread> jobThreads) {
        return recordingThreadOf(jobThreads, new CountDownLatch(0));
    }

    //A job which records the thread which runs it, and then waits for the latch to go on
    private static OLAPTest.DegreeCounter recordingThreadOf(List<Thread> jobThreads, CountDownLatch goOn) {
        return new OLAPTest.DegreeCounter() {
            @Override
            public void setup(Memory memory) {
                jobThreads.add(Thread.currentThread());
                try {
                    if (!goOn.await(WAIT_MS, TimeUnit.MILLISECONDS)) {
                        throw new IllegalStateException("the job was never let go on");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        };
    }

    @Test
    public void anOlapJobRunsOnAThreadOfTheComputersAndNotOfTheCommonPool() throws Exception {
        final List<Thread> jobThreads = new CopyOnWriteArrayList<>();
        final JanusGraph graph = openGraph();
        try {
            addVerticesConcurrently(graph);
            final ComputerResult result = graph.compute().program(recordingThreadOf(jobThreads)).submit().get();
            result.close();

            assertEquals(1, jobThreads.size());
            final Thread jobThread = jobThreads.get(0);
            assertTrue(jobThread.getName().startsWith("FulgoraGraphComputer-"), jobThread.getName());
            assertTrue(jobThread.isDaemon());
        } finally {
            graph.close();
        }
        assertNoNewThreadOutlives(JVM_WIDE);
    }

    @Test
    public void anOlapJobSubmittedOnceItsGraphHasClosedFails() {
        final JanusGraph graph = openGraph();
        graph.close();

        final Future<ComputerResult> job = graph.compute().program(new OLAPTest.DegreeCounter()).submit();
        final ExecutionException e = assertThrows(ExecutionException.class, job::get);
        assertTrue(e.getCause() instanceof IllegalStateException, e.toString());
    }

    //The graph closes while one job holds its single thread and another waits in the queue: the running one fails
    //once it goes on, as its graph has closed, and the queued one fails as soon as it starts, before any of its work
    @Test
    public void anOlapJobStillQueuedWhenItsGraphClosesFails() throws Exception {
        final Map<String, Object> settings = new HashMap<>();
        settings.put("computer.job-pool-size", 1);
        final JanusGraph graph = openGraph(settings);
        final CountDownLatch graphClosed = new CountDownLatch(1);
        final Future<ComputerResult> running = graph.compute()
            .program(recordingThreadOf(new CopyOnWriteArrayList<>(), graphClosed)).submit();
        final List<Thread> queuedJobThreads = new CopyOnWriteArrayList<>();
        final Future<ComputerResult> queued = graph.compute()
            .program(recordingThreadOf(queuedJobThreads, new CountDownLatch(0))).submit();
        graph.close();
        graphClosed.countDown();

        assertThrows(ExecutionException.class, () -> running.get(WAIT_MS, TimeUnit.MILLISECONDS));
        final ExecutionException e = assertThrows(ExecutionException.class, () -> queued.get(WAIT_MS, TimeUnit.MILLISECONDS));
        assertTrue(e.getCause() instanceof IllegalStateException, e.toString());
        assertEquals("Graph has been shut down", e.getCause().getMessage());
        assertTrue(queuedJobThreads.isEmpty(), "the queued job's program was set up on " + queuedJobThreads);
    }

    @Test
    public void theOlapJobsOfAGraphRunOnAsManyThreadsAsConfiguredWhichEndOnceIdleForTheConfiguredTime()
            throws Exception {
        final List<Thread> jobThreads = new CopyOnWriteArrayList<>();
        final Map<String, Object> settings = new HashMap<>();
        settings.put("computer.job-pool-size", 1);
        settings.put("computer.job-keep-alive-time", 100);
        final JanusGraph graph = openGraph(settings);
        try {
            addVerticesConcurrently(graph);
            //Both at once: the first holds the thread until the second is submitted, which then waits for it, and runs
            //on its thread
            final CountDownLatch bothSubmitted = new CountDownLatch(1);
            final List<Future<ComputerResult>> jobs = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                jobs.add(graph.compute().program(recordingThreadOf(jobThreads, bothSubmitted)).submit());
            }
            bothSubmitted.countDown();
            for (Future<ComputerResult> job : jobs) {
                job.get().close();
            }

            assertEquals(2, jobThreads.size());
            assertSame(jobThreads.get(0), jobThreads.get(1));
            //While the graph is open, rather than after the default minute
            jobThreads.get(0).join(WAIT_MS);
            assertFalse(jobThreads.get(0).isAlive());
        } finally {
            graph.close();
        }
    }

    @Test
    public void noThreadOfAGraphOutlivesItsClose() throws Exception {
        final JanusGraph graph = openGraph();
        try {
            addVerticesConcurrently(graph);
            //A transaction recovery which is never shut down: it ends with the graph
            JanusGraphFactory.startTransactionRecovery(graph, Instant.now());
            //Acknowledged evictions, while a transaction is open
            final JanusGraphTransaction open = graph.newTransaction();
            open.addVertex();
            for (int i = 0; i < 5; i++) {
                final JanusGraphManagement mgmt = graph.openManagement();
                mgmt.makePropertyKey("p" + i).dataType(String.class).make();
                mgmt.commit();
            }
            for (int i = 0; i < 5; i++) {
                final JanusGraphManagement mgmt = graph.openManagement();
                mgmt.changeName(mgmt.getPropertyKey("p" + i), "q" + i);
                mgmt.commit();
            }
            //The acknowledgements wait for the open transaction
            final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
            while (newThreads(named("ManagementLogger-ack-")).isEmpty() && System.nanoTime() - deadline < 0) {
                Thread.sleep(50);
            }
            assertEquals(1, newThreads(named("ManagementLogger-ack-")).size());
            graph.compute().program(new OLAPTest.DegreeCounter()).submit().get().close();
        } finally {
            graph.close();
        }
        assertNoNewThreadOutlives(JVM_WIDE);
    }
}
