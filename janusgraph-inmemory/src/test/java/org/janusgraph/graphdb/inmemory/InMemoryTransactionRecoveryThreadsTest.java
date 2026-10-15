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

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.log.TransactionRecovery;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.LogManager;
import org.janusgraph.diskstorage.log.Message;
import org.janusgraph.diskstorage.log.MessageReader;
import org.janusgraph.diskstorage.log.ReadMarker;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.log.StandardTransactionLogProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Transaction recovery repairs transactions on threads of its own, not on the common pool, and its threads end when
 * it is shut down or its graph closes.
 */
public class InMemoryTransactionRecoveryThreadsTest {

    private static final String USER_LOG = "recovery-threads";
    private static final long WAIT_MS = TimeUnit.SECONDS.toMillis(30);
    //The least time recovery waits before it gives a transaction up, whatever tx.max-commit-time says
    private static final Duration LEAST_COMMIT_TIME = Duration.ofSeconds(5);

    /**
     * A user log which rejects every message, recording the thread which tried to add it. An add after the first, a
     * repair's, takes the given time before it fails.
     */
    public static class RejectingLogManager implements LogManager {
        static final List<Thread> ADDING_THREADS = new CopyOnWriteArrayList<>();
        static volatile long repairAddMillis = 0;

        public RejectingLogManager(Configuration config) {
        }

        @Override
        public Log openLog(String name) {
            return new Log() {
                @Override
                public Future<Message> add(StaticBuffer content) {
                    ADDING_THREADS.add(Thread.currentThread());
                    if (ADDING_THREADS.size() > 1 && repairAddMillis > 0) {
                        try {
                            Thread.sleep(repairAddMillis);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    throw new JanusGraphException("Log unavailable");
                }

                @Override
                public Future<Message> add(StaticBuffer content, StaticBuffer key) {
                    return add(content);
                }

                @Override
                public void registerReader(ReadMarker readMarker, MessageReader... reader) {
                }

                @Override
                public void registerReaders(ReadMarker readMarker, Iterable<MessageReader> readers) {
                }

                @Override
                public boolean unregisterReader(MessageReader reader) {
                    return false;
                }

                @Override
                public boolean unregisterReaderAndStopReadingProcess(MessageReader reader) {
                    return false;
                }

                @Override
                public String getName() {
                    return name;
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public void close() {
        }
    }

    private Set<Thread> threadsBefore;

    @BeforeEach
    public void setUp() {
        threadsBefore = Thread.getAllStackTraces().keySet();
        RejectingLogManager.ADDING_THREADS.clear();
        RejectingLogManager.repairAddMillis = 0;
    }

    private List<Thread> newThreadsNamed(String prefix) {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(t -> !threadsBefore.contains(t) && t.getName().startsWith(prefix))
            .collect(Collectors.toList());
    }

    private static void assertEnd(List<Thread> threads) throws InterruptedException {
        for (Thread thread : threads) {
            thread.join(WAIT_MS);
            assertFalse(thread.isAlive(), thread + " is still alive");
        }
    }

    private static JanusGraph openGraph() {
        return openGraph(Collections.emptyMap());
    }

    private static JanusGraph openGraph(Map<String, Object> settings) {
        final JanusGraphFactory.Builder builder = JanusGraphFactory.build()
            .set("storage.backend", "inmemory")
            .set("tx.log-tx", true)
            //Recovery gives a transaction up when it has read the log 5 s, the least it waits, past its first entry
            .set("tx.max-commit-time", 1000)
            .set("log.tx.read-interval", 100)
            .set("log.tx.read-lag-time", 50)
            .set("log.user.backend", RejectingLogManager.class.getName());
        settings.forEach(builder::set);
        return builder.open();
    }

    //Commits a transaction whose user-log write fails, which leaves it for recovery to repair
    private static void commitTransactionToRepair(JanusGraph graph) {
        final int adds = RejectingLogManager.ADDING_THREADS.size();
        final JanusGraphTransaction tx = graph.buildTransaction().logIdentifier(USER_LOG).start();
        tx.addVertex();
        tx.commit();
        assertEquals(adds + 1, RejectingLogManager.ADDING_THREADS.size());
    }

    private static long[] statistics(TransactionRecovery recovery) {
        return ((StandardTransactionLogProcessor) recovery).getStatistics();
    }

    //Waits until the recovery has attempted the repair, which fails, as the user log rejects the message again
    private static void awaitFailedRepair(TransactionRecovery recovery) throws InterruptedException {
        awaitFailedRepairs(recovery, 1);
    }

    private static void awaitFailedRepairs(TransactionRecovery recovery, int repairs) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
        while (statistics(recovery)[2] < repairs && System.nanoTime() - deadline < 0) {
            Thread.sleep(100);
        }
        assertArrayEquals(new long[]{0, repairs, repairs}, statistics(recovery));
    }

    @Test
    public void recoveryRepairsOnThreadsOfItsOwnWhichEndWhenItIsShutDown() throws Exception {
        final JanusGraph graph = openGraph();
        try {
            final Instant start = Instant.now();
            commitTransactionToRepair(graph);
            final TransactionRecovery recovery = JanusGraphFactory.startTransactionRecovery(graph, start);

            awaitFailedRepair(recovery);
            assertEquals(2, RejectingLogManager.ADDING_THREADS.size());
            final Thread repairThread = RejectingLogManager.ADDING_THREADS.get(1);
            assertTrue(repairThread.getName().startsWith("TxLogProcessorRepair-"), repairThread.getName());

            final long shutdownStart = System.nanoTime();
            recovery.shutdown();
            assertEnd(Collections.singletonList(repairThread));
            //Ended by shutdown(), not by the minute without work after which a repair thread ends anyway
            assertTrue(System.nanoTime() - shutdownStart < TimeUnit.SECONDS.toNanos(30),
                "shutdown() and the end of the repair thread took "
                    + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - shutdownStart) + " s");
            assertEnd(newThreadsNamed("TxLogProcessorRepair-"));
            assertEnd(newThreadsNamed("TxLogProcessorCleanup"));
        } finally {
            graph.close();
        }
    }

    @Test
    public void recoveryRepairsOnAsManyThreadsAsConfigured() throws Exception {
        final JanusGraph graph = openGraph(Collections.singletonMap("tx.recovery.repair-pool-size", 1));
        try {
            final Instant start = Instant.now();
            commitTransactionToRepair(graph);
            commitTransactionToRepair(graph);
            final TransactionRecovery recovery = JanusGraphFactory.startTransactionRecovery(graph, start);

            awaitFailedRepairs(recovery, 2);
            //The commits' adds, then the repairs', which ran on one thread
            assertEquals(4, RejectingLogManager.ADDING_THREADS.size());
            assertSame(RejectingLogManager.ADDING_THREADS.get(2), RejectingLogManager.ADDING_THREADS.get(3));
            assertEquals(1, newThreadsNamed("TxLogProcessorRepair-").size());
        } finally {
            graph.close();
        }
    }

    @Test
    public void aRepairThreadEndsOnceIdleForTheConfiguredTime() throws Exception {
        final JanusGraph graph = openGraph(Collections.singletonMap("tx.recovery.repair-keep-alive-time", 100));
        try {
            final Instant start = Instant.now();
            commitTransactionToRepair(graph);
            final TransactionRecovery recovery = JanusGraphFactory.startTransactionRecovery(graph, start);

            awaitFailedRepair(recovery);
            //While the recovery runs, rather than after the default minute
            assertEnd(Collections.singletonList(RejectingLogManager.ADDING_THREADS.get(1)));
        } finally {
            graph.close();
        }
    }

    @Test
    public void shutdownWaitsForTheRepairsItsRecoveryHasStarted() throws Exception {
        //Long enough for shutdown() to return before the repair, unless it waits for it
        RejectingLogManager.repairAddMillis = 2000;
        final JanusGraph graph = openGraph();
        try {
            final Instant start = Instant.now();
            commitTransactionToRepair(graph);
            final Instant committed = Instant.now();
            final TransactionRecovery recovery = JanusGraphFactory.startTransactionRecovery(graph, start);
            //Shut down once the log has been read past the moment the transaction is given up, plus the up to about a
            //second the cache's timer wheel takes to expire it
            final Instant givenUp = committed.plus(LEAST_COMMIT_TIME).plusMillis(1500);
            final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
            Instant progress = ((StandardJanusGraph) graph).getBackend().getSystemTxLog().getReadProgress();
            while ((progress == null || progress.isBefore(givenUp)) && System.nanoTime() - deadline < 0) {
                Thread.sleep(20);
                progress = ((StandardJanusGraph) graph).getBackend().getSystemTxLog().getReadProgress();
            }

            recovery.shutdown();

            assertArrayEquals(new long[]{0, 1, 1}, statistics(recovery), "shutdown() returned before the repair");
        } finally {
            graph.close();
        }
    }

    @Test
    public void recoveryEndsWhenItsGraphCloses() throws Exception {
        final JanusGraph graph = openGraph();
        try {
            final Instant start = Instant.now();
            commitTransactionToRepair(graph);
            final TransactionRecovery recovery = JanusGraphFactory.startTransactionRecovery(graph, start);
            awaitFailedRepair(recovery);
            assertEquals(1, newThreadsNamed("TxLogProcessorCleanup").size());
            assertEquals(1, newThreadsNamed("TxLogProcessorRepair-").size());
        } finally {
            graph.close();
        }
        //Never shut down, it ends with the graph: the cleaner, which runs every 5 s, and the repair thread, which
        //would otherwise wait a minute for more work
        assertEnd(newThreadsNamed("TxLogProcessorCleanup"));
        assertEnd(newThreadsNamed("TxLogProcessorRepair-"));
    }

    @Test
    public void aTransactionGivenUpOnAfterShutdownIsCountedAsNotRepairedAndNotRepaired() throws Exception {
        final JanusGraph graph = openGraph();
        try {
            final Instant start = Instant.now();
            commitTransactionToRepair(graph);
            final TransactionRecovery recovery = JanusGraphFactory.startTransactionRecovery(graph, start);
            //Before the transaction is given up on, which takes 5 s of log past its first entry
            recovery.shutdown();
            assertArrayEquals(new long[]{0, 0, 0}, statistics(recovery), "given up on before shutdown() returned");

            //Not recurring, the recovery reads the log on: the transactions committed from now on move its clock past
            //the moment the first one is given up on
            final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
            //The third statistic is the last one a transaction given up on after shutdown() increments
            while (statistics(recovery)[2] < 1 && System.nanoTime() - deadline < 0) {
                final JanusGraphTransaction tx = graph.newTransaction();
                tx.addVertex();
                tx.commit();
                Thread.sleep(200);
            }

            final long[] statistics = statistics(recovery);
            assertEquals(1, statistics[1], "given up on: " + Arrays.toString(statistics));
            assertEquals(1, statistics[2], "counted as repaired: " + Arrays.toString(statistics));
            assertEquals(1, RejectingLogManager.ADDING_THREADS.size(), "a repair wrote to the user log after shutdown()");
            assertEquals(Collections.emptyList(), newThreadsNamed("TxLogProcessorRepair-"));
        } finally {
            graph.close();
        }
    }
}
