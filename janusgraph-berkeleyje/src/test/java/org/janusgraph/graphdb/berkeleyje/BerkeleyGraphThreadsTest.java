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

package org.janusgraph.graphdb.berkeleyje;

import org.janusgraph.BerkeleyStorageSetup;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.olap.OLAPTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.COMPUTER_JOB_POOL_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDS_BLOCK_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_READ_INTERVAL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_SEND_DELAY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.MANAGEMENT_LOG;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_LOG_TRANSACTIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * No thread which a BerkeleyJE graph starts outlives its close, whatever background work it did, and an invalid option
 * of those threads fails the open before the instance registers.
 */
public class BerkeleyGraphThreadsTest {

    private static final long WAIT_MS = TimeUnit.SECONDS.toMillis(15);
    //Threads which the JVM keeps for all its code, and which outlive a graph by design: the common pool's, and threads
    //the JVM itself may start at any time
    private static final Predicate<Thread> JVM_WIDE = t -> Stream.of("ForkJoinPool.commonPool-worker-",
            "Attach Listener", "process reaper", "JFR ", "CompletableFutureDelayScheduler", "Common-Cleaner")
        .anyMatch(t.getName()::startsWith);

    @Test
    public void noThreadOfAGraphOutlivesItsClose(@TempDir Path directory) throws Exception {
        final Set<Thread> before = Thread.getAllStackTraces().keySet();
        final ModifiableConfiguration config = BerkeleyStorageSetup.getBerkeleyJEConfiguration(directory.toString())
            .set(SYSTEM_LOG_TRANSACTIONS, true)
            //Blocks of 50 ids, so that the writes below renew the blocks of many pools many times
            .set(IDS_BLOCK_SIZE, 50)
            //Schema changes arrive within a tenth of a second, rather than within the default five seconds
            .set(LOG_READ_INTERVAL, Duration.ofMillis(100), MANAGEMENT_LOG)
            .set(LOG_SEND_DELAY, Duration.ZERO, MANAGEMENT_LOG);
        final JanusGraph graph = JanusGraphFactory.open(config.getConfiguration());
        try {
            //Defined up front: the writers' transactions would contend to define it
            final JanusGraphManagement labels = graph.openManagement();
            labels.makeEdgeLabel("knows").make();
            labels.commit();
            //Vertices with an edge each from several threads at once, which take their ids from the pools of several
            //partitions
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
            final long ackDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
            while (newThreads(before).stream().noneMatch(name -> name.startsWith("ManagementLogger-ack-"))
                && System.nanoTime() - ackDeadline < 0) {
                Thread.sleep(50);
            }
            assertTrue(newThreads(before).stream().anyMatch(name -> name.startsWith("ManagementLogger-ack-")));
            graph.compute().program(new OLAPTest.DegreeCounter()).submit().get().close();
        } finally {
            graph.close();
        }

        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
        List<String> alive = newThreads(before);
        while (!alive.isEmpty() && System.nanoTime() - deadline < 0) {
            Thread.sleep(50);
            alive = newThreads(before);
        }
        assertEquals(new ArrayList<>(), alive);
    }

    @Test
    public void aGraphWhichFailsToOpenOnAnInvalidThreadOptionLeavesNoInstanceOpen(@TempDir Path directory) {
        final ModifiableConfiguration config = BerkeleyStorageSetup.getBerkeleyJEConfiguration(directory.toString());
        //Set unchecked, as in a properties file: the graph checks the value when it reads it
        final WriteConfiguration invalid = config.getConfiguration().copy();
        invalid.set(ConfigElement.getPath(COMPUTER_JOB_POOL_SIZE), 0);
        assertThrows(IllegalArgumentException.class, () -> JanusGraphFactory.open(invalid));

        final JanusGraph graph = JanusGraphFactory.open(config.getConfiguration());
        try {
            final JanusGraphManagement mgmt = graph.openManagement();
            assertEquals(1, mgmt.getOpenInstances().size(), "open instances: " + mgmt.getOpenInstances());
            mgmt.rollback();
        } finally {
            graph.close();
        }
    }

    private static List<String> newThreads(Set<Thread> before) {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(t -> !before.contains(t) && !JVM_WIDE.test(t))
            .map(Thread::getName)
            .collect(Collectors.toList());
    }
}
