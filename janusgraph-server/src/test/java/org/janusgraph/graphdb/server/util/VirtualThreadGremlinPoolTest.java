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

package org.janusgraph.graphdb.server.util;

import org.apache.tinkerpop.gremlin.server.Settings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.janusgraph.graphdb.server.util.VirtualThreadsTest.isVirtual;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class VirtualThreadGremlinPoolTest {

    /**
     * Virtual threads on Java 24 and later, platform threads before, so that the bounds of the pool are tested on
     * every Java version.
     */
    private static ThreadFactory threadFactory() {
        return VirtualThreads.isSupported()
            ? VirtualThreads.newThreadFactory("pool-test-", 1)
            : Executors.defaultThreadFactory();
    }

    @Test
    public void testRunsPoolSizeRequestsAtOnceAndQueuesMaxWorkQueueSizeMore() throws Exception {
        ThreadPoolExecutor pool = VirtualThreadGremlinPool.create(2, 3, threadFactory());
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(2);
        AtomicInteger running = new AtomicInteger();
        AtomicInteger mostRunning = new AtomicInteger();
        AtomicInteger done = new AtomicInteger();
        Runnable request = () -> {
            mostRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            running.decrementAndGet();
            done.incrementAndGet();
        };
        try {
            for (int i = 0; i < 5; i++) {
                pool.execute(request);
            }
            assertTrue(started.await(10, TimeUnit.SECONDS));
            assertEquals(2, pool.getActiveCount());
            assertEquals(3, pool.getQueue().size());

            // Gremlin Server answers a request it can't submit with TOO_MANY_REQUESTS
            assertThrows(RejectedExecutionException.class, () -> pool.execute(request));
            assertThrows(RejectedExecutionException.class, () -> pool.submit(request));

            release.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            assertEquals(5, done.get());
            assertEquals(2, mostRunning.get());
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    public void testShutdownRunsWaitingRequestsAndRejectsNewOnes() throws Exception {
        ThreadPoolExecutor pool = VirtualThreadGremlinPool.create(1, 2, threadFactory());
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        AtomicInteger done = new AtomicInteger();
        try {
            pool.execute(() -> {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                done.incrementAndGet();
            });
            assertTrue(started.await(10, TimeUnit.SECONDS));
            pool.execute(done::incrementAndGet);
            pool.execute(done::incrementAndGet);

            // Gremlin Server stops its pool with shutdown() and awaitTermination()
            pool.shutdown();
            assertThrows(RejectedExecutionException.class, () -> pool.execute(done::incrementAndGet));
            release.countDown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            assertEquals(3, done.get());
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    public void testShutdownNowInterruptsRunningRequestsAndReturnsWaitingOnes() throws Exception {
        ThreadPoolExecutor pool = VirtualThreadGremlinPool.create(1, 2, threadFactory());
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        try {
            pool.execute(() -> {
                started.countDown();
                try {
                    Thread.sleep(TimeUnit.MINUTES.toMillis(1));
                } catch (InterruptedException e) {
                    interrupted.countDown();
                }
            });
            assertTrue(started.await(10, TimeUnit.SECONDS));
            Runnable waiting = () -> {};
            pool.execute(waiting);

            List<Runnable> notRun = pool.shutdownNow();
            assertEquals(1, notRun.size());
            assertSame(waiting, notRun.get(0));
            assertTrue(interrupted.await(10, TimeUnit.SECONDS));
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void testIdleThreadsEnd() {
        ThreadPoolExecutor pool = VirtualThreadGremlinPool.create(4, 8, threadFactory());
        try {
            assertEquals(4, pool.getCorePoolSize());
            assertEquals(4, pool.getMaximumPoolSize());
            assertEquals(8, pool.getQueue().remainingCapacity());
            assertTrue(pool.allowsCoreThreadTimeOut());
            assertEquals(VirtualThreadGremlinPool.KEEP_ALIVE_MILLIS, pool.getKeepAliveTime(TimeUnit.MILLISECONDS));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void testRejectsInvalidSizes() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> VirtualThreadGremlinPool.create(0, 1, threadFactory()));
        assertEquals("gremlinPool must be positive, or 0 for the number of processors: 0", e.getMessage());
        e = assertThrows(IllegalArgumentException.class, () -> VirtualThreadGremlinPool.create(1, 0, threadFactory()));
        assertEquals("maxWorkQueueSize must be positive: 0", e.getMessage());
    }

    @Test
    @EnabledForJreRange(minVersion = VirtualThreads.MIN_JAVA_VERSION)
    public void testCreatesPoolOfVirtualThreadsFromSettings() throws Exception {
        Settings settings = new Settings();
        ThreadPoolExecutor pool = VirtualThreadGremlinPool.create(settings);
        try {
            assertEquals(Runtime.getRuntime().availableProcessors(), pool.getCorePoolSize());
            assertEquals(Runtime.getRuntime().availableProcessors(), pool.getMaximumPoolSize());
            assertEquals(settings.maxWorkQueueSize, pool.getQueue().remainingCapacity());

            Future<Thread> thread = pool.submit(Thread::currentThread);
            assertTrue(isVirtual(thread.get(10, TimeUnit.SECONDS)));
            assertEquals("gremlin-server-exec-1", thread.get().getName());
        } finally {
            pool.shutdownNow();
        }

        settings.gremlinPool = 7;
        settings.maxWorkQueueSize = 11;
        pool = VirtualThreadGremlinPool.create(settings);
        try {
            assertEquals(7, pool.getCorePoolSize());
            assertEquals(7, pool.getMaximumPoolSize());
            assertEquals(11, pool.getQueue().remainingCapacity());
        } finally {
            pool.shutdownNow();
        }

        settings.gremlinPool = -1;
        assertThrows(IllegalArgumentException.class, () -> VirtualThreadGremlinPool.create(settings));
    }

    @Test
    @EnabledForJreRange(maxVersion = VirtualThreads.MIN_JAVA_VERSION - 1)
    public void testRefusedBeforeJava24() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> VirtualThreadGremlinPool.create(new Settings()));
        assertTrue(e.getMessage().startsWith("gremlinPoolVirtualThreads requires Java 24 or later, but JanusGraph "
            + "Server runs on Java "), e.getMessage());
    }
}
