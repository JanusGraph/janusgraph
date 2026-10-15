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

package org.janusgraph.graphdb.util;

import org.janusgraph.core.JanusGraphException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
public class WorkerPoolTest {

    private static final Duration SHORT = Duration.ofMillis(300);

    @Test
    public void closeWaitsAsLongAsTasksKeepEnding() {
        //Ten tasks of 150 ms on one thread take five times the progress timeout, but one ends within each
        final AtomicInteger ended = new AtomicInteger();
        try (WorkerPool workers = new WorkerPool("test", 1, SHORT)) {
            for (int i = 0; i < 10; i++) {
                workers.submit(() -> {
                    pause(150);
                    ended.incrementAndGet();
                });
            }
        }
        assertEquals(10, ended.get());
    }

    @Test
    public void closeWaitsIndefinitelyWhereTheProgressTimeoutIsTooLongForNanoseconds() {
        final AtomicInteger ended = new AtomicInteger();
        try (WorkerPool workers = new WorkerPool("test", 1, Duration.ofMillis(Long.MAX_VALUE))) {
            for (int i = 0; i < 3; i++) {
                workers.submit(() -> {
                    pause(50);
                    ended.incrementAndGet();
                });
            }
        }
        assertEquals(3, ended.get());
    }

    @Test
    public void closeStopsTasksWhichMakeNoProgressAndFails() {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch never = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean();
        final AtomicInteger ran = new AtomicInteger();
        final WorkerPool workers = new WorkerPool("stuck test", 1, SHORT);
        workers.submit(() -> {
            started.countDown();
            try {
                never.await();
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        });
        for (int i = 0; i < 3; i++) {
            workers.submit(ran::incrementAndGet);
        }
        awaitQuietly(started);

        final JanusGraphException failure = assertThrows(JanusGraphException.class, workers::close);
        assertTrue(failure.getMessage().startsWith("No task of the stuck test ended for PT0.3S"), failure.getMessage());
        assertTrue(failure.getMessage().contains("interrupted the 1 which ran and dropped the 3 which hadn't started"),
            failure.getMessage());
        awaitTrue(interrupted);
        assertEquals(0, ran.get());
    }

    @Test
    public void submitStopsTasksWhichMakeNoProgressWhileItWaitsForRoom() {
        //The thread is stuck, and the queue holds 128 tasks, so the next task waits for room which never comes
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch never = new CountDownLatch(1);
        final WorkerPool workers = new WorkerPool("stuck test", 1, SHORT);
        workers.submit(() -> {
            started.countDown();
            try {
                never.await();
            } catch (InterruptedException e) {
                //Ends
            }
        });
        awaitQuietly(started);
        for (int i = 0; i < 128; i++) {
            workers.submit(() -> { });
        }

        final JanusGraphException failure = assertThrows(JanusGraphException.class, () -> workers.submit(() -> { }));
        assertTrue(failure.getMessage().contains("while it waited for room in its queue"), failure.getMessage());
        //The failure was thrown already
        workers.close();
    }

    @Test
    public void closeThrowsAnExceptionOfItsOwnCausedByTheFirstFailureAndSkipsTheTasksAfterIt() {
        final IllegalStateException first = new IllegalStateException("The task failed");
        final AtomicInteger ran = new AtomicInteger();
        final WorkerPool workers = new WorkerPool("test", 1, Duration.ofMinutes(1));
        workers.submit(() -> {
            throw first;
        });
        for (int i = 0; i < 5; i++) {
            workers.submit(ran::incrementAndGet);
        }

        final JanusGraphException failure = assertThrows(JanusGraphException.class, workers::close);
        assertSame(first, failure.getCause());
        assertEquals(0, failure.getSuppressed().length);
        assertEquals(0, first.getSuppressed().length);
        assertEquals("1 of the 6 tasks of the test failed, and the 5 which hadn't started were skipped",
            failure.getMessage());
        assertEquals(0, ran.get());
    }

    @Test
    public void theFailuresOfTasksAreAttachedOnceEachAndUpToTheCap() {
        //All the tasks start before any fails. The first throws an exception of its own, then two throw an instance
        //which they share, and then the others one of their own each
        final int tasks = 24;
        final RuntimeException first = new IllegalStateException("first");
        final RuntimeException shared = new IllegalStateException("shared");
        final CountDownLatch started = new CountDownLatch(tasks);
        final List<RuntimeException> thrown = Collections.synchronizedList(new ArrayList<>());
        final WorkerPool workers = new WorkerPool("test", tasks, Duration.ofMinutes(1));
        for (int i = 0; i < tasks; i++) {
            final int task = i;
            workers.submit(() -> {
                started.countDown();
                awaitQuietly(started);
                final RuntimeException failure;
                if (task == 0) {
                    failure = first;
                } else if (task < 3) {
                    awaitFailures(workers, 1);
                    failure = shared;
                } else {
                    awaitFailures(workers, 3);
                    failure = new IllegalStateException("task " + task);
                }
                thrown.add(failure);
                throw failure;
            });
        }

        final JanusGraphException failure = assertThrows(JanusGraphException.class, workers::close);
        assertEquals("24 of the 24 tasks of the test failed; 11 of them are attached, each exception once",
            failure.getMessage());
        assertSame(first, failure.getCause());
        final Set<Throwable> attached = Collections.newSetFromMap(new IdentityHashMap<>());
        attached.add(failure.getCause());
        Collections.addAll(attached, failure.getSuppressed());
        assertEquals(WorkerPool.MAX_SUPPRESSED_FAILURES, failure.getSuppressed().length);
        assertEquals(WorkerPool.MAX_SUPPRESSED_FAILURES + 1, attached.size());
        assertTrue(attached.contains(shared));
        for (Throwable each : attached) {
            assertTrue(thrown.stream().anyMatch(t -> t == each));
            assertNotSame(failure, each);
        }
        assertEquals(0, first.getSuppressed().length);
        assertEquals(0, shared.getSuppressed().length);
    }

    @Test
    public void closeStopsWhenItsThreadIsInterruptedAndKeepsTheFailuresUpToTheCap() throws Exception {
        //One task never ends, and more tasks than the cap fail, all of them having started before any fails
        final int failing = WorkerPool.MAX_SUPPRESSED_FAILURES + 2;
        final CountDownLatch started = new CountDownLatch(failing + 1);
        final CountDownLatch never = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean();
        final List<RuntimeException> thrown = Collections.synchronizedList(new ArrayList<>());
        final WorkerPool workers = new WorkerPool("test", failing + 1, Duration.ofMinutes(1));
        workers.submit(() -> {
            started.countDown();
            try {
                never.await();
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        });
        for (int i = 0; i < failing; i++) {
            final int task = i;
            workers.submit(() -> {
                started.countDown();
                awaitQuietly(started);
                final RuntimeException failure = new IllegalStateException("task " + task);
                thrown.add(failure);
                throw failure;
            });
        }
        final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        final AtomicBoolean interruptStatus = new AtomicBoolean();
        final Thread closer = new Thread(() -> {
            try {
                workers.close();
            } catch (Throwable e) {
                closeFailure.set(e);
            }
            interruptStatus.set(Thread.currentThread().isInterrupted());
        });
        awaitFailures(workers, failing);
        assertEquals(failing, workers.failureCount());
        closer.start();
        closer.interrupt();
        closer.join(TimeUnit.SECONDS.toMillis(10));

        assertFalse(closer.isAlive());
        final Throwable failure = closeFailure.get();
        assertTrue(failure instanceof JanusGraphException, String.valueOf(failure));
        assertTrue(failure.getCause() instanceof InterruptedException, String.valueOf(failure));
        assertEquals("The test was interrupted while it waited for its tasks to end: it interrupted the tasks which " +
            "ran and dropped the 0 which hadn't started; 12 of the 13 tasks of the test failed; 10 of them are " +
            "attached, each exception once", failure.getMessage());
        //Where the interrupt is the cause, the failures are all suppressed, up to the cap as well
        final Set<Throwable> attached = Collections.newSetFromMap(new IdentityHashMap<>());
        Collections.addAll(attached, failure.getSuppressed());
        assertEquals(WorkerPool.MAX_SUPPRESSED_FAILURES, failure.getSuppressed().length);
        assertEquals(WorkerPool.MAX_SUPPRESSED_FAILURES, attached.size());
        for (Throwable each : attached) {
            assertTrue(thrown.stream().anyMatch(t -> t == each));
        }
        assertTrue(interruptStatus.get());
        awaitTrue(interrupted);
    }

    //Waits, for 10 s at most, until the pool counted the number of failures, which it recorded by then
    private static void awaitFailures(WorkerPool workers, int failures) {
        final long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (workers.failureCount() < failures && System.nanoTime() < end) {
            pause(1);
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void awaitTrue(AtomicBoolean flag) {
        final long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!flag.get() && System.nanoTime() < end) {
            pause(10);
        }
        assertTrue(flag.get());
    }

    //Pauses for the time whatever interrupts the thread meanwhile, as a task which doesn't heed interrupts does
    private static void pause(long millis) {
        final long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        for (long left = millis; left > 0; left = TimeUnit.NANOSECONDS.toMillis(end - System.nanoTime())) {
            try {
                Thread.sleep(left);
            } catch (InterruptedException e) {
                //Goes on pausing
            }
        }
    }
}
