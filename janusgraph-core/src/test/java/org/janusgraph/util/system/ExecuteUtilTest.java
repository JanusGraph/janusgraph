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

package org.janusgraph.util.system;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.janusgraph.util.system.ExecuteUtil.gracefulExecutorServiceShutdown;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExecuteUtilTest {

    private static final long LONG_WAIT_MS = TimeUnit.MINUTES.toMillis(1);

    //Initializes ExecuteUtil, and with it its logger, before a test interrupts its thread: logging which first starts
    //on an interrupted thread fails to initialize
    @BeforeAll
    public static void initializeExecuteUtil() {
        gracefulExecutorServiceShutdown(null, 0);
    }

    /**
     * Submits a task which waits until it is interrupted, and returns once it runs.
     */
    private static void startTaskWaitingForInterrupt(ExecutorService executor, AtomicBoolean interrupted)
        throws InterruptedException {
        final CountDownLatch started = new CountDownLatch(1);
        executor.execute(() -> {
            started.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        });
        assertTrue(started.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void waitsForTheTasksToFinish() throws InterruptedException {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final CountDownLatch finished = new CountDownLatch(1);
        executor.execute(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                return;
            }
            finished.countDown();
        });

        gracefulExecutorServiceShutdown(executor, LONG_WAIT_MS);

        assertTrue(executor.isTerminated());
        assertTrue(finished.await(0, TimeUnit.MILLISECONDS), "the task was interrupted");
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    public void interruptsTheTasksWhichOutlastTheWait() throws InterruptedException {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicBoolean taskInterrupted = new AtomicBoolean();
        startTaskWaitingForInterrupt(executor, taskInterrupted);

        gracefulExecutorServiceShutdown(executor, 100);

        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        assertTrue(taskInterrupted.get());
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    public void restoresAnInterruptWhichEndsTheWait() throws InterruptedException {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicBoolean taskInterrupted = new AtomicBoolean();
        startTaskWaitingForInterrupt(executor, taskInterrupted);
        final Thread waiting = Thread.currentThread();
        final Thread interrupter = new Thread(() -> {
            //Interrupts the test thread once it waits for the executor to terminate, or after 10 seconds at the latest,
            //so that the test fails rather than hangs should the wait never show
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (waiting.getState() != Thread.State.TIMED_WAITING && System.nanoTime() - deadline < 0) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
            waiting.interrupt();
        });
        interrupter.start();

        final long start = System.nanoTime();
        try {
            gracefulExecutorServiceShutdown(executor, LONG_WAIT_MS);
            assertTrue(Thread.currentThread().isInterrupted(), "the interrupt was not restored");
        } finally {
            Thread.interrupted();
            interrupter.join();
        }
        assertTrue(System.nanoTime() - start < TimeUnit.MILLISECONDS.toNanos(LONG_WAIT_MS),
            "the interrupt did not end the wait");
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        assertTrue(taskInterrupted.get(), "the tasks of an interrupted wait were not interrupted");
    }

    @Test
    public void keepsAnInterruptWhichPrecedesTheWait() throws InterruptedException {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicBoolean taskInterrupted = new AtomicBoolean();
        startTaskWaitingForInterrupt(executor, taskInterrupted);

        Thread.currentThread().interrupt();
        try {
            gracefulExecutorServiceShutdown(executor, LONG_WAIT_MS);
            assertTrue(Thread.currentThread().isInterrupted(), "the interrupt was swallowed");
        } finally {
            Thread.interrupted();
        }
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        assertTrue(taskInterrupted.get());
    }
}
