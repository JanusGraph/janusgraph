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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BackgroundThreadTest {

    /**
     * Waits for nothing and does nothing, so that close() lands at every point of the loop, and records whether its
     * action or its cleanup ever ran with the thread interrupted.
     */
    private static class BusyThread extends BackgroundThread {
        final AtomicBoolean interruptedInAction = new AtomicBoolean();
        volatile Boolean interruptedInCleanup;

        BusyThread() {
            super("busy", false);
        }

        @Override
        protected void waitCondition() {
        }

        @Override
        protected void action() {
            if (isInterrupted()) interruptedInAction.set(true);
        }

        @Override
        protected void cleanup() {
            interruptedInCleanup = isInterrupted();
        }
    }

    /**
     * The action and the cleanup may run blocking operations which an interrupt breaks - the log's send thread flushes
     * its messages through the storage backend, and BerkeleyJE invalidates its whole environment when a thread is
     * interrupted in the middle of a file operation - so close() must never interrupt them.
     */
    @Test
    public void closeInterruptsNeitherTheActionNorTheCleanup() {
        //Each thread lives only until the close() which follows its start(), so 5000 of them take well under a second,
        //and caught the race on master in most runs
        for (int i = 0; i < 5000; i++) {
            final BusyThread thread = new BusyThread();
            thread.start();
            thread.close(Duration.ofSeconds(10));
            assertFalse(thread.isAlive());
            assertFalse(thread.interruptedInAction.get(), "action ran interrupted, iteration " + i);
            assertEquals(Boolean.FALSE, thread.interruptedInCleanup, "cleanup ran interrupted, iteration " + i);
        }
    }

    /**
     * A thread which has left its loop and is running its cleanup is not interrupted by close(): the loop can end on
     * close()'s soft interruption before close() has sent its interrupt, which then used to land in the cleanup.
     * Leaving the loop through an interrupt of its own, the thread is in its cleanup for certain when close() runs.
     */
    @Test
    public void closeDoesNotInterruptACleanupInProgress() throws InterruptedException {
        final CountDownLatch inCleanup = new CountDownLatch(1);
        final CountDownLatch closeHasRun = new CountDownLatch(1);
        final AtomicBoolean interruptedInCleanup = new AtomicBoolean();
        final BackgroundThread thread = new BackgroundThread("cleaning", false) {
            @Override
            protected void waitCondition() throws InterruptedException {
                Thread.sleep(Long.MAX_VALUE);
            }

            @Override
            protected void action() {
            }

            @Override
            protected void cleanup() {
                inCleanup.countDown();
                //Waits in parks, which an interrupt ends without clearing it, so that one stays pending for the check
                //below, and no longer than 10 s, so that it stops when the test fails before it lets the cleanup go on
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (closeHasRun.getCount() > 0 && System.nanoTime() - deadline < 0) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                interruptedInCleanup.set(isInterrupted());
            }
        };
        thread.start();
        thread.interrupt();
        assertTrue(inCleanup.await(10, TimeUnit.SECONDS));

        final Thread closer = new Thread(() -> thread.close(Duration.ofSeconds(10)));
        closer.start();
        //close() interrupts, if at all, before it waits for the thread to end
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (closer.getState() != Thread.State.TIMED_WAITING && closer.isAlive()) {
            assertTrue(System.nanoTime() - deadline < 0, "close() neither returned nor began to wait for the thread");
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        closeHasRun.countDown();
        closer.join(10000);
        thread.join(10000);
        assertFalse(interruptedInCleanup.get(), "close() interrupted the cleanup");
    }
}
