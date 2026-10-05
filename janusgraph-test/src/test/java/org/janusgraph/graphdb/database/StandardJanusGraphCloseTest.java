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

package org.janusgraph.graphdb.database;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.jupiter.api.Test;

import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class StandardJanusGraphCloseTest {

    /**
     * Closing a graph rolls back its open transactions, of which another thread, such as the reader of the management
     * log, may close one after the graph has listed it as open
     */
    @Test
    public void closeSkipsATransactionAnotherThreadClosesMeanwhile() throws Exception {
        final StandardJanusGraph graph = (StandardJanusGraph) JanusGraphFactory.open("inmemory");
        final StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
        final CountDownLatch holding = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicReference<Throwable> rollbackFailure = new AtomicReference<>();
        final AtomicReference<Throwable> closeFailure = new AtomicReference<>();

        // another thread holds the monitor of the transaction, as its commit and its rollback do
        final Thread other = new Thread(() -> {
            synchronized (tx) {
                holding.countDown();
                try {
                    release.await();
                    tx.rollback();
                } catch (Throwable e) {
                    rollbackFailure.set(e);
                }
            }
        });
        final Thread closing = new Thread(() -> {
            try {
                graph.close();
            } catch (Throwable e) {
                closeFailure.set(e);
            }
        });
        other.setDaemon(true);
        closing.setDaemon(true);
        try {
            other.start();
            assertTrue(holding.await(10, TimeUnit.SECONDS));
            closing.start();
            // the graph waits for the monitor to roll the transaction back, which the other thread closes first
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!waitsFor(closing, tx)) {
                // a close which ended without waiting, whether it failed or not, is reported at once
                if (!closing.isAlive()) {
                    assertNoFailure("closing the graph", closeFailure);
                    fail("closing the graph ended without waiting for the transaction");
                }
                assertTrue(System.nanoTime() < deadline, "the graph didn't wait for the transaction");
                Thread.sleep(10);
            }
        } finally {
            release.countDown();
            other.join(TimeUnit.SECONDS.toMillis(10));
            closing.join(TimeUnit.SECONDS.toMillis(10));
            if (!closing.isAlive() && graph.isOpen()) {
                graph.close();
            }
        }

        assertFalse(other.isAlive(), "the other thread didn't finish");
        assertFalse(closing.isAlive(), "closing the graph didn't finish");
        assertNoFailure("the other thread's rollback", rollbackFailure);
        assertNoFailure("closing the graph", closeFailure);
        assertFalse(graph.isOpen());
        assertFalse(tx.isOpen());
    }

    private static void assertNoFailure(String what, AtomicReference<Throwable> failure) {
        if (failure.get() != null) {
            throw new AssertionError(what + " failed", failure.get());
        }
    }

    private static boolean waitsFor(Thread thread, Object monitor) {
        final ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(thread.getId());
        final LockInfo lock = info == null ? null : info.getLockInfo();
        // the state and the lock of one snapshot of the thread
        return lock != null && info.getThreadState() == Thread.State.BLOCKED
            && lock.getIdentityHashCode() == System.identityHashCode(monitor);
    }
}
