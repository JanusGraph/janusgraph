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

package org.janusgraph.diskstorage.util;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An operation which fails temporarily is reattempted after a wait of 25 or 50 ms, then after waits which each are
 * once or twice the one before, as long as the next wait ends within the time given. Any other failure ends it at once.
 */
public class BackendOperationTest {

    private static final Duration TEN_SECONDS = Duration.ofSeconds(10);

    private static long millisSince(long nanoTime) {
        return Duration.ofNanos(System.nanoTime() - nanoTime).toMillis();
    }

    @Test
    public void shouldCallAnOperationWhichSucceedsOnce() throws BackendException {
        final AtomicInteger attempts = new AtomicInteger();

        assertEquals("done", BackendOperation.executeDirect(() -> {
            attempts.incrementAndGet();
            return "done";
        }, TEN_SECONDS));
        assertEquals(1, attempts.get());
    }

    @Test
    public void shouldReattemptATemporaryFailureAfterAWait() throws BackendException {
        final AtomicInteger attempts = new AtomicInteger();
        final long start = System.nanoTime();

        assertEquals("done", BackendOperation.executeDirect(() -> {
            if (attempts.incrementAndGet() < 3) {
                throw new TemporaryBackendException("not yet");
            }
            return "done";
        }, TEN_SECONDS));
        assertEquals(3, attempts.get());
        //Two waits, of at least 25 ms each, less some slack for coarse timers
        assertTrue(millisSince(start) >= 40, "waited " + millisSince(start) + " ms");
    }

    //The innermost backend failure decides
    @Test
    public void shouldReattemptAFailureCausedByATemporaryOne() throws BackendException {
        final AtomicInteger attempts = new AtomicInteger();

        assertEquals("done", BackendOperation.executeDirect(() -> {
            if (attempts.incrementAndGet() < 2) {
                throw new IllegalStateException(new TemporaryBackendException("not yet"));
            }
            return "done";
        }, TEN_SECONDS));
        assertEquals(2, attempts.get());
    }

    @Test
    public void shouldGiveUpWhenTheNextWaitWouldEndPastTheTimeGiven() {
        final AtomicInteger attempts = new AtomicInteger();
        final TemporaryBackendException failure = new TemporaryBackendException("still failing");
        final long start = System.nanoTime();

        final TemporaryBackendException thrown = assertThrows(TemporaryBackendException.class,
            () -> BackendOperation.executeDirect(() -> {
                attempts.incrementAndGet();
                throw failure;
            }, Duration.ofMillis(500)));
        assertSame(failure, thrown.getCause());
        //The first wait, of at most 50 ms, fits
        assertTrue(attempts.get() > 1, attempts.get() + " attempts");
        //No wait ends past the time given, which leaves only the slack of a busy machine
        assertTrue(millisSince(start) < 2_000, "gave up after " + millisSince(start) + " ms");
    }

    @Test
    public void shouldNotReattemptAPermanentFailure() {
        final AtomicInteger attempts = new AtomicInteger();
        final PermanentBackendException failure = new PermanentBackendException("broken");

        assertSame(failure, assertThrows(PermanentBackendException.class, () -> BackendOperation.executeDirect(() -> {
            attempts.incrementAndGet();
            throw failure;
        }, TEN_SECONDS)));
        assertEquals(1, attempts.get());
    }

    @Test
    public void shouldNotReattemptAFailureWhichIsNoBackendFailure() {
        final AtomicInteger attempts = new AtomicInteger();
        final IllegalStateException failure = new IllegalStateException("broken");

        final PermanentBackendException thrown = assertThrows(PermanentBackendException.class,
            () -> BackendOperation.executeDirect(() -> {
                attempts.incrementAndGet();
                throw failure;
            }, TEN_SECONDS));
        assertSame(failure, thrown.getCause());
        assertEquals(1, attempts.get());
    }

    //The operation interrupts its thread, so the wait which follows ends at once
    @Test
    public void shouldNotWaitOnAnInterruptedThread() {
        final AtomicInteger attempts = new AtomicInteger();
        try {
            final PermanentBackendException thrown = assertThrows(PermanentBackendException.class,
                () -> BackendOperation.executeDirect(() -> {
                    attempts.incrementAndGet();
                    Thread.currentThread().interrupt();
                    throw new TemporaryBackendException("not yet");
                }, TEN_SECONDS));
            assertTrue(thrown.getCause() instanceof InterruptedException, String.valueOf(thrown.getCause()));
            assertEquals(1, attempts.get());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }
}
