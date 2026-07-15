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

package org.janusgraph.diskstorage.keycolumnvalue.scan;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.KeyColumnValueStoreUtil;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.inmemory.InMemoryStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that cancelling a <i>running</i> scan promptly unwinds the scan pipeline
 * ({@link MultiThreadsRowsCollector} and its data-puller threads) rather than hanging.
 * <p>
 * The collector hands rows off through bounded blocking queues using {@code take()}. If
 * cancellation did not interrupt the collector while it is blocked, its data-puller threads would
 * remain alive indefinitely. This test loads enough data that the scan is still running when it is
 * cancelled, then asserts that the puller threads terminate within a generous timeout and that the
 * scan did not run to completion.
 */
public class ScanCancellationTest {

    private static final TimestampProvider TIMES = TimestampProviders.MICRO;
    private static final String STORE_NAME = "scancancel";
    /** Large enough that an uninterrupted scan (sleeping per row) takes far longer than the test's timeouts. */
    private static final int NUM_KEYS = 10000;
    private static final long SLEEP_MILLIS_PER_ROW = 5;

    private KeyColumnValueStoreManager manager;
    private KeyColumnValueStore store;

    @BeforeEach
    public void setUp() throws BackendException {
        manager = new InMemoryStoreManager();
        store = manager.openDatabase(STORE_NAME);
        final StoreTransaction tx = manager.beginTransaction(
            StandardBaseTransactionConfig.of(TIMES, manager.getFeatures().getKeyConsistentTxConfig()));
        for (int i = 0; i < NUM_KEYS; i++) {
            KeyColumnValueStoreUtil.insert(store, tx, i, "col", "val");
        }
        tx.commit();
    }

    @AfterEach
    public void tearDown() throws BackendException {
        if (store != null) store.close();
        if (manager != null) manager.close();
    }

    @Test
    public void cancellingRunningScanUnwindsPipelinePromptly() throws Exception {
        final StandardScanner scanner = new StandardScanner(manager);
        final CountDownLatch firstProcess = new CountDownLatch(1);
        final AtomicInteger processed = new AtomicInteger(0);

        final ScanJobFuture future = scanner.build()
            .setStoreName(STORE_NAME)
            .setNumProcessingThreads(2)
            .setWorkBlockSize(100)
            .setTimestampProvider(TIMES)
            .setJob(new SlowGroundingScanJob(SLEEP_MILLIS_PER_ROW, firstProcess, processed))
            .execute();

        // Wait until the scan is actively processing rows so we cancel a genuinely running scan.
        assertTrue(firstProcess.await(20, TimeUnit.SECONDS), "scan never started processing rows");

        assertTrue(future.cancel(true), "cancel(true) should succeed on a still-running scan");
        assertTrue(future.isCancelled(), "future should report cancelled");

        // The collector blocks on take()/put(); cancellation must interrupt it so the pipeline
        // unwinds. A regression (collector blocked forever) would leave data-puller threads alive.
        assertTrue(awaitNoDataPullerThreads(15_000),
            "scan pipeline did not unwind after cancellation - a data-puller thread is still alive");

        // Cancellation must actually curtail work; the scan must not have run to completion.
        assertTrue(processed.get() < NUM_KEYS,
            "scan processed all " + NUM_KEYS + " keys despite being cancelled (processed=" + processed.get() + ")");
    }

    /**
     * Cancels the scan while the scan thread is still inside job setup (before the row collector
     * exists). Historically this threw NullPointerException out of {@code cancel(true)}; with the
     * fix the cancel is a clean no-op on the missing collector, and the scan thread observes the
     * volatile {@code interrupted} flag right after setup, tears the pipeline down and never
     * processes a row nor invokes the finish-job callback.
     */
    @Test
    public void cancelBeforeSetupCompletesMustNotRunTheScan() throws Exception {
        final StandardScanner scanner = new StandardScanner(manager);
        final CountDownLatch setupEntered = new CountDownLatch(1);
        final CountDownLatch cancelIssued = new CountDownLatch(1);
        final CountDownLatch iterationEnded = new CountDownLatch(1);
        final AtomicBoolean finishJobCalled = new AtomicBoolean(false);
        final AtomicInteger processed = new AtomicInteger(0);

        final ScanJobFuture future = scanner.build()
            .setStoreName(STORE_NAME)
            // A single worker keeps the workerIterationEnd signal unambiguous on a regressed path.
            .setNumProcessingThreads(1)
            .setWorkBlockSize(100)
            .setTimestampProvider(TIMES)
            .setFinishJob(m -> finishJobCalled.set(true))
            .setJob(new SetupBlockingScanJob(setupEntered, cancelIssued, iterationEnded, processed))
            .execute();

        assertTrue(setupEntered.await(20, TimeUnit.SECONDS), "scan never entered job setup");

        // The collector does not exist yet; cancel(true) used to NPE out of this call.
        assertTrue(future.cancel(true), "cancel(true) should succeed on a scan still in setup");
        assertTrue(future.isCancelled(), "future should report cancelled");

        cancelIssued.countDown(); // let the scan thread finish setup and observe the cancellation

        assertThrows(CancellationException.class, () -> future.get(20, TimeUnit.SECONDS));
        assertTrue(iterationEnded.await(20, TimeUnit.SECONDS), "scan thread never wound down the job");
        assertTrue(awaitNoDataPullerThreads(15_000),
            "scan pipeline did not unwind after a pre-setup cancellation");

        // Bounded absence-check rather than a single fixed sleep: on the fixed path nothing can
        // invoke finishJob or process rows anymore, while a regressed path (scan running despite
        // the cancel) trips the assertions the moment it does either.
        final long absenceDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
        while (System.nanoTime() - absenceDeadline < 0) {
            assertFalse(finishJobCalled.get(), "finishJob must not run for a cancelled scan");
            assertEquals(0, processed.get(), "a scan cancelled before setup completed must process no rows");
            Thread.sleep(25);
        }
    }

    private static boolean awaitNoDataPullerThreads(long timeoutMillis) throws InterruptedException {
        // nanoTime is monotonic; a wall-clock jump must not shorten or extend the wait.
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() - deadline < 0) {
            if (!anyDataPullerThreadAlive()) return true;
            Thread.sleep(25);
        }
        return !anyDataPullerThreadAlive();
    }

    private static boolean anyDataPullerThreadAlive() {
        for (final Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.isAlive() && t.getName() != null && t.getName().startsWith("data-puller-")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Scan job whose setup ({@code workerIterationStart}) blocks until the test has issued the
     * cancellation, guaranteeing the cancel lands while the row collector does not exist yet.
     */
    private static final class SetupBlockingScanJob implements ScanJob {

        private final CountDownLatch setupEntered;
        private final CountDownLatch cancelIssued;
        private final CountDownLatch iterationEnded;
        private final AtomicInteger processed;

        private SetupBlockingScanJob(CountDownLatch setupEntered, CountDownLatch cancelIssued,
                                     CountDownLatch iterationEnded, AtomicInteger processed) {
            this.setupEntered = setupEntered;
            this.cancelIssued = cancelIssued;
            this.iterationEnded = iterationEnded;
            this.processed = processed;
        }

        @Override
        public List<SliceQuery> getQueries() {
            return Collections.singletonList(new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128)));
        }

        @Override
        public Predicate<StaticBuffer> getKeyFilter() {
            return k -> true;
        }

        @Override
        public void workerIterationStart(Configuration jobConfig, Configuration graphConfig, ScanMetrics metrics) {
            setupEntered.countDown();
            try {
                cancelIssued.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void workerIterationEnd(ScanMetrics metrics) {
            iterationEnded.countDown();
        }

        @Override
        public void process(StaticBuffer key, Map<SliceQuery, EntryList> entries, ScanMetrics metrics) {
            processed.incrementAndGet();
        }

        @Override
        public ScanJob clone() {
            return new SetupBlockingScanJob(setupEntered, cancelIssued, iterationEnded, processed);
        }
    }

    /**
     * Grounding scan job over the full key range that sleeps on every processed row, keeping the
     * scan running long enough to be cancelled mid-flight.
     */
    private static final class SlowGroundingScanJob implements ScanJob {

        private final long sleepMillisPerRow;
        private final CountDownLatch firstProcess;
        private final AtomicInteger processed;

        private SlowGroundingScanJob(long sleepMillisPerRow, CountDownLatch firstProcess, AtomicInteger processed) {
            this.sleepMillisPerRow = sleepMillisPerRow;
            this.firstProcess = firstProcess;
            this.processed = processed;
        }

        @Override
        public List<SliceQuery> getQueries() {
            return Collections.singletonList(new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128)));
        }

        @Override
        public Predicate<StaticBuffer> getKeyFilter() {
            return k -> true;
        }

        @Override
        public void workerIterationStart(Configuration jobConfig, Configuration graphConfig, ScanMetrics metrics) {
            // no-op
        }

        @Override
        public void workerIterationEnd(ScanMetrics metrics) {
            // no-op
        }

        @Override
        public void process(StaticBuffer key, Map<SliceQuery, EntryList> entries, ScanMetrics metrics) {
            firstProcess.countDown();
            processed.incrementAndGet();
            try {
                Thread.sleep(sleepMillisPerRow);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public ScanJob clone() {
            return new SlowGroundingScanJob(sleepMillisPerRow, firstProcess, processed);
        }
    }
}
