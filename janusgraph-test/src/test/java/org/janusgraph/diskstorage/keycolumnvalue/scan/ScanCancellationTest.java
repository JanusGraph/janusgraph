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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

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

    private static boolean awaitNoDataPullerThreads(long timeoutMillis) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
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
