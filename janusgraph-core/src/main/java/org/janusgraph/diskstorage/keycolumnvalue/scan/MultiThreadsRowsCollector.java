// Copyright 2017 JanusGraph Authors
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
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVSUtil;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;

import static org.janusgraph.diskstorage.keycolumnvalue.scan.StandardScannerExecutor.Row;

/**
 * Uses separate thread per query. May be used for {@link KeyColumnValueStore}
 * that preserves keys order while running parallel scans (f.e. Cassandra)
 *
 * @author Sergii Karpenko (sergiy.karpenko@gmail.com)
 */

class MultiThreadsRowsCollector extends RowsCollector {

    private static final int MAX_KEY_LENGTH = 128; //in bytes

    /**
     * Sentinel pushed onto a data puller's queue when that puller has produced all of its rows.
     * It lets the collector block on {@link BlockingQueue#take()} (waking the instant a row is
     * handed off) instead of polling on a timer, while still learning when a query is exhausted.
     */
    private static final SliceResult END_OF_DATA = new SliceResult(null, null, null);

    private static final Logger log = LoggerFactory.getLogger(MultiThreadsRowsCollector.class);

    /** Creates the backend iterator a {@link DataPuller} drains for one {@link SliceQuery}. */
    @FunctionalInterface
    interface KeyIteratorSupplier {
        KeyIterator create(SliceQuery query) throws BackendException;
    }

    private final StoreFeatures storeFeatures;
    private final StoreTransaction storeTx;
    private final List<SliceQuery> queries;
    private final Predicate<StaticBuffer> keyFilter;
    private final List<StaticBuffer> keysToScan;
    private final Configuration graphConfiguration;
    private final KeyIteratorSupplier keyIteratorSupplier;
    private final String threadNameSuffix;
    private final DataPuller[] pullThreads;
    private final BlockingQueue<SliceResult>[] dataQueues;
    private volatile boolean interrupted = false;
    /** The thread executing {@link #run()}; interrupted by {@link #interrupt()} to unblock {@code take()}. */
    private volatile Thread collectorThread;
    /** Per-query flag set once a query's {@link #END_OF_DATA} sentinel has been observed. */
    private boolean[] finishedQueries;

    MultiThreadsRowsCollector(
        KeyColumnValueStore store,
        StoreFeatures storeFeatures,
        StoreTransaction storeTx,
        List<SliceQuery> queries,
        Predicate<StaticBuffer> keyFilter,
        List<StaticBuffer> keysToScan,
        BlockingQueue<Row> rowQueue,
        Configuration graphConfiguration) throws BackendException {
        this(store, storeFeatures, storeTx, queries, keyFilter, keysToScan, rowQueue, graphConfiguration, null, "");
    }

    /**
     * @param keyIteratorSupplier iterator source for each query; when null the store's regular
     *                            whole-key-space scan is used (or the keysToScan subset when given)
     * @param threadNameSuffix    appended to data-puller thread names so concurrent collectors
     *                            (one per scan split) stay distinguishable in thread dumps
     */
    MultiThreadsRowsCollector(
        KeyColumnValueStore store,
        StoreFeatures storeFeatures,
        StoreTransaction storeTx,
        List<SliceQuery> queries,
        Predicate<StaticBuffer> keyFilter,
        List<StaticBuffer> keysToScan,
        BlockingQueue<Row> rowQueue,
        Configuration graphConfiguration,
        KeyIteratorSupplier keyIteratorSupplier,
        String threadNameSuffix) throws BackendException {

        super(store, rowQueue);
        this.storeFeatures = storeFeatures;
        this.storeTx = storeTx;
        this.queries = queries;
        this.keyFilter = keyFilter;
        this.keysToScan = keysToScan;
        this.graphConfiguration = graphConfiguration;
        this.keyIteratorSupplier = keyIteratorSupplier;
        this.threadNameSuffix = threadNameSuffix;

        this.dataQueues = new BlockingQueue[queries.size()];
        this.pullThreads = new DataPuller[queries.size()];

        setUp(queries);
    }

    /**
     * Creates every query's hand-off queue and backend iterator BEFORE starting any puller thread.
     * With this start-last ordering a creation failure for a later query needs no thread teardown -
     * the half-built collector is never assigned anywhere cleanup could reach it, and a puller that
     * had already started would keep pulling until it blocks on its full queue, stoppable only by
     * interruption (which not every store supports). The already-created iterators are just closed.
     */
    private void setUp(List<SliceQuery> queries) throws BackendException {
        try {
            int pos = 0;
            for (SliceQuery sliceQuery : queries) {
                createDataPuller(sliceQuery, storeTx, pos);
                pos++;
            }
        } catch (Throwable t) {
            for (DataPuller created : pullThreads) {
                if (created != null) {
                    closeIteratorQuietly(created.keyIterator);
                }
            }
            throw t;
        }
        int started = 0;
        try {
            for (DataPuller pullThread : pullThreads) {
                pullThread.start();
                started++;
            }
        } catch (Throwable t) {
            cleanup(); // best effort: interrupt the pullers that did start
            for (int i = started; i < pullThreads.length; i++) {
                closeIteratorQuietly(pullThreads[i].keyIterator);
            }
            throw t;
        }
    }

    private static void closeIteratorQuietly(KeyIterator keyIterator) {
        try {
            keyIterator.close();
        } catch (Exception e) {
            log.warn("Could not close storage iterator while unwinding a failed scan setup", e);
        }
    }

    void run() throws InterruptedException, TemporaryBackendException {
        collectorThread = Thread.currentThread();
        int numQueries = queries.size();
        SliceResult[] currentResults = new SliceResult[numQueries];
        finishedQueries = new boolean[numQueries];
        try {
            while (!interrupted) {
                collectDataFromPullers(currentResults, numQueries);

                SliceResult conditionQuery = currentResults[0];
                if (conditionQuery==null) break; //Termination condition - primary query has no more data
                final StaticBuffer key = conditionQuery.key;

                Row e = buildRow(numQueries, currentResults, key);

                putRow(e);
            }
        } catch (InterruptedException e) {
            // interrupt() was invoked (scan cancellation) and unblocked our take()/put(). The thrown
            // InterruptedException already cleared this thread's interrupt status, so downstream
            // join()/cleanup() proceed normally; StandardScannerExecutor observes the `interrupted`
            // flag and completes the scan as interrupted.
            interrupted = true;
        }
        if (!interrupted) {
            throwIfAnyPullerFailed();
        }
    }

    /**
     * Zero-delay backstop for pullers whose sentinel the merge loop never consumed: a query can end
     * the scan with a pending, never-matching row (e.g. a key written between the grounding query's
     * pass and this query's) that permanently occupies its slot, so its later sentinel is never
     * taken. The rows already merged are complete either way, but a failure recorded by such a
     * puller still marks the scan as incomplete-by-error. Deliberately does NOT join or wait: a
     * still-running straggler puller is normal here (see above) and is interrupted by
     * {@link #join()}/{@link #cleanup()}.
     */
    private void throwIfAnyPullerFailed() throws TemporaryBackendException {
        for (int i = 0; i < pullThreads.length; i++) {
            throwIfPullerFailed(i);
        }
    }

    private void throwIfPullerFailed(int queryIndex) throws TemporaryBackendException {
        final Throwable failure = pullThreads[queryIndex].failure;
        if (failure != null) {
            throw new TemporaryBackendException(
                "Data puller [" + queryIndex + threadNameSuffix + "] failed; failing the scan because its result would be incomplete", failure);
        }
    }

    /**
     * Blocks on each unfinished query's queue until the next row (or its {@link #END_OF_DATA}
     * sentinel) arrives. Unlike a timed poll, this adds no per-row latency: the collector wakes the
     * instant a data puller hands off a row. A query whose sentinel has been seen is skipped.
     * <p>
     * A puller that dies on a storage error still enqueues its END_OF_DATA sentinel, which is
     * indistinguishable from legitimate query exhaustion - so the puller's failure is checked the
     * moment its sentinel is observed. Without that check the scan would stream the entire remaining
     * key space only to be discarded at the end, or worse, complete "successfully" with silently
     * missing rows (for a reindex: an incomplete index getting ENABLED). {@link DataPuller#failure}
     * is written before the sentinel is enqueued, so observing the sentinel guarantees the failure
     * is visible.
     */
    private void collectDataFromPullers(SliceResult[] currentResults, int numQueries)
            throws InterruptedException, TemporaryBackendException {
        for (int i = 0; i < numQueries; i++) {
            if (currentResults[i]!=null || finishedQueries[i]) continue;

            SliceResult qr = dataQueues[i].take(); //Blocks until a row or the END_OF_DATA sentinel is available
            if (qr == END_OF_DATA) {
                throwIfPullerFailed(i);
                finishedQueries[i] = true; //No more data for this query; leave currentResults[i] null
            } else {
                currentResults[i] = qr;
            }
        }
    }

    private Row buildRow(int numQueries, SliceResult[] currentResults, StaticBuffer key) {
        Map<SliceQuery,EntryList> queryResults = new HashMap<>(numQueries);
        for (int i = 0; i< currentResults.length; i++) {
            SliceQuery query = queries.get(i);
            EntryList entries = EntryList.EMPTY_LIST;
            if (currentResults[i]!=null && currentResults[i].key.equals(key)) {
                assert query.equals(currentResults[i].query);
                entries = currentResults[i].entries;
                currentResults[i]=null;
            }
            queryResults.put(query,entries);
        }
        return new Row(key, queryResults);
    }

    @Override
    void join() throws InterruptedException {
        int i = 0;
        for (DataPuller dataPuller : pullThreads) {
            dataPuller.join(10);
            if (dataPuller.isAlive()) {
                log.warn("Data pulling thread [{}] did not terminate. Forcing termination",i);
                if (storeFeatures.supportsInterruption()) {
                    dataPuller.interrupt();
                } else {
                    log.warn("Store does not support interruption, so data pulling thread [{}] cannot be interrupted", i);
                    dataPuller.finished = true;
                }
            }
            i++;
        }
    }

    @Override
    void interrupt() {
        interrupted = true;
        // The collector blocks on take()/put(); interrupt its thread so the cancellation is observed
        // immediately rather than after the next row arrives.
        final Thread t = collectorThread;
        if (t != null) {
            t.interrupt();
        }
    }

    @Override
    void cleanup() {
        if (pullThreads!=null) {
            for (DataPuller pullThread : pullThreads) {
                if (pullThread != null && pullThread.isAlive()) {
                    if (storeFeatures.supportsInterruption()) {
                        pullThread.interrupt();
                    } else {
                        log.warn("Store does not support interruption, so data pulling thread cannot be interrupted");
                        pullThread.finished = true;
                    }
                }
            }
        }
    }

    private void createDataPuller(SliceQuery sq, StoreTransaction stx, int pos) throws BackendException {
        final BlockingQueue<SliceResult> queue = new LinkedBlockingQueue<>(
            this.graphConfiguration.get(GraphDatabaseConfiguration.PAGE_SIZE));
        dataQueues[pos] = queue;

        KeyIterator keyIterator;
        if (keysToScan != null) {
            keyIterator = store.getKeys(keysToScan, sq, stx);
        } else if (keyIteratorSupplier != null) {
            keyIterator = keyIteratorSupplier.create(sq);
        } else {
            keyIterator = KCVSUtil.getKeys(store, sq, storeFeatures, MAX_KEY_LENGTH, stx);
        }

        DataPuller dp = new DataPuller(sq, queue, keyIterator, keyFilter);
        pullThreads[pos] = dp;
        dp.setName("data-puller-" + pos + threadNameSuffix); // setting the name for thread dumps!
    }

    /** Per-puller counters for scan progress logging: rows handed off and time spent blocked on hand-off. */
    @Override
    String getPullersProgress() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pullThreads.length; i++) {
            final DataPuller puller = pullThreads[i];
            if (i > 0) sb.append(", ");
            sb.append(i).append(threadNameSuffix)
                .append(": rows=").append(puller.rowsPulled)
                .append(" handOffBlockedMs=").append(puller.blockedNanos / 1_000_000)
                .append(puller.finished ? " (finished)" : "");
        }
        return sb.toString();
    }

    private static class DataPuller extends Thread {

        private final BlockingQueue<SliceResult> queue;
        private final KeyIterator keyIterator;
        private final SliceQuery query;
        private final Predicate<StaticBuffer> keyFilter;
        private volatile boolean finished;
        /** Rows handed to the collector queue; written only by this thread. */
        private volatile long rowsPulled;
        /** Nanos spent blocked handing rows to a full collector queue; written only by this thread. */
        private volatile long blockedNanos;
        /** Non-null when the puller died on a storage error; checked when its END_OF_DATA sentinel is observed. */
        private volatile Throwable failure;

        private DataPuller(SliceQuery query, BlockingQueue<SliceResult> queue,
                           KeyIterator keyIterator, Predicate<StaticBuffer> keyFilter) {
            this.query = query;
            this.queue = queue;
            this.keyIterator = keyIterator;
            this.keyFilter = keyFilter;
            this.finished = false;
        }

        @Override
        public void run() {
            boolean interruptedWhilePulling = false;
            try {
                while (keyIterator.hasNext()) {
                    StaticBuffer key = keyIterator.next();
                    // NOTE: the per-key entry iterator is deliberately NOT closed. In backends like
                    // inmemory (InMemoryKeyColumnValueStore.RowIterator) closing it closes the whole
                    // key iterator; draining it (EntryArrayList.of) releases per-key state, and the
                    // key iterator itself is closed in the finally block below.
                    RecordIterator<Entry> entries = keyIterator.getEntries();
                    if (!keyFilter.test(key)) continue;
                    EntryList entryList = EntryArrayList.of(entries);
                    final SliceResult result = new SliceResult(query, key, entryList);
                    // Hot path stays timing-free: only measure when the queue is full and we must block.
                    if (!queue.offer(result)) {
                        final long blockStart = System.nanoTime();
                        queue.put(result);
                        blockedNanos += System.nanoTime() - blockStart;
                    }
                    rowsPulled++;
                }
            } catch (InterruptedException e) {
                interruptedWhilePulling = true;
                log.error("Data-pulling thread interrupted while waiting on queue or data", e);
            } catch (Throwable e) {
                failure = e;
                log.error("Could not load data from storage", e);
            } finally {
                try {
                    keyIterator.close();
                } catch (IOException e) {
                    log.warn("Could not close storage iterator ", e);
                }
                finished=true;
                // On normal completion signal the collector that this query is exhausted so its
                // blocking take() wakes up. The collector is actively draining, so this completes as
                // soon as a slot frees. We must NOT attempt this when interrupted: cancellation stops
                // the collector, so the queue may be full and never drained again - a blocking put
                // here would hang this thread until cleanup() forcibly interrupts it. A missed
                // sentinel is harmless because the scan is being cancelled.
                if (!interruptedWhilePulling && !Thread.currentThread().isInterrupted()) {
                    try {
                        queue.put(END_OF_DATA);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        public boolean isFinished() {
            return finished;
        }
    }

    private static class SliceResult {

        final SliceQuery query;
        final StaticBuffer key;
        final EntryList entries;

        private SliceResult(SliceQuery query, StaticBuffer key, EntryList entries) {
            this.query = query;
            this.key = key;
            this.entries = entries;
        }
    }

}
