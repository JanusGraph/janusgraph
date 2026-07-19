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
import java.util.ArrayDeque;
import java.util.Deque;
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
    /**
     * Per-secondary-query buffer of rows taken from the data queue but not yet matched to a
     * grounding key, oldest first; see {@link #secondaryEntries(int, StaticBuffer)}. Index 0
     * (the grounding query) is unused - its rows are consumed one per merge iteration. On a
     * key-ordered scan the buffer holds at most one row (the merge-join look-ahead).
     */
    private Deque<SliceResult>[] pendingRows;

    /**
     * Whether this scan iterates keys in their natural {@link StaticBuffer} order for every query,
     * which lets the merge classify any row against the current grounding key by comparison. Token-
     * ordered scans (e.g. CQL/Murmur3) iterate in a backend-internal order the merge cannot compute,
     * so they use the bounded-buffer recovery in {@link #bufferedSecondaryEntries(int, StaticBuffer)}
     * instead. Targeted scans (keysToScan) and supplier-provided iterators (token-range splits) make
     * no natural-order promise either.
     */
    private final boolean scanKeysNaturallyOrdered;

    /**
     * Upper bound of {@link #pendingRows} per query on scans without a computable key order. Large
     * enough that a row whose key was written during the scan (and therefore never appears in the
     * grounding stream) is evicted by a later match long before the buffer fills, small enough that
     * buffered rows stay negligible next to the per-query hand-off queues. When the buffer does fill
     * without a match, the current key yields no entries for that query - the historical single-slot
     * behavior - instead of blocking.
     */
    private static final int MAX_PENDING_ROWS_PER_QUERY = 32;

    /**
     * Upper bound of BLOCKING waits for new secondary rows per grounding key on the buffered path
     * (wait-free polls of already-produced rows are not budgeted). Large enough to unconditionally
     * recover the current key's row behind realistic bursts of stale rows even when rows trickle in
     * one at a time, small enough that a grounding key never stalls the merge for long behind a
     * secondary puller that must scan far between rows.
     */
    private static final int MAX_BLOCKING_TAKES_PER_KEY = 8;

    /** Preceding grounding key of a natural-order multi-query scan; see {@link #verifyNaturalKeyOrder}. */
    private StaticBuffer previousGroundingKey;

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
        this.scanKeysNaturallyOrdered =
            storeFeatures.isKeyOrdered() && keysToScan == null && keyIteratorSupplier == null;

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
        final int numQueries = queries.size();
        finishedQueries = new boolean[numQueries];
        @SuppressWarnings("unchecked")
        final Deque<SliceResult>[] buffers = new ArrayDeque[numQueries];
        pendingRows = buffers;
        for (int i = 1; i < numQueries; i++) {
            pendingRows[i] = new ArrayDeque<>();
        }
        try {
            while (!interrupted) {
                final SliceResult primary = nextPrimaryRow();
                if (primary == null) break; //Termination condition - primary query has no more data
                final StaticBuffer key = primary.key;
                if (scanKeysNaturallyOrdered && numQueries > 1) {
                    verifyNaturalKeyOrder(key);
                }

                final Map<SliceQuery, EntryList> queryResults = new HashMap<>(numQueries);
                assert queries.get(0).equals(primary.query);
                queryResults.put(queries.get(0), primary.entries);
                for (int i = 1; i < numQueries; i++) {
                    queryResults.put(queries.get(i), secondaryEntries(i, key));
                }
                putRow(new Row(key, queryResults));
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
     * Next row of the grounding (primary) query, or null once it is exhausted. The grounding query
     * matches at least one cell of every key, so its stream defines the key set of the scan.
     * <p>
     * A puller that dies on a storage error still enqueues its END_OF_DATA sentinel, which is
     * indistinguishable from legitimate exhaustion - so the puller's failure is checked the moment
     * its sentinel is observed (here and in {@link #secondaryEntries(int, StaticBuffer)}). Without
     * that check the scan would stream the entire remaining key space only to be discarded at the
     * end, or worse, complete "successfully" with silently missing rows (for a reindex: an
     * incomplete index getting ENABLED). {@link DataPuller#failure} is written before the sentinel
     * is enqueued, so observing the sentinel guarantees the failure is visible.
     */
    private SliceResult nextPrimaryRow() throws InterruptedException, TemporaryBackendException {
        if (finishedQueries[0]) {
            return null;
        }
        final SliceResult qr = dataQueues[0].take(); //Blocks until a row or the END_OF_DATA sentinel is available
        if (qr == END_OF_DATA) {
            throwIfPullerFailed(0);
            finishedQueries[0] = true;
            return null;
        }
        return qr;
    }

    /**
     * The merge join in {@link #orderedSecondaryEntries(int, StaticBuffer)} drops every row comparing
     * below the current grounding key as provably stale, which is only sound while the scan really
     * yields keys in their natural order - the promise {@link StoreFeatures#isKeyOrdered()} makes for
     * whole-key-space scans. A store that broke it would not fail visibly: rows of live keys would be
     * dropped as stale and the scan would complete "successfully" with silently missing data (for a
     * reindex: an incomplete index getting ENABLED). So the promise is verified against the grounding
     * stream as it is consumed - all of one scan's streams share its iteration order - and a violation
     * fails the scan instead. Single-query scans skip this: nothing is merged, so no row can be dropped.
     */
    private void verifyNaturalKeyOrder(final StaticBuffer key) throws TemporaryBackendException {
        if (previousGroundingKey != null && previousGroundingKey.compareTo(key) >= 0) {
            throw new TemporaryBackendException("Scan of store [" + store.getName() + "] returned keys out of " +
                "their natural order although the store declares itself key-ordered (StoreFeatures.isKeyOrdered()); " +
                "failing the scan because the ordered merge drops out-of-order rows, so its result would silently miss data");
        }
        previousGroundingKey = key;
    }

    /**
     * Entries of secondary query {@code queryIndex} for the given grounding key.
     * <p>
     * All queries iterate keys in the same order, and the grounding stream covers every key a
     * secondary stream can contain EXCEPT keys whose row set changed while the scan runs: a key
     * created after the grounding puller passed its position (or deleted before it arrived there)
     * can appear only in a secondary stream. Such a stale row can never match a grounding key.
     * Holding a single pending row per query (the historical design) therefore wedged the merge on
     * live graphs: the stale row occupied the slot forever and every later key silently received
     * {@link EntryList#EMPTY_LIST} for the query - for a reindex, documents missing that query's
     * data for the rest of the scan.
     * <p>
     * On a key-ordered scan every row is classified directly against the current grounding key
     * (classic merge join, at most one look-ahead row buffered). Otherwise the shared iteration
     * order still proves any buffered row OLDER than a matching row stale, so a bounded buffer
     * recovers from stale rows as later keys match. Dropping a stale row loses nothing: a key
     * written after the scan passed it is indexed by the normal live-write path, never by the scan.
     */
    private EntryList secondaryEntries(final int queryIndex, final StaticBuffer key)
            throws InterruptedException, TemporaryBackendException {
        return scanKeysNaturallyOrdered ? orderedSecondaryEntries(queryIndex, key)
            : bufferedSecondaryEntries(queryIndex, key);
    }

    /**
     * Merge-join for key-ordered scans: a strict key comparison classifies every row as stale
     * (behind the grounding stream - dropped), matching (consumed), or ahead (kept as the single
     * look-ahead row). Stale rows can never accumulate, no matter how sparse the query.
     */
    private EntryList orderedSecondaryEntries(final int queryIndex, final StaticBuffer key)
            throws InterruptedException, TemporaryBackendException {
        final Deque<SliceResult> pending = pendingRows[queryIndex];
        SliceResult next = pending.pollFirst();
        int dropped = 0; //Logged once per invocation to keep log volume bounded under stale bursts
        try {
            while (true) {
                if (next == null) {
                    if (finishedQueries[queryIndex]) {
                        return EntryList.EMPTY_LIST;
                    }
                    final SliceResult qr = dataQueues[queryIndex].take(); //Blocks until a row or the END_OF_DATA sentinel is available
                    if (qr == END_OF_DATA) {
                        throwIfPullerFailed(queryIndex);
                        finishedQueries[queryIndex] = true;
                        return EntryList.EMPTY_LIST;
                    }
                    next = qr;
                }
                final int order = next.key.compareTo(key);
                if (order == 0) {
                    assert queries.get(queryIndex).equals(next.query);
                    return next.entries;
                }
                if (order > 0) {
                    pending.addFirst(next); //Ahead of the grounding stream; the look-ahead row for later keys
                    return EntryList.EMPTY_LIST;
                }
                dropped++; //Behind the grounding stream: proven stale
                next = null;
            }
        } finally {
            logDroppedStaleRows(dropped, queryIndex);
        }
    }

    /**
     * Bounded-buffer recovery for scans without a computable key order (e.g. token-ordered CQL
     * scans): unmatched rows are buffered up to {@link #MAX_PENDING_ROWS_PER_QUERY}; every buffered
     * row older than a matching row is proven stale and dropped. If the buffer fills without a
     * match (which takes {@value #MAX_PENDING_ROWS_PER_QUERY} consecutive stale rows between two
     * matches of this query), the affected keys yield no entries for this query - the historical
     * per-key behavior - while buffered rows keep matching and evicting as later keys arrive.
     * <p>
     * Already-produced rows are always drained (a wait-free {@code poll()}), but BLOCKING waits for
     * new rows are budgeted per grounding key ({@link #MAX_BLOCKING_TAKES_PER_KEY}): a secondary
     * query much sparser than the grounding stream produces rows only as fast as its puller scans
     * the key space, and waiting on it up to the buffer cap would stall the whole merge - in the
     * extreme, serializing the scan behind the secondary stream instead of overlapping the two.
     */
    private EntryList bufferedSecondaryEntries(final int queryIndex, final StaticBuffer key)
            throws InterruptedException, TemporaryBackendException {
        final Deque<SliceResult> pending = pendingRows[queryIndex];
        SliceResult match = null;
        for (SliceResult buffered : pending) {
            if (buffered.key.equals(key)) {
                match = buffered;
                break;
            }
        }
        int blockingTakes = 0;
        while (match == null && !finishedQueries[queryIndex] && pending.size() < MAX_PENDING_ROWS_PER_QUERY) {
            SliceResult qr = dataQueues[queryIndex].poll(); //Wait-free: whatever the puller already produced
            if (qr == null) {
                if (blockingTakes >= MAX_BLOCKING_TAKES_PER_KEY) {
                    break; //Budget spent; do not stall the merge waiting on a sparser secondary stream
                }
                blockingTakes++;
                qr = dataQueues[queryIndex].take(); //Blocks until a row or the END_OF_DATA sentinel is available
            }
            if (qr == END_OF_DATA) {
                throwIfPullerFailed(queryIndex);
                finishedQueries[queryIndex] = true; //Buffered rows may still match later grounding keys
                break;
            }
            if (qr.key.equals(key)) {
                match = qr;
                break;
            }
            pending.addLast(qr); //Ahead of the grounding stream (or stale); kept for later keys
        }
        if (match == null) {
            return EntryList.EMPTY_LIST;
        }
        dropStaleRowsBefore(match, pending, queryIndex);
        assert queries.get(queryIndex).equals(match.query);
        return match.entries;
    }

    /**
     * Drops every buffered row preceding {@code match} as proven stale and removes {@code match}
     * itself as consumed. A match taken straight from the data queue is not buffered, in which case
     * every buffered row precedes it in stream order and all of them are dropped.
     */
    private void dropStaleRowsBefore(final SliceResult match, final Deque<SliceResult> pending, final int queryIndex) {
        int dropped = 0;
        while (!pending.isEmpty()) {
            if (pending.pollFirst() == match) {
                break;
            }
            dropped++;
        }
        logDroppedStaleRows(dropped, queryIndex);
    }

    private void logDroppedStaleRows(final int dropped, final int queryIndex) {
        if (dropped > 0 && log.isDebugEnabled()) {
            log.debug("Dropped {} row(s) of query [{}{}] whose key(s) changed during the scan and are " +
                "absent from the grounding stream; such keys are indexed by the live-write path instead",
                dropped, queryIndex, threadNameSuffix);
        }
    }

    /**
     * Zero-delay backstop for pullers whose sentinel the merge loop never consumed (a query can end
     * the scan with buffered rows and rows still queued behind them). The rows already merged are
     * complete either way, but a failure recorded by such a puller still marks the scan as
     * incomplete-by-error. Deliberately does NOT join or wait: a still-running straggler puller is
     * normal here and is interrupted by {@link #join()}/{@link #cleanup()}.
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
