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
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.KeyColumnValueStoreUtil;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.inmemory.InMemoryStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KCVSManagerProxy;
import org.janusgraph.diskstorage.keycolumnvalue.KCVSProxy;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The multi-query scan merge must not lose data when a key's rows change while the scan runs. Such
 * a key can appear in a secondary slice query's stream but not in the grounding (key-existence)
 * stream, whose puller already passed the key's position. Historically that stale row permanently
 * occupied the merge's single pending slot for the query, so every later key was silently merged
 * with empty results for it - for a reindex, documents missing that query's data for the rest of
 * the scan.
 * <p>
 * These tests inject stale keys into the secondary stream only, at their correct sort positions,
 * and assert every real key still carries its secondary data (and that the stale keys do not
 * surface as scan rows). Both merge strategies are exercised: the merge join used when the scan
 * iterates keys in natural order, and the bounded-buffer recovery used when the key order is not
 * computable (simulated by masking {@code keyOrdered} off the in-memory store's features, as on
 * token-ordered backends like CQL). Secondary queries with data on only a subset of keys (the
 * common case - a slice query only returns keys with matching columns) are covered explicitly.
 */
public class ScanStaleSecondaryRowTest {

    private static final TimestampProvider TIMES = TimestampProviders.MICRO;
    private static final String STORE_NAME = "stalerowscan";
    private static final int NUM_KEYS = 500;
    /** Every SPARSE_STRIDE-th key carries the secondary column in the sparse scenarios. */
    private static final int SPARSE_STRIDE = 10;

    private static final SliceQuery GROUNDING_QUERY =
        new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128)).setLimit(1);
    /** Covers only the "s" column, so keys without it are absent from this query's stream. */
    private static final SliceQuery SECONDARY_QUERY = new SliceQuery(
        KeyColumnValueStoreUtil.stringToByteBuffer("s"), KeyColumnValueStoreUtil.stringToByteBuffer("t"));

    private static final String PROCESSED_ROWS = "processed-rows";
    private static final String ROWS_WITH_SECONDARY = "rows-with-secondary";

    private KeyColumnValueStoreManager manager;
    private KeyColumnValueStore store;

    @BeforeEach
    public void setUp() throws BackendException {
        manager = new InMemoryStoreManager();
        store = manager.openDatabase(STORE_NAME);
    }

    @AfterEach
    public void tearDown() throws BackendException {
        if (store != null) store.close();
        if (manager != null) manager.close();
    }

    /** Every key has secondary data; stale bursts of one and two rows; bounded-buffer merge path. */
    @Test
    public void staleRowsMustNotWedgeADenseQueryOnTheBufferedPath() throws Exception {
        loadData(1);
        final Map<Integer, Integer> staleBursts = new HashMap<>();
        staleBursts.put(100, 1);
        staleBursts.put(250, 2);
        staleBursts.put(400, 1);

        runScanAndAssert(withoutNaturalKeyOrder(manager), staleBursts, NUM_KEYS);
    }

    /**
     * Only every tenth key has secondary data and a burst of 45 consecutive stale rows (more than
     * the merge's pending buffer would hold) arrives between two matches: the natural-key-order
     * merge join classifies each stale row directly and must lose nothing, no matter the burst size.
     */
    @Test
    public void manyConsecutiveStaleRowsMustNotStarveASparseQueryOnTheOrderedPath() throws Exception {
        loadData(SPARSE_STRIDE);
        final Map<Integer, Integer> staleBursts = new HashMap<>();
        staleBursts.put(20, 45);

        runScanAndAssert(manager, staleBursts, NUM_KEYS / SPARSE_STRIDE);
    }

    /** Sparse secondary data with small stale bursts on the bounded-buffer merge path. */
    @Test
    public void staleRowsMustNotStarveASparseQueryOnTheBufferedPath() throws Exception {
        loadData(SPARSE_STRIDE);
        final Map<Integer, Integer> staleBursts = new HashMap<>();
        staleBursts.put(10, 2);
        staleBursts.put(30, 3);

        runScanAndAssert(withoutNaturalKeyOrder(manager), staleBursts, NUM_KEYS / SPARSE_STRIDE);
    }

    /**
     * Characterizes the bounded-buffer path's documented limit: a burst of stale rows LARGER than
     * the pending buffer (45 > 32) between two matches cannot be resolved without a computable key
     * order - staleness is only ever proven by a later match, and none of the buffered stale rows
     * will ever match. The merge then degrades exactly like the historical behavior for the keys
     * after the burst (empty results for this query), but the scan must still complete: every key
     * surfaces with its grounding data, nothing hangs and nothing fails. Keys with secondary data
     * before the burst keep it. The same burst on the ordered path loses nothing (see
     * {@link #manyConsecutiveStaleRowsMustNotStarveASparseQueryOnTheOrderedPath()}), which is why
     * backends should expose a key order where they can.
     */
    @Test
    public void staleBurstBeyondTheBufferDegradesButNeverHangsTheBufferedPath() throws Exception {
        loadData(SPARSE_STRIDE);
        final int burstAfterMatch = 20;
        final Map<Integer, Integer> staleBursts = new HashMap<>();
        staleBursts.put(burstAfterMatch, 45);

        // Matches 1..20 keep their data; the rows of matches 21..50 sit behind the unresolvable
        // stale rows and are never reached - the documented degradation, bounded to this query.
        runScanAndAssert(withoutNaturalKeyOrder(manager), staleBursts, burstAfterMatch);
    }

    /**
     * The natural-order merge join drops every row comparing below the current grounding key, which
     * is only sound while the store's scan really yields keys in their natural order - the promise
     * {@code StoreFeatures.isKeyOrdered()} makes. A store that breaks the promise must fail the scan
     * loudly: completing it would silently drop live keys' rows as "stale", the very data loss this
     * class guards against. (Backends without a natural scan order, e.g. token-ordered CQL, declare
     * {@code keyOrdered=false} and use the buffered strategy instead - see the tests above.)
     */
    @Test
    public void keyOrderViolationOnTheOrderedPathMustFailTheScanInsteadOfDroppingRows() throws Exception {
        loadData(1);

        final ExecutionException failure = assertThrows(ExecutionException.class,
            () -> runScan(misorderedGroundingManager(manager)));

        final Throwable cause = failure.getCause();
        assertTrue(cause instanceof TemporaryBackendException,
            "an order-contract violation must surface as a backend failure but was: " + cause);
        assertTrue(cause.getMessage().contains("natural order"), cause.getMessage());
    }

    /**
     * @param secondaryStride every how-many-th key receives the secondary "s" column (1 = every key)
     */
    private void loadData(int secondaryStride) throws BackendException {
        final StoreTransaction tx = manager.beginTransaction(
            StandardBaseTransactionConfig.of(TIMES, manager.getFeatures().getKeyConsistentTxConfig()));
        for (int i = 0; i < NUM_KEYS; i++) {
            KeyColumnValueStoreUtil.insert(store, tx, i, "a", "ground" + i);
            if (i % secondaryStride == 0) {
                KeyColumnValueStoreUtil.insert(store, tx, i, "s", "sec" + i);
            }
        }
        tx.commit();
    }

    private void runScanAndAssert(KeyColumnValueStoreManager scanManager, Map<Integer, Integer> staleBursts,
                                  int expectedRowsWithSecondary) throws Exception {
        final ScanMetrics metrics = runScan(staleRowInjectingManager(scanManager, staleBursts));

        assertEquals(NUM_KEYS, metrics.getCustom(PROCESSED_ROWS),
            "every real key must surface exactly once; stale keys must not surface at all");
        assertEquals(expectedRowsWithSecondary, metrics.getCustom(ROWS_WITH_SECONDARY),
            "every key with secondary data must keep it; a wedged or starved merge blanks keys after a stale row");
        assertEquals(NUM_KEYS, metrics.get(ScanMetrics.Metric.SUCCESS));
        assertEquals(0, metrics.get(ScanMetrics.Metric.FAILURE));
    }

    private ScanMetrics runScan(KeyColumnValueStoreManager scanManager) throws Exception {
        return new StandardScanner(scanManager).build()
            .setStoreName(STORE_NAME)
            .setNumProcessingThreads(2)
            .setWorkBlockSize(100)
            .setTimestampProvider(TIMES)
            .setJob(new SecondaryDataCountingJob())
            .execute()
            .get(60, TimeUnit.SECONDS);
    }

    /**
     * Masks {@code keyOrdered} off the store's features, making the merge treat the scan like a
     * token-ordered backend (no computable key order) and use its bounded-buffer strategy.
     */
    private static KeyColumnValueStoreManager withoutNaturalKeyOrder(KeyColumnValueStoreManager real) {
        return new KCVSManagerProxy(real) {
            @Override
            public StoreFeatures getFeatures() {
                return new StandardStoreFeatures.Builder(real.getFeatures()).keyOrdered(false).build();
            }
        };
    }

    /**
     * Wraps the store manager so the SECONDARY query's key stream contains extra keys the grounding
     * stream does not have, at their correct sort positions - exactly what a key written mid-scan
     * between the two pullers' passes looks like.
     *
     * @param staleBursts number of consecutive stale keys to inject (value) after the n-th key
     *                    returned by the secondary stream (n = map key, 1-based)
     */
    private static KeyColumnValueStoreManager staleRowInjectingManager(KeyColumnValueStoreManager real,
                                                                       Map<Integer, Integer> staleBursts) {
        return new KCVSManagerProxy(real) {
            @Override
            public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
                return staleRowInjectingStore(real.openDatabase(name, metaData), staleBursts);
            }
        };
    }

    private static KeyColumnValueStore staleRowInjectingStore(KeyColumnValueStore real,
                                                              Map<Integer, Integer> staleBursts) {
        return new KCVSProxy(real) {
            @Override
            public KeyIterator getKeys(SliceQuery columnQuery, StoreTransaction txh) throws BackendException {
                final KeyIterator keys = real.getKeys(columnQuery, unwrapTx(txh));
                // Only the secondary stream sees the stale keys (the grounding puller "passed their
                // position before they were written"). Match the queries exactly so a change to the
                // scan's query shapes fails loudly instead of silently injecting into the wrong stream.
                if (columnQuery.equals(GROUNDING_QUERY)) {
                    return keys;
                }
                if (columnQuery.equals(SECONDARY_QUERY)) {
                    return injectStaleKeys(keys, staleBursts);
                }
                throw new IllegalStateException("Unexpected scan query: " + columnQuery);
            }
        };
    }

    /**
     * Serves the grounding stream with one key out of natural order - what the scan of a store that
     * (wrongly) declares {@code keyOrdered} while iterating in some other consistent order looks
     * like. The scan must die on the grounding stream's order violation, so the secondary stream
     * needs no doctoring.
     */
    private static KeyColumnValueStoreManager misorderedGroundingManager(KeyColumnValueStoreManager real) {
        return new KCVSManagerProxy(real) {
            @Override
            public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
                final KeyColumnValueStore store = real.openDatabase(name, metaData);
                return new KCVSProxy(store) {
                    @Override
                    public KeyIterator getKeys(SliceQuery columnQuery, StoreTransaction txh) throws BackendException {
                        final KeyIterator keys = store.getKeys(columnQuery, unwrapTx(txh));
                        return columnQuery.equals(GROUNDING_QUERY) ? injectMisorderedKey(keys, 10) : keys;
                    }
                };
            }
        };
    }

    /** Inserts, after the n-th real key, a key sorting BEFORE it: its strict prefix (one byte shorter). */
    private static KeyIterator injectMisorderedKey(KeyIterator delegate, int afterNthKey) {
        return new KeyIterator() {
            private int realKeysReturned = 0;
            private StaticBuffer lastRealKey = null;
            private boolean injectNow = false;
            private boolean currentIsInjected = false;

            @Override
            public boolean hasNext() {
                return injectNow || delegate.hasNext();
            }

            @Override
            public StaticBuffer next() {
                if (injectNow) {
                    injectNow = false;
                    currentIsInjected = true;
                    final byte[] prefix = new byte[lastRealKey.length() - 1];
                    for (int i = 0; i < prefix.length; i++) {
                        prefix[i] = lastRealKey.getByte(i);
                    }
                    return StaticArrayBuffer.of(prefix);
                }
                final StaticBuffer key = delegate.next();
                currentIsInjected = false;
                lastRealKey = key;
                injectNow = ++realKeysReturned == afterNthKey;
                return key;
            }

            @Override
            public RecordIterator<Entry> getEntries() {
                return currentIsInjected ? staleEntries() : delegate.getEntries();
            }

            @Override
            public void close() throws IOException {
                delegate.close();
            }
        };
    }

    private static KeyIterator injectStaleKeys(KeyIterator delegate, Map<Integer, Integer> staleBursts) {
        return new KeyIterator() {
            private int realKeysReturned = 0;
            private int staleToInject = 0;
            private int staleSequence = 0;
            private StaticBuffer lastRealKey = null;
            private boolean currentIsStale = false;

            @Override
            public boolean hasNext() {
                return staleToInject > 0 || delegate.hasNext();
            }

            @Override
            public StaticBuffer next() {
                if (staleToInject > 0) {
                    staleToInject--;
                    currentIsStale = true;
                    return staleKeyAfter(lastRealKey, staleSequence++);
                }
                final StaticBuffer key = delegate.next();
                currentIsStale = false;
                lastRealKey = key;
                realKeysReturned++;
                final Integer burst = staleBursts.get(realKeysReturned);
                if (burst != null) {
                    staleSequence = 0;
                    staleToInject = burst;
                }
                return key;
            }

            @Override
            public RecordIterator<Entry> getEntries() {
                return currentIsStale ? staleEntries() : delegate.getEntries();
            }

            @Override
            public void close() throws IOException {
                delegate.close();
            }
        };
    }

    /**
     * A key sorting directly after {@code realKey} and before the next real key: appending a suffix
     * byte places the stale key right after {@code realKey} in unsigned byte order but before any
     * following distinct key ({@code KeyColumnValueStoreUtil} writes fixed-length keys, so no real
     * key is a prefix of another), and ascending suffix bytes keep multiple stale keys ordered
     * among themselves.
     */
    private static StaticBuffer staleKeyAfter(StaticBuffer realKey, int sequence) {
        final byte[] bytes = new byte[realKey.length() + 1];
        for (int i = 0; i < realKey.length(); i++) {
            bytes[i] = realKey.getByte(i);
        }
        bytes[realKey.length()] = (byte) sequence;
        return StaticArrayBuffer.of(bytes);
    }

    private static RecordIterator<Entry> staleEntries() {
        final Iterator<Entry> single = Collections.singletonList(
            StaticArrayEntry.of(KeyColumnValueStoreUtil.stringToByteBuffer("s"),
                KeyColumnValueStoreUtil.stringToByteBuffer("stale"))).iterator();
        return new RecordIterator<Entry>() {
            @Override
            public boolean hasNext() {
                return single.hasNext();
            }

            @Override
            public Entry next() {
                return single.next();
            }

            @Override
            public void close() {
                // NOP
            }
        };
    }

    private static final class SecondaryDataCountingJob implements ScanJob {

        @Override
        public List<SliceQuery> getQueries() {
            return Arrays.asList(GROUNDING_QUERY, SECONDARY_QUERY);
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
            metrics.incrementCustom(PROCESSED_ROWS);
            final EntryList secondary = entries.get(SECONDARY_QUERY);
            if (secondary != null && !secondary.isEmpty()) {
                metrics.incrementCustom(ROWS_WITH_SECONDARY);
            }
        }

        @Override
        public ScanJob clone() {
            return new SecondaryDataCountingJob();
        }
    }
}
