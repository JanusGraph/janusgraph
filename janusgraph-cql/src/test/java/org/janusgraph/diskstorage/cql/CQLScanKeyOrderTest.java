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

package org.janusgraph.diskstorage.cql;

import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.KeyColumnValueStoreUtil;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SplittableScanStore;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJob;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.diskstorage.keycolumnvalue.scan.StandardScanner;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Conformance of the client-computed CQL scan order ({@code StoreFeatures.getScanKeyOrder()}) with
 * the order a real Cassandra actually iterates keys in. Under the Murmur3 partitioner scans stream
 * in ring order - (token, key bytes) - which the driver can compute client-side; declaring it lets
 * the multi-query scan merge use its lossless merge-join strategy instead of the bounded-buffer
 * strategy whose recovery from concurrent writes is capped. The declaration is only safe if it
 * EXACTLY matches the server (a mismatch makes the merge drop live rows as stale), so this test
 * checks the promise empirically: every whole-scan and split iteration must be strictly increasing
 * under the declared comparator, and a multi-query scan must pass the scan framework's order
 * tripwire, which fails the scan on the first out-of-order key.
 * <p>
 * Under the ByteOrderedPartitioner no comparator is declared BY DESIGN - scans iterate in the
 * natural key order and the merge join engages through {@code StoreFeatures.isKeyOrdered()} - so
 * the comparator-conformance tests are skipped on key-ordered clusters (the end-to-end merge test
 * still runs there, exercising the natural-order join against a real backend).
 */
@Testcontainers
public class CQLScanKeyOrderTest {

    private static final TimestampProvider TIMES = TimestampProviders.MICRO;
    private static final String STORE_NAME = "scanordertest";
    private static final int NUM_KEYS = 300;

    private static final SliceQuery EVERYTHING =
        new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128));
    private static final SliceQuery GROUNDING_QUERY =
        new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128)).setLimit(1);

    private static final String PROCESSED_ROWS = "processed-rows";
    private static final String ROWS_WITH_SECONDARY = "rows-with-secondary";

    @Container
    public static final JanusGraphCassandraContainer cqlContainer = new JanusGraphCassandraContainer();

    private CQLStoreManager manager;
    private KeyColumnValueStore store;

    @BeforeEach
    public void setUp() throws BackendException {
        manager = new CQLStoreManager(cqlContainer.getConfiguration(CQLScanKeyOrderTest.class.getSimpleName()));
        store = manager.openDatabase(STORE_NAME);
        loadData();
    }

    @AfterEach
    public void tearDown() throws BackendException {
        if (store != null) store.close();
        if (manager != null) {
            manager.clearStorage();
            manager.close();
        }
    }

    /**
     * The comparator-conformance tests only apply to token-ordered clusters: a key-ordered cluster
     * (ByteOrderedPartitioner) iterates scans in the natural key order and declares that through
     * {@code StoreFeatures.isKeyOrdered()} instead of providing a comparator.
     */
    private void assumeTokenOrderedCluster() {
        Assumptions.assumeFalse(manager.getFeatures().isKeyOrdered(),
            "key-ordered (byte-ordered) cluster: scans follow the natural key order and no client-computed comparator is declared");
    }

    @Test
    public void murmur3StoreMustDeclareAComputableScanKeyOrder() {
        assumeTokenOrderedCluster();
        assertNotNull(manager.getFeatures().getScanKeyOrder(),
            "CQL with the Murmur3 partitioner must expose its token order for the lossless scan merge");
    }

    @Test
    public void wholeScanMustIterateInTheDeclaredOrder() throws Exception {
        assumeTokenOrderedCluster();
        final Comparator<StaticBuffer> order = manager.getFeatures().getScanKeyOrder();
        assertNotNull(order);

        final StoreTransaction tx = beginTx();
        try {
            final List<StaticBuffer> keys = drainKeys(store.getKeys(EVERYTHING, tx));
            assertEquals(NUM_KEYS, keys.size());
            assertStrictlyIncreasing(keys, order);
            assertTrue(countNaturalInversions(keys) > 0,
                "token order coincided with natural key order; the conformance check would be vacuous");
        } finally {
            tx.rollback();
        }
    }

    /**
     * With parallel-scan-token-ranges > 1 a whole-key-space getKeys concatenates one pager per
     * token range; the concatenation must follow ring order for the global iteration to stay
     * monotonic under the declared comparator.
     */
    @Test
    public void wholeScanWithParallelTokenRangesMustIterateInTheDeclaredOrder() throws Exception {
        assumeTokenOrderedCluster();
        store.close();
        manager.close();
        final ModifiableConfiguration config =
            cqlContainer.getConfiguration(CQLScanKeyOrderTest.class.getSimpleName());
        config.set(CQLConfigOptions.PARALLEL_SCAN_TOKEN_RANGES, 5);
        manager = new CQLStoreManager(config);
        store = manager.openDatabase(STORE_NAME);

        wholeScanMustIterateInTheDeclaredOrder();
    }

    @Test
    public void splitScansMustIterateInTheDeclaredOrder() throws Exception {
        assumeTokenOrderedCluster();
        final Comparator<StaticBuffer> order = manager.getFeatures().getScanKeyOrder();
        assertNotNull(order);
        final int splitCount = 4;

        final StoreTransaction tx = beginTx();
        try {
            final SplittableScanStore splittable = assertInstanceOf(SplittableScanStore.class, store,
                "the CQL store must expose split scans for the split-order conformance check");
            final Set<StaticBuffer> union = new HashSet<>();
            for (int split = 0; split < splitCount; split++) {
                final List<StaticBuffer> keys = drainKeys(splittable.getKeysForSplit(EVERYTHING, tx, split, splitCount));
                assertStrictlyIncreasing(keys, order);
                union.addAll(keys);
            }
            assertEquals(NUM_KEYS, union.size(), "splits must tile the key space exactly");
        } finally {
            tx.rollback();
        }
    }

    /**
     * End to end: a multi-query scan on the real backend runs the merge join under the declared
     * order with the framework's order tripwire active, so any client/server order mismatch fails
     * the scan, and every key must keep its secondary data (an out-of-order comparator would blank
     * keys by dropping their rows as stale before tripping).
     */
    @Test
    public void multiQueryScanMustMergeLosslesslyUnderTheDeclaredOrder() throws Exception {
        final ScanMetrics metrics = new StandardScanner(manager).build()
            .setStoreName(STORE_NAME)
            .setNumProcessingThreads(2)
            .setWorkBlockSize(100)
            .setTimestampProvider(TIMES)
            .setJob(new SecondaryDataCountingJob())
            .execute()
            .get(120, TimeUnit.SECONDS);

        assertEquals(NUM_KEYS, metrics.getCustom(PROCESSED_ROWS));
        assertEquals(NUM_KEYS, metrics.getCustom(ROWS_WITH_SECONDARY),
            "every key must keep its secondary data under the merge join");
        assertEquals(NUM_KEYS, metrics.get(ScanMetrics.Metric.SUCCESS));
        assertEquals(0, metrics.get(ScanMetrics.Metric.FAILURE));
    }

    private void loadData() throws BackendException {
        final StoreTransaction tx = beginTx();
        try {
            for (int i = 0; i < NUM_KEYS; i++) {
                KeyColumnValueStoreUtil.insert(store, tx, i, "a", "value" + i);
            }
        } catch (BackendException | RuntimeException e) {
            tx.rollback();
            throw e;
        }
        tx.commit();
    }

    private StoreTransaction beginTx() throws BackendException {
        return manager.beginTransaction(
            StandardBaseTransactionConfig.of(TIMES, manager.getFeatures().getKeyConsistentTxConfig()));
    }

    private static List<StaticBuffer> drainKeys(KeyIterator iterator) throws Exception {
        final List<StaticBuffer> keys = new ArrayList<>();
        try {
            while (iterator.hasNext()) {
                keys.add(iterator.next());
            }
        } finally {
            iterator.close();
        }
        return keys;
    }

    private static void assertStrictlyIncreasing(List<StaticBuffer> keys, Comparator<StaticBuffer> order) {
        for (int i = 1; i < keys.size(); i++) {
            final int cmp = order.compare(keys.get(i - 1), keys.get(i));
            assertTrue(cmp < 0, "scan iteration violated the declared order at position " + i + " (compare=" + cmp + ")");
        }
    }

    private static int countNaturalInversions(List<StaticBuffer> keys) {
        int inversions = 0;
        for (int i = 1; i < keys.size(); i++) {
            if (keys.get(i - 1).compareTo(keys.get(i)) > 0) {
                inversions++;
            }
        }
        return inversions;
    }

    private static final class SecondaryDataCountingJob implements ScanJob {

        @Override
        public List<SliceQuery> getQueries() {
            return Arrays.asList(GROUNDING_QUERY, EVERYTHING);
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
            final EntryList secondary = entries.get(EVERYTHING);
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
