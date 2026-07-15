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
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A data puller that dies on a storage error must fail the whole scan. Historically the dead puller
 * signalled end-of-data, the merge loop treated the query as exhausted and the scan completed
 * "successfully" with silently missing rows - for a reindex that means an incomplete index getting
 * ENABLED. This test simulates a storage error mid-scan and asserts the scan future surfaces it.
 */
public class ScanPullerFailureTest {

    private static final TimestampProvider TIMES = TimestampProviders.MICRO;
    private static final String STORE_NAME = "pullerfailure";
    private static final int NUM_KEYS = 5000;
    private static final int FAIL_AFTER_KEYS = 100;

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
    public void pullerStorageFailureFailsTheScan() {
        final StandardScanner scanner = new StandardScanner(failingManager(manager));

        final ScanJobFuture future;
        try {
            future = scanner.build()
                .setStoreName(STORE_NAME)
                .setNumProcessingThreads(2)
                .setWorkBlockSize(100)
                .setTimestampProvider(TIMES)
                .setJob(new CountingScanJob())
                .execute();
        } catch (BackendException e) {
            throw new AssertionError(e);
        }

        final ExecutionException failure =
            assertThrows(ExecutionException.class, () -> future.get(60, TimeUnit.SECONDS),
                "scan must fail when a data puller dies instead of completing with missing rows");
        Throwable cause = failure.getCause();
        while (cause != null && !(cause instanceof TemporaryBackendException)) {
            cause = cause.getCause();
        }
        assertInstanceOf(TemporaryBackendException.class, cause,
            "failure should be reported as a TemporaryBackendException, got: " + failure);
        assertTrue(cause.getMessage().contains("incomplete"),
            "failure should explain the scan result would be incomplete: " + cause.getMessage());
    }

    /**
     * Wraps the store manager so the unordered-scan {@link KeyIterator} throws a simulated storage
     * error after {@link #FAIL_AFTER_KEYS} keys.
     */
    private static KeyColumnValueStoreManager failingManager(KeyColumnValueStoreManager real) {
        return new KCVSManagerProxy(real) {
            @Override
            public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
                return failingStore(manager.openDatabase(name, metaData));
            }
        };
    }

    private static KeyColumnValueStore failingStore(KeyColumnValueStore real) {
        return new KCVSProxy(real) {
            @Override
            public KeyIterator getKeys(SliceQuery columnQuery, StoreTransaction txh) throws BackendException {
                return failAfter(store.getKeys(columnQuery, unwrapTx(txh)), FAIL_AFTER_KEYS);
            }
        };
    }

    private static KeyIterator failAfter(KeyIterator delegate, int keys) {
        return new KeyIterator() {
            private int seen = 0;

            @Override
            public RecordIterator<Entry> getEntries() {
                return delegate.getEntries();
            }

            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public StaticBuffer next() {
                if (++seen > keys) {
                    throw new RuntimeException("Simulated storage failure during scan");
                }
                return delegate.next();
            }

            @Override
            public void close() throws IOException {
                delegate.close();
            }
        };
    }

    private static final class CountingScanJob implements ScanJob {

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
            // no-op
        }

        @Override
        public ScanJob clone() {
            return this;
        }
    }
}
