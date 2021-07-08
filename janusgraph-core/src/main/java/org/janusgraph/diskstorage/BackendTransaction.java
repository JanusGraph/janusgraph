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

package org.janusgraph.diskstorage;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.cache.CacheTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVSCache;
import org.janusgraph.diskstorage.log.kcvs.ExternalCachePersistor;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Bundles all storage/index transactions and provides a proxy for some of their
 * methods for convenience. Also increases robustness of read call by attempting
 * read calls multiple times on failure.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BackendTransaction implements LoggableTransaction {

    private static final Logger log =
            LoggerFactory.getLogger(BackendTransaction.class);

    public static final int MIN_TASKS_TO_PARALLELIZE = 2;

    //Assumes 64 bit key length as specified in IDManager
    public static final StaticBuffer EDGESTORE_MIN_KEY = BufferUtil.zeroBuffer(8);
    public static final StaticBuffer EDGESTORE_MAX_KEY = BufferUtil.oneBuffer(8);

    private final CacheTransaction storeTx;
    private final BaseTransactionConfig txConfig;
    private final StoreFeatures storeFeatures;

    private final KCVSCache edgeStore;
    private final KCVSCache indexStore;
    private final KCVSCache txLogStore;

    private final Duration maxReadTime;

    private final Executor threadPool;

    private final Map<String, IndexTransaction> indexTx;

    private boolean acquiredLock = false;
    private boolean cacheEnabled = true;

    public BackendTransaction(CacheTransaction storeTx, BaseTransactionConfig txConfig,
                              StoreFeatures features, KCVSCache edgeStore, KCVSCache indexStore,
                              KCVSCache txLogStore, Duration maxReadTime,
                              Map<String, IndexTransaction> indexTx, Executor threadPool) {
        this.storeTx = storeTx;
        this.txConfig = txConfig;
        this.storeFeatures = features;
        this.edgeStore = edgeStore;
        this.indexStore = indexStore;
        this.txLogStore = txLogStore;
        this.maxReadTime = maxReadTime;
        this.indexTx = indexTx;
        this.threadPool = threadPool;
    }

    public boolean hasAcquiredLock() {
        return acquiredLock;
    }

    public StoreTransaction getStoreTransaction() {
        return storeTx;
    }

    public ExternalCachePersistor getTxLogPersistor() {
        return new ExternalCachePersistor(txLogStore,storeTx);
    }

    public BaseTransactionConfig getBaseTransactionConfig() {
        return txConfig;
    }

    public boolean hasIndexTransaction(String index) {
        Preconditions.checkArgument(StringUtils.isNotBlank(index), "index cannot be blank");
        return indexTx.containsKey(index);
    }

    public IndexTransaction getIndexTransaction(String index) {
        Preconditions.checkArgument(StringUtils.isNotBlank(index), "index cannot be blank");
        IndexTransaction itx = indexTx.get(index);
        return Preconditions.checkNotNull(itx, "Unknown index: " + index);
    }

    public void disableCache() {
        this.cacheEnabled = false;
    }

    public void enableCache() {
        this.cacheEnabled = true;
    }

    public void commitStorage() throws BackendException {
        storeTx.commit();
    }

    public Map<String,Throwable> commitIndexes() {
        final Map<String,Throwable> exceptions = new HashMap<>(indexTx.size());
        for (Map.Entry<String,IndexTransaction> indexTransactionEntry : indexTx.entrySet()) {
            try {
                indexTransactionEntry.getValue().commit();
            } catch (Throwable e) {
                exceptions.put(indexTransactionEntry.getKey(),e);
            }
        }
        return exceptions;
    }

    @Override
    public void commit() throws BackendException {
        storeTx.commit();
        for (IndexTransaction itx : indexTx.values()) itx.commit();
    }

    /**
     * Rolls back all transactions and makes sure that this does not get cut short
     * by exceptions. If exceptions occur, the storage exception takes priority on re-throw.
     * @throws BackendException
     */
    @Override
    public void rollback() throws BackendException {
        Throwable exception = null;
        for (IndexTransaction itx : indexTx.values()) {
            try {
                itx.rollback();
            } catch (Throwable e) {
                exception = e;
            }
        }
        storeTx.rollback();
        if (exception!=null) { //throw any encountered index transaction rollback exceptions
            if (exception instanceof BackendException) throw (BackendException)exception;
            else throw new PermanentBackendException("Unexpected exception",exception);
        }
    }


    @Override
    public void logMutations(DataOutput out) {
        //Write
        storeTx.logMutations(out);
        for (Map.Entry<String, IndexTransaction> itx : indexTx.entrySet()) {
            out.writeObjectNotNull(itx.getKey());
            itx.getValue().logMutations(out);
        }
    }

    /* ###################################################
            Convenience Write Methods
     */

    /**
     * Applies the specified insertion and deletion mutations on the edge store to the provided key.
     * Both, the list of additions or deletions, may be empty or NULL if there is nothing to be added and/or deleted.
     *
     * @param key       Key
     * @param additions List of entries (column + value) to be added
     * @param deletions List of columns to be removed
     */
    public void mutateEdges(StaticBuffer key, List<Entry> additions, List<Entry> deletions) throws BackendException {
        edgeStore.mutateEntries(key, additions, deletions, storeTx);
    }

    /**
     * Applies the specified insertion and deletion mutations on the property index to the provided key.
     * Both, the list of additions or deletions, may be empty or NULL if there is nothing to be added and/or deleted.
     *
     * @param key       Key
     * @param additions List of entries (column + value) to be added
     * @param deletions List of columns to be removed
     */
    public void mutateIndex(StaticBuffer key, List<Entry> additions, List<Entry> deletions) throws BackendException {
        indexStore.mutateEntries(key, additions, deletions, storeTx);
    }

    /**
     * Acquires a lock for the key-column pair on the edge store which ensures that nobody else can take a lock on that
     * respective entry for the duration of this lock (but somebody could potentially still overwrite
     * the key-value entry without taking a lock).
     * The expectedValue defines the value expected to match the value at the time the lock is acquired (or null if it is expected
     * that the key-column pair does not exist).
     * <p>
     * If this method is called multiple times with the same key-column pair in the same transaction, all but the first invocation are ignored.
     * <p>
     * The lock has to be released when the transaction closes (commits or aborts).
     *
     * @param key           Key on which to lock
     * @param column        Column the column on which to lock
     */
    public void acquireEdgeLock(StaticBuffer key, StaticBuffer column) throws BackendException {
        acquiredLock = true;
        edgeStore.acquireLock(key, column, null, storeTx);
    }

    public void acquireEdgeLock(StaticBuffer key, Entry entry) throws BackendException {
        acquiredLock = true;
        edgeStore.acquireLock(key, entry.getColumnAs(StaticBuffer.STATIC_FACTORY), entry.getValueAs(StaticBuffer.STATIC_FACTORY), storeTx);
    }

    /**
     * Acquires a lock for the key-column pair on the property index which ensures that nobody else can take a lock on that
     * respective entry for the duration of this lock (but somebody could potentially still overwrite
     * the key-value entry without taking a lock).
     * The expectedValue defines the value expected to match the value at the time the lock is acquired (or null if it is expected
     * that the key-column pair does not exist).
     * <p>
     * If this method is called multiple times with the same key-column pair in the same transaction, all but the first invocation are ignored.
     * <p>
     * The lock has to be released when the transaction closes (commits or aborts).
     *
     * @param key           Key on which to lock
     * @param column        Column the column on which to lock
     */
    public void acquireIndexLock(StaticBuffer key, StaticBuffer column) throws BackendException {
        acquiredLock = true;
        indexStore.acquireLock(key, column, null, storeTx);
    }

    public void acquireIndexLock(StaticBuffer key, Entry entry) throws BackendException {
        acquiredLock = true;
        indexStore.acquireLock(key, entry.getColumnAs(StaticBuffer.STATIC_FACTORY), entry.getValueAs(StaticBuffer.STATIC_FACTORY), storeTx);
    }

    /* ###################################################
            Convenience Read Methods
     */

    public EntryList edgeStoreQuery(final KeySliceQuery query) {
        return executeRead(new Callable<EntryList>() {
            @Override
            public EntryList call() throws Exception {
                return cacheEnabled?edgeStore.getSlice(query, storeTx):
                                    edgeStore.getSliceNoCache(query,storeTx);
            }

            @Override
            public String toString() {
                return "EdgeStoreQuery";
            }
        });
    }

    public Map<StaticBuffer,EntryList> edgeStoreMultiQuery(final List<StaticBuffer> keys, final SliceQuery query) {
        if (storeFeatures.hasMultiQuery()) {
            return executeRead(new Callable<Map<StaticBuffer,EntryList>>() {
                @Override
                public Map<StaticBuffer,EntryList> call() throws Exception {
                    return cacheEnabled?edgeStore.getSlice(keys, query, storeTx):
                                        edgeStore.getSliceNoCache(keys, query, storeTx);
                }

                @Override
                public String toString() {
                    return "MultiEdgeStoreQuery";
                }
            });
        } else {
            final Map<StaticBuffer,EntryList> results = new HashMap<>(keys.size());
            if (threadPool == null || keys.size() < MIN_TASKS_TO_PARALLELIZE) {
                for (StaticBuffer key : keys) {
                    results.put(key,edgeStoreQuery(new KeySliceQuery(key, query)));
                }
            } else {
                final CountDownLatch doneSignal = new CountDownLatch(keys.size());
                final AtomicInteger failureCount = new AtomicInteger(0);
                EntryList[] resultArray = new EntryList[keys.size()];
                for (int i = 0; i < keys.size(); i++) {
                    threadPool.execute(new SliceQueryRunner(new KeySliceQuery(keys.get(i), query),
                            doneSignal, failureCount, resultArray, i));
                }
                try {
                    doneSignal.await();
                } catch (InterruptedException e) {
                    throw new JanusGraphException("Interrupted while waiting for multi-query to complete", e);
                }
                if (failureCount.get() > 0) {
                    throw new JanusGraphException("Could not successfully complete multi-query. " + failureCount.get() + " individual queries failed.");
                }
                for (int i=0;i<keys.size();i++) {
                    assert resultArray[i]!=null;
                    results.put(keys.get(i),resultArray[i]);
                }
            }
            return results;
        }
    }

    private class SliceQueryRunner implements Runnable {

        final KeySliceQuery kq;
        final CountDownLatch doneSignal;
        final AtomicInteger failureCount;
        final Object[] resultArray;
        final int resultPosition;

        private SliceQueryRunner(KeySliceQuery kq, CountDownLatch doneSignal, AtomicInteger failureCount,
                                 Object[] resultArray, int resultPosition) {
            this.kq = kq;
            this.doneSignal = doneSignal;
            this.failureCount = failureCount;
            this.resultArray = resultArray;
            this.resultPosition = resultPosition;
        }

        @Override
        public void run() {
            try {
                List<Entry> result;
                result = edgeStoreQuery(kq);
                resultArray[resultPosition] = result;
            } catch (Exception e) {
                failureCount.incrementAndGet();
                log.warn("Individual query in multi-transaction failed: ", e);
            } finally {
                doneSignal.countDown();
            }
        }
    }

    public KeyIterator edgeStoreKeys(final SliceQuery sliceQuery) {
        if (!storeFeatures.hasScan())
            throw new UnsupportedOperationException("The configured storage backend does not support global graph operations - use Faunus instead");

        return executeRead(new Callable<KeyIterator>() {
            @Override
            public KeyIterator call() throws Exception {
                return (storeFeatures.isKeyOrdered())
                        ? edgeStore.getKeys(new KeyRangeQuery(EDGESTORE_MIN_KEY, EDGESTORE_MAX_KEY, sliceQuery), storeTx)
                        : edgeStore.getKeys(sliceQuery, storeTx);
            }

            @Override
            public String toString() {
                return "EdgeStoreKeys";
            }
        });
    }

    public KeyIterator edgeStoreKeys(final KeyRangeQuery range) {
        Preconditions.checkArgument(storeFeatures.hasOrderedScan(), "The configured storage backend does not support ordered scans");

        return executeRead(new Callable<KeyIterator>() {
            @Override
            public KeyIterator call() throws Exception {
                return edgeStore.getKeys(range, storeTx);
            }

            @Override
            public String toString() {
                return "EdgeStoreKeys";
            }
        });
    }

    public EntryList indexQuery(final KeySliceQuery query) {
        return executeRead(new Callable<EntryList>() {
            @Override
            public EntryList call() throws Exception {
                return cacheEnabled?indexStore.getSlice(query, storeTx):
                                    indexStore.getSliceNoCache(query, storeTx);
            }

            @Override
            public String toString() {
                return "VertexIndexQuery";
            }
        });

    }


    public Stream<String> indexQuery(final String index, final IndexQuery query) {
        final IndexTransaction indexTx = getIndexTransaction(index);
        return executeRead(new Callable<Stream<String>>() {
            @Override
            public Stream<String> call() throws Exception {
                return indexTx.queryStream(query);
            }

            @Override
            public String toString() {
                return "IndexQuery";
            }
        });
    }

    public Long indexQueryCount(final String index, final IndexQuery query) {
        final IndexTransaction indexTx = getIndexTransaction(index);
        return executeRead(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return indexTx.queryCount(query);
            }

            @Override
            public String toString() {
                return "indexQueryCount";
            }
        });

    }

    public Stream<RawQuery.Result<String>> rawQuery(final String index, final RawQuery query) {
        final IndexTransaction indexTx = getIndexTransaction(index);
        return executeRead(new Callable<Stream<RawQuery.Result<String>>>() {
            @Override
            public Stream<RawQuery.Result<String>> call() throws Exception {
                return indexTx.queryStream(query);
            }

            @Override
            public String toString() {
                return "RawQuery";
            }
        });
    }

    private static class TotalsCallable implements Callable<Long> {
    	private final RawQuery query;
    	private final IndexTransaction indexTx;

    	public TotalsCallable(final RawQuery query, final IndexTransaction indexTx) {
    		this.query = query;
    		this.indexTx = indexTx;
    	}

        @Override
        public Long call() throws Exception {
            return indexTx.totals(this.query);
        }

        @Override
        public String toString() {
            return "Totals";
        }
    }

    public Long totals(final String index, final RawQuery query) {
        final IndexTransaction indexTx = getIndexTransaction(index);
        return executeRead(new TotalsCallable(query, indexTx));
    }


    private <V> V executeRead(Callable<V> exe) throws JanusGraphException {
        try {
            return BackendOperation.execute(exe, maxReadTime);
        } catch (JanusGraphException e) {
            // support traversal interruption
            // TODO: Refactor to allow direct propagation of underlying interrupt exception
            if (Thread.interrupted()) throw new TraversalInterruptedException();
            throw e;
        }
    }

}
