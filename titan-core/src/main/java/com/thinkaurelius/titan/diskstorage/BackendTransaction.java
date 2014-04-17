package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.indexing.IndexEntry;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.diskstorage.indexing.IndexTransaction;
import com.thinkaurelius.titan.diskstorage.indexing.RawQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache.CacheTransaction;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.types.ExternalIndexType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bundles all transaction handles from the various backend systems and provides a proxy for some of their
 * methods for convenience.
 * Also increases robustness of read call by attempting read calls multiple times on failure.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BackendTransaction implements TransactionHandle, LoggableTransaction {

    private static final Logger log =
            LoggerFactory.getLogger(BackendTransaction.class);

    public static final int MIN_TASKS_TO_PARALLELIZE = 2;

    //Assumes 64 bit key length as specified in IDManager
    public static final StaticBuffer EDGESTORE_MIN_KEY = BufferUtil.zeroBuffer(8);
    public static final StaticBuffer EDGESTORE_MAX_KEY = BufferUtil.oneBuffer(8);

    private final CacheTransaction storeTx;
    private final TransactionHandleConfig txConfig;
    private final StoreFeatures storeFeatures;

    private final KeyColumnValueStore edgeStore;
    private final KeyColumnValueStore indexStore;

    private final int maxReadRetryAttempts;
    private final int retryStorageWaitTime;

    private final Executor threadPool;

    private final Map<String, IndexTransaction> indexTx;

    public BackendTransaction(CacheTransaction storeTx, TransactionHandleConfig txConfig,
                              StoreFeatures features, KeyColumnValueStore edgeStore, KeyColumnValueStore indexStore,
                              int maxReadRetryAttempts, int retryStorageWaitTime,
                              Map<String, IndexTransaction> indexTx, Executor threadPool) {
        this.storeTx = storeTx;
        this.txConfig = txConfig;
        this.storeFeatures = features;
        this.edgeStore = edgeStore;
        this.indexStore = indexStore;
        this.maxReadRetryAttempts = maxReadRetryAttempts;
        this.retryStorageWaitTime = retryStorageWaitTime;
        this.indexTx = indexTx;
        this.threadPool = threadPool;
    }

    public StoreTransaction getStoreTransactionHandle() {
        return storeTx;
    }

    public TransactionHandleConfig getConfiguration() {
        return txConfig;
    }

    public IndexTransaction getIndexTransactionHandle(String index) {
        Preconditions.checkArgument(StringUtils.isNotBlank(index));
        IndexTransaction itx = indexTx.get(index);
        Preconditions.checkNotNull(itx, "Unknown index: " + index);
        return itx;
    }

    @Override
    public void commit() throws StorageException {
        storeTx.commit();
        for (IndexTransaction itx : indexTx.values()) itx.commit();
    }

    @Override
    public void rollback() throws StorageException {
        storeTx.rollback();
        for (IndexTransaction itx : indexTx.values()) itx.rollback();
    }

    @Override
    public void flush() throws StorageException {
        storeTx.flush();
        for (IndexTransaction itx : indexTx.values()) itx.flush();
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
    public void mutateEdges(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions) throws StorageException {
        edgeStore.mutate(key, additions, deletions, storeTx);
    }

    /**
     * Applies the specified insertion and deletion mutations on the property index to the provided key.
     * Both, the list of additions or deletions, may be empty or NULL if there is nothing to be added and/or deleted.
     *
     * @param key       Key
     * @param additions List of entries (column + value) to be added
     * @param deletions List of columns to be removed
     */
    public void mutateIndex(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions) throws StorageException {
        indexStore.mutate(key, additions, deletions, storeTx);
    }

    /**
     * Acquires a lock for the key-column pair on the edge store which ensures that nobody else can take a lock on that
     * respective entry for the duration of this lock (but somebody could potentially still overwrite
     * the key-value entry without taking a lock).
     * The expectedValue defines the value expected to match the value at the time the lock is acquired (or null if it is expected
     * that the key-column pair does not exist).
     * <p/>
     * If this method is called multiple times with the same key-column pair in the same transaction, all but the first invocation are ignored.
     * <p/>
     * The lock has to be released when the transaction closes (commits or aborts).
     *
     * @param key           Key on which to lock
     * @param column        Column the column on which to lock
     */
    public void acquireEdgeLock(StaticBuffer key, StaticBuffer column) throws StorageException {
        edgeStore.acquireLock(key, column, null, storeTx);
    }

    public void acquireEdgeLock(StaticBuffer key, Entry entry) throws StorageException {
        edgeStore.acquireLock(key, entry.getColumnAs(StaticBuffer.STATIC_FACTORY), entry.getValueAs(StaticBuffer.STATIC_FACTORY), storeTx);
    }

    /**
     * Acquires a lock for the key-column pair on the property index which ensures that nobody else can take a lock on that
     * respective entry for the duration of this lock (but somebody could potentially still overwrite
     * the key-value entry without taking a lock).
     * The expectedValue defines the value expected to match the value at the time the lock is acquired (or null if it is expected
     * that the key-column pair does not exist).
     * <p/>
     * If this method is called multiple times with the same key-column pair in the same transaction, all but the first invocation are ignored.
     * <p/>
     * The lock has to be released when the transaction closes (commits or aborts).
     *
     * @param key           Key on which to lock
     * @param column        Column the column on which to lock
     * @param expectedValue The expected value for the specified key-column pair on which to lock. Null if it is expected that the pair does not exist
     */
    public void acquireIndexLock(StaticBuffer key, StaticBuffer column) throws StorageException {
        indexStore.acquireLock(key, column, null, storeTx);
    }

    public void acquireIndexLock(StaticBuffer key, Entry entry) throws StorageException {
        indexStore.acquireLock(key, entry.getColumnAs(StaticBuffer.STATIC_FACTORY), entry.getValueAs(StaticBuffer.STATIC_FACTORY), storeTx);
    }

    /* ###################################################
            Convenience Read Methods
     */

    public EntryList edgeStoreQuery(final KeySliceQuery query) {
        return executeRead(new Callable<EntryList>() {
            @Override
            public EntryList call() throws Exception {
                return edgeStore.getSlice(query, storeTx);
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
                    return edgeStore.getSlice(keys, query, storeTx);
                }

                @Override
                public String toString() {
                    return "MultiEdgeStoreQuery";
                }
            });
        } else {
            final Map<StaticBuffer,EntryList> results = new HashMap<StaticBuffer,EntryList>(keys.size());
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
                    throw new TitanException("Interrupted while waiting for multi-query to complete", e);
                }
                if (failureCount.get() > 0) {
                    throw new TitanException("Could not successfully complete multi-query. " + failureCount.get() + " individual queries failed.");
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
                if (maxReadRetryAttempts > 1)
                    result = edgeStoreQuery(kq);
                else //Premature optimization
                    result = edgeStore.getSlice(kq, storeTx);
                resultArray[resultPosition] = result;
            } catch (Exception e) {
                failureCount.incrementAndGet();
                log.warn("Individual query in multi-transaction failed: ", e);
            } finally {
                doneSignal.countDown();
            }
        }
    }

    //TODO: remove and also KeyColumnValueStore.containsKey
    public boolean edgeStoreContainsKey(final StaticBuffer key) {
        return executeRead(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return edgeStore.containsKey(key, storeTx);
            }

            @Override
            public String toString() {
                return "EdgeStoreContainsKey";
            }
        });
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
                return indexStore.getSlice(query, storeTx);
            }

            @Override
            public String toString() {
                return "VertexIndexQuery";
            }
        });

    }


    public List<String> indexQuery(final String index, final IndexQuery query) {
        final IndexTransaction indexTx = getIndexTransactionHandle(index);
        return executeRead(new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
                return indexTx.query(query);
            }

            @Override
            public String toString() {
                return "IndexQuery";
            }
        });
    }

    public Iterable<RawQuery.Result<String>> rawQuery(final String index, final RawQuery query) {
        final IndexTransaction indexTx = getIndexTransactionHandle(index);
        return executeRead(new Callable<Iterable<RawQuery.Result<String>>>() {
            @Override
            public Iterable<RawQuery.Result<String>> call() throws Exception {
                return indexTx.query(query);
            }

            @Override
            public String toString() {
                return "RawQuery";
            }
        });
    }


    private final <V> V executeRead(Callable<V> exe) throws TitanException {
        return BackendOperation.execute(exe, maxReadRetryAttempts, retryStorageWaitTime);
    }


}
