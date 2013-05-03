package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.diskstorage.indexing.IndexTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Bundles all transaction handles from the various backend systems and provides a proxy for some of their
 * methods for convenience.
 * Also increases robustness of read call by attempting read calls multiple times on failure.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BackendTransaction implements TransactionHandle {

    private static final Logger log =
            LoggerFactory.getLogger(BackendTransaction.class);

    private final StoreTransaction storeTx;
    private final KeyColumnValueStore edgeStore;
    private final KeyColumnValueStore vertexIndexStore;
    private final KeyColumnValueStore edgeIndexStore;

    private final int maxReadRetryAttempts;
    private final int retryStorageWaitTime;

    private final Map<String,IndexTransaction> indexTx;

    public BackendTransaction(StoreTransaction storeTx, KeyColumnValueStore edgeStore,
                              KeyColumnValueStore vertexIndexStore, KeyColumnValueStore edgeIndexStore,
                              int maxReadRetryAttempts, int retryStorageWaitTime,
                              Map<String, IndexTransaction> indexTx) {
        this.storeTx = storeTx;
        this.edgeStore = edgeStore;
        this.vertexIndexStore = vertexIndexStore;
        this.edgeIndexStore = edgeIndexStore;
        this.maxReadRetryAttempts = maxReadRetryAttempts;
        this.retryStorageWaitTime = retryStorageWaitTime;
        this.indexTx = indexTx;
    }

    public StoreTransaction getStoreTransactionHandle() {
        return storeTx;
    }

    public IndexTransaction getIndexTransactionHandle(String index) {
        Preconditions.checkArgument(StringUtils.isNotBlank(index));
        IndexTransaction itx = indexTx.get(index);
        Preconditions.checkNotNull(itx,"Unknown index: " + index);
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
    public void mutateEdges(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions) throws StorageException {
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
    public void mutateVertexIndex(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions) throws StorageException {
        vertexIndexStore.mutate(key, additions, deletions, storeTx);
    }

    public void mutateEdgeIndex(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions) throws StorageException {
        edgeIndexStore.mutate(key, additions, deletions, storeTx);
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
     * @param expectedValue The expected value for the specified key-column pair on which to lock. Null if it is expected that the pair does not exist
     */
    public void acquireEdgeLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue) throws StorageException {
        edgeStore.acquireLock(key, column, expectedValue, storeTx);
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
    public void acquireVertexIndexLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue) throws StorageException {
        vertexIndexStore.acquireLock(key, column, expectedValue, storeTx);
    }

    /* ###################################################
            Convenience Read Methods
     */

    public List<Entry> edgeStoreQuery(final KeySliceQuery query) {
        return executeRead(new Callable<List<Entry>>() {
            @Override
            public List<Entry> call() throws Exception {
                return edgeStore.getSlice(query,storeTx);
            }
        });
    }

    public boolean edgeStoreContainsKey(final ByteBuffer key)  {
        return executeRead(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return edgeStore.containsKey(key,storeTx);
            }
        });
    }

    public RecordIterator<ByteBuffer> edgeStoreKeys() {
        return executeRead(new Callable<RecordIterator<ByteBuffer>>() {
            @Override
            public RecordIterator<ByteBuffer> call() throws Exception {
                return edgeStore.getKeys(storeTx);
            }
        });
    }

    public List<Entry> vertexIndexQuery(final KeySliceQuery query) {
        return executeRead(new Callable<List<Entry>>() {
            @Override
            public List<Entry> call() throws Exception {
                return vertexIndexStore.getSlice(query,storeTx);
            }
        });

    }

    public List<Entry> edgeIndexQuery(final KeySliceQuery query) {
        return executeRead(new Callable<List<Entry>>() {
            @Override
            public List<Entry> call() throws Exception {
                return edgeIndexStore.getSlice(query,storeTx);
            }
        });
    }

    public List<String> indexQuery(String index, final IndexQuery query) {
        final IndexTransaction indexTx = getIndexTransactionHandle(index);
        return executeRead(new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
                return indexTx.query(query);
            }
        });
    }


    private final<V> V executeRead(Callable<V> exe) throws TitanException {
        for (int readAttempt = 0; readAttempt < maxReadRetryAttempts; readAttempt++) {
            try {
                return exe.call();
            } catch (StorageException e) {
                if (e instanceof TemporaryStorageException) {
                    if (readAttempt < maxReadRetryAttempts - 1) temporaryStorageException(e,retryStorageWaitTime);
                    else throw readException(e, maxReadRetryAttempts);
                } else throw readException(e,0);
            } catch (Exception e) {
                throw readException(e,0);
            }
        }
        throw new AssertionError("Invalid state");
    }

    private final static TitanException readException(Exception e, int attempts) {
        if (attempts == 0)
            return new TitanException("Could not read from storage", e);
        else
            return new TitanException("Could not read from storage after " + attempts + " attempts", e);
    }

    public final static void temporaryStorageException(Throwable e, int retryStorageWaitTime) {
        Preconditions.checkArgument(e instanceof TemporaryStorageException);
        log.info("Temporary exception in storage backend. Attempting retry in {} ms. {}", retryStorageWaitTime, e);
        //Wait before retry
        if (retryStorageWaitTime > 0) {
            try {
                Thread.sleep(retryStorageWaitTime);
            } catch (InterruptedException r) {
                throw new TitanException("Interrupted while waiting to retry failed storage operation", e);
            }
        }
    }

}
