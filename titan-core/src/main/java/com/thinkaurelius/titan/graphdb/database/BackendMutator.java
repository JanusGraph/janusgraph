package com.thinkaurelius.titan.graphdb.database;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class BackendMutator {

    private final Backend backend;
    private final KeyColumnValueStore edgeStore;
    private final KeyColumnValueStore vertexIndexStore;
    private final BackendTransaction tx;
    private final StoreTransaction stx;

    public BackendMutator(final Backend backend, final TransactionHandle txh) {
        Preconditions.checkArgument(txh != null && txh instanceof BackendTransaction);
        Preconditions.checkNotNull(backend);
        this.backend = backend;
        this.edgeStore = backend.getEdgeStore();
        this.vertexIndexStore = backend.getVertexIndexStore();
        this.tx = (BackendTransaction) txh;
        this.stx = tx.getStoreTransactionHandle();
    }


    /**
     * Applies the specified insertion and deletion mutations on the edge store to the provided key.
     * Both, the list of additions or deletions, may be empty or NULL if there is nothing to be added and/or deleted.
     *
     * @param key       Key
     * @param additions List of entries (column + value) to be added
     * @param deletions List of columns to be removed
     */
    public void mutateEdges(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions) throws StorageException {
        edgeStore.mutate(key, additions, deletions, stx);
    }

    /**
     * Applies the specified insertion and deletion mutations on the property index to the provided key.
     * Both, the list of additions or deletions, may be empty or NULL if there is nothing to be added and/or deleted.
     *
     * @param key       Key
     * @param additions List of entries (column + value) to be added
     * @param deletions List of columns to be removed
     */
    public void mutateIndex(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions) throws StorageException {
        vertexIndexStore.mutate(key, additions, deletions, stx);
    }

    public void addExternalIndex(String index, String docid, String key, Object value) throws StorageException {
        tx.getIndexTransactionHandle(index).add(docid,key,value);
    }

    public void deleteExternalIndex(String index, String docid, String key) throws StorageException {
        tx.getIndexTransactionHandle(index).delete(docid,key);
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
        edgeStore.acquireLock(key, column, expectedValue, stx);
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
        vertexIndexStore.acquireLock(key, column, expectedValue, stx);
    }


    /**
     * Persists any mutation that is currently buffered.
     */
    public void flush() throws StorageException {
        stx.flush();
    }


}
