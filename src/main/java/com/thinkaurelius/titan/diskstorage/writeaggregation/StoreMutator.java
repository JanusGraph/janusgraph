package com.thinkaurelius.titan.diskstorage.writeaggregation;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStore;

import java.nio.ByteBuffer;
import java.util.List;

public interface StoreMutator {

    /**
     * Applies the specified insertion and deletion mutations on the edge store to the provided key.
     * Both, the list of additions or deletions, may be empty or NULL if there is nothing to be added and/or deleted.
     *
     * @param key Key
     * @param additions List of entries (column + value) to be added
     * @param deletions List of columns to be removed
     */
	public void mutateEdges(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions);

    /**
     * Applies the specified insertion and deletion mutations on the property index to the provided key.
     * Both, the list of additions or deletions, may be empty or NULL if there is nothing to be added and/or deleted.
     *
     * @param key Key
     * @param additions List of entries (column + value) to be added
     * @param deletions List of columns to be removed
     */
    public void mutateIndex(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions);

    /**
     * Acquires a lock for the key-column pair on the edge store which ensures that nobody else can take a lock on that
     * respective entry for the duration of this lock (but somebody could potentially still overwrite
     * the key-value entry without taking a lock).
     * The expectedValue defines the value expected to match the value at the time the lock is acquired (or null if it is expected
     * that the key-column pair does not exist).
     *
     * If this method is called multiple times with the same key-column pair in the same transaction, all but the first invocation are ignored.
     *
     * The lock has to be released when the transaction closes (commits or aborts).
     *
     * @param key Key on which to lock
     * @param column Column the column on which to lock
     * @param expectedValue The expected value for the specified key-column pair on which to lock. Null if it is expected that the pair does not exist
     */
    public void acquireEdgeLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue);

    /**
     * Acquires a lock for the key-column pair on the property index which ensures that nobody else can take a lock on that
     * respective entry for the duration of this lock (but somebody could potentially still overwrite
     * the key-value entry without taking a lock).
     * The expectedValue defines the value expected to match the value at the time the lock is acquired (or null if it is expected
     * that the key-column pair does not exist).
     *
     * If this method is called multiple times with the same key-column pair in the same transaction, all but the first invocation are ignored.
     *
     * The lock has to be released when the transaction closes (commits or aborts).
     *
     * @param key Key on which to lock
     * @param column Column the column on which to lock
     * @param expectedValue The expected value for the specified key-column pair on which to lock. Null if it is expected that the pair does not exist
     */
    public void acquireIndexLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue);


    /**
     * Persists any mutation that is currently buffered.
     *
     */
	public void flush();
	
}
