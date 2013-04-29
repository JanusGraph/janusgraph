package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.diskstorage.StorageException;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Interface to a data store that has a BigTable like representation of its data. In other words, the data store is comprised of a set of rows
 * each of which is uniquely identified by a key. Each row is composed of a column-value pairs. For a given key, a subset of the column-value
 * pairs that fall within a column interval can be quickly retrieved.
 *
 * This interface provides methods for retrieving and mutating the data.
 *
 * In this generic representation keys, columns and values are represented as ByteBuffers.
 *
 * See {@linktourl http://en.wikipedia.org/wiki/BigTable}
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 *
 */
public interface KeyColumnValueStore {

    public static final List<Entry> NO_ADDITIONS = ImmutableList.of();
    public static final List<ByteBuffer> NO_DELETIONS = ImmutableList.of();

    /**
     * Returns true if the specified key exists in the store, i.e. there is at least one column-value
     * pair for the key.
     *
     * @param key Key
     * @param txh Transaction
     * @return TRUE, if key has at least one column-value pair, else FALSE
     */
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException;

    /**
     * Retrieves the list of entries (i.e. column-value pairs) for a specified query.
     *
     * @param query       Query to get results for
     * @param txh         Transaction
     * @return List of entries up to a maximum of "limit" entries
     * @throws StorageException when columnEnd < columnStart as determined in
     *                          {@link com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil#isSmallerThan(ByteBuffer, ByteBuffer)}
     * @see KeySliceQuery
     */
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException;

    //TODO: for declarative query optimization
    //public List<List<Entry>> getSlice(List<ByteBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException;

    /**
     * Applies the specified insertion and deletion mutations to the provided key.
     * Both, the list of additions or deletions, may be empty or NULL if there is nothing to be added and/or deleted.
     *
     * @param key       Key
     * @param additions List of entries (column + value) to be added (possibly empty but not NULL)
     * @param deletions List of columns to be removed (possibly empty but not NULL)
     * @param txh       Transaction under which to execute the operation
     */
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransaction txh) throws StorageException;

    /**
     * Acquires a lock for the key-column pair which ensures that nobody else can take a lock on that
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
     * @param txh           Transaction
     */
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue, StoreTransaction txh) throws StorageException;


    /**
     * Returns an iterator over all keys in this store. The keys may be
     * ordered but not necessarily.
     *
     * @return An iterator over all keys in this store.
     * @throws UnsupportedOperationException if the underlying store does not support this operation. Check {@link StoreFeatures#supportsScan()} first.
     */
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction txh) throws StorageException;

    //TODO: for Fulgora, replace with these two: one for stores that maintain key order and those without according to StoreFeatures.isKeyOrdered()
//    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws StorageException;
//    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws StorageException; - like current getKeys if slice is empty, i.e. start==end


    /**
     * Returns an array that describes the key boundaries of the locally hosted partition of this store.
     * <p/>
     * The array has two entries: the first marks the lower bound for the keys stored locally (inclusive) and the other
     * marks the upper bound (exclusive).
     *
     * @return An array with two entries describing the locally hosted partition of this store.
     * @throws StorageException
     * @throws UnsupportedOperationException if the underlying store does not support this operation. Check {@link StoreFeatures#hasLocalKeyPartition()} first.
     */
    public ByteBuffer[] getLocalKeyPartition() throws StorageException;

    /**
     * Returns the name of this store. Each store has a unique name which is used to open it.
     *
     * @return
     * @see KeyColumnValueStoreManager#openDatabase(String)
     */
    public String getName();

    /**
     * Closes this store
     *
     * @throws StorageException
     */
    public void close() throws StorageException;


}
