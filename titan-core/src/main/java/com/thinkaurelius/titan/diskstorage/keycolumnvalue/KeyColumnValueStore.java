package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;

import java.util.List;
import java.util.Map;

/**
 * Interface to a data store that has a BigTable like representation of its data. In other words, the data store is comprised of a set of rows
 * each of which is uniquely identified by a key. Each row is composed of a column-value pairs. For a given key, a subset of the column-value
 * pairs that fall within a column interval can be quickly retrieved.
 * <p/>
 * This interface provides methods for retrieving and mutating the data.
 * <p/>
 * In this generic representation keys, columns and values are represented as ByteBuffers.
 * <p/>
 * See {@linktourl http://en.wikipedia.org/wiki/BigTable}
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public interface KeyColumnValueStore {

    public static final List<Entry> NO_ADDITIONS = ImmutableList.of();
    public static final List<StaticBuffer> NO_DELETIONS = ImmutableList.of();

    /**
     * Retrieves the list of entries (i.e. column-value pairs) for a specified query.
     *
     * @param query Query to get results for
     * @param txh   Transaction
     * @return List of entries up to a maximum of "limit" entries
     * @throws com.thinkaurelius.titan.diskstorage.BackendException when columnEnd < columnStart
     * @see KeySliceQuery
     */
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException;

    /**
     * Retrieves the list of entries (i.e. column-value pairs) as specified by the given {@link SliceQuery} for all
     * of the given keys together.
     *
     * @param keys  List of keys
     * @param query Slicequery specifying matching entries
     * @param txh   Transaction
     * @return The result of the query for each of the given keys as a map from the key to the list of result entries.
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     */
    public Map<StaticBuffer,EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException;

    /**
     * Verifies acquisition of locks {@code txh} from previous calls to
     * {@link #acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)}
     * , then writes supplied {@code additions} and/or {@code deletions} to
     * {@code key} in the underlying data store. Deletions are applied strictly
     * before additions. In other words, if both an addition and deletion are
     * supplied for the same column, then the column will first be deleted and
     * then the supplied Entry for the column will be added.
     * <p/>
     * <p/>
     * <p/>
     * Implementations which don't support locking should skip the initial lock
     * verification step but otherwise behave as described above.
     *
     * @param key       the key under which the columns in {@code additions} and
     *                  {@code deletions} will be written
     * @param additions the list of Entry instances representing column-value pairs to
     *                  create under {@code key}, or null to add no column-value pairs
     * @param deletions the list of columns to delete from {@code key}, or null to
     *                  delete no columns
     * @param txh       the transaction to use
     * @throws com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException if locking is supported by the implementation and at least
     *                          one lock acquisition attempted by
     *                          {@link #acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)}
     *                          has failed
     */
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException;

    /**
     * Attempts to claim a lock on the value at the specified {@code key} and
     * {@code column} pair. These locks are discretionary.
     * <p/>
     * <p/>
     * <p/>
     * If locking fails, implementations of this method may, but are not
     * required to, throw {@link com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException}.
     * This method is not required
     * to determine whether locking actually succeeded and may return without
     * throwing an exception even when the lock can't be acquired. Lock
     * acquisition is only only guaranteed to be verified by the first call to
     * {@link #mutate(StaticBuffer, List, List, StoreTransaction)} on any given
     * {@code txh}.
     * <p/>
     * <p/>
     * <p/>
     * The {@code expectedValue} must match the actual value present at the
     * {@code key} and {@code column} pair. If the true value does not match the
     * {@code expectedValue}, the lock attempt fails and
     * {@code LockingException} is thrown. This method may check
     * {@code expectedValue}. The {@code mutate()} mutate is required to check
     * it.
     * <p/>
     * <p/>
     * <p/>
     * When this method is called multiple times on the same {@code key},
     * {@code column}, and {@code txh}, calls after the first have no effect.
     * <p/>
     * <p/>
     * <p/>
     * Locks acquired by this method must be automatically released on
     * transaction {@code commit()} or {@code rollback()}.
     * <p/>
     * <p/>
     * <p/>
     * Implementations which don't support locking should throw
     * {@link UnsupportedOperationException}.
     *
     * @param key
     *            the key on which to lock
     * @param column
     *            the column on which to lock
     * @param expectedValue
     *            the expected value for the specified key-column pair on which
     *            to lock (null means the pair must have no value)
     * @param txh
     *            the transaction to use
     * @throws com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException
     *             the lock could not be acquired due to contention with other
     *             transactions or a locking-specific storage problem
     */
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException;

    /**
     * Returns a {@link KeyIterator} over all keys that fall within the key-range specified by the given query and have one or more columns matching the column-range.
     * Calling {@link KeyIterator#getEntries()} returns the list of all entries that match the column-range specified by the given query.
     * <p/>
     * This method is only supported by stores which keep keys in byte-order.
     *
     * @param query
     * @param txh
     * @return
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     */
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException;

    /**
     * Returns a {@link KeyIterator} over all keys in the store that have one or more columns matching the column-range. Calling {@link KeyIterator#getEntries()}
     * returns the list of all entries that match the column-range specified by the given query.
     * <p/>
     * This method is only supported by stores which do not keep keys in byte-order.
     *
     * @param query
     * @param txh
     * @return
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     */
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException;
    // like current getKeys if column-slice is such that it queries for vertex state property

    /**
     * Returns the name of this store. Each store has a unique name which is used to open it.
     *
     * @return store name
     * @see KeyColumnValueStoreManager#openDatabase(String)
     */
    public String getName();

    /**
     * Closes this store
     *
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     */
    public void close() throws BackendException;


}
