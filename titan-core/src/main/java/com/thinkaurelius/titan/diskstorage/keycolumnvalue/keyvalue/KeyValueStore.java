package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;

/**
 * Interface for a data store that represents data in the simple key->value data model where each key is uniquely
 * associated with a value. Keys and values are generic ByteBuffers.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public interface KeyValueStore {

    /**
     * Deletes the given key from the store.
     *
     * @param key
     * @param txh
     * @throws StorageException
     */
    public void delete(StaticBuffer key, StoreTransaction txh) throws StorageException;

    /**
     * Returns the value associated with the given key.
     *
     * @param key
     * @param txh
     * @return
     * @throws StorageException
     */
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws StorageException;

    /**
     * Returns true iff the store contains the given key, else false
     *
     * @param key
     * @param txh
     * @return
     * @throws StorageException
     */
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException;


    /**
     * Acquires a lock for the given key and expected value (null, if not value is expected).
     *
     * @param key
     * @param expectedValue
     * @param txh
     * @throws StorageException
     */
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException;

    /**
     * Returns the range of keys that are stored locally.
     *
     * This is an optional operation and only makes sense in the context of distributed stores.
     *
     * @return
     * @throws StorageException
     */
    public StaticBuffer[] getLocalKeyPartition() throws StorageException;

    /**
     * Returns the name of this store
     *
     * @return
     */
    public String getName();

    /**
     * Closes this store and releases its resources.
     *
     * @throws StorageException
     */
    public void close() throws StorageException;

}
