package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     */
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException;

    /**
     * Returns the value associated with the given key.
     *
     * @param key
     * @param txh
     * @return
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     */
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException;

    /**
     * Returns true iff the store contains the given key, else false
     *
     * @param key
     * @param txh
     * @return
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     */
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException;


    /**
     * Acquires a lock for the given key and expected value (null, if not value is expected).
     *
     * @param key
     * @param expectedValue
     * @param txh
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     */
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException;

    /**
     * Returns the name of this store
     *
     * @return
     */
    public String getName();

    /**
     * Closes this store and releases its resources.
     *
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     */
    public void close() throws BackendException;

}
