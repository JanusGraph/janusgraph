package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.BackendException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface CustomizeStoreKCVSManager extends KeyColumnValueStoreManager {

    /**
     * Opens a database against this {@link KeyColumnValueStoreManager} with the given
     * TTL in seconds.
     *
     * @param name Name of database
     * @param ttlInSeconds TTL for the entries in this {@link KeyColumnValueStore}
     * @return Database Handle
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     * @see KeyColumnValueStoreManager#openDatabase(String)
     *
     */
    public KeyColumnValueStore openDatabase(String name, int ttlInSeconds) throws BackendException;

}
