package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.StorageException;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface StoreManager {

    /**
     * Returns a transaction handle for a new transaction.
     *
     * @return New Transaction Handle
     */
    public StoreTransaction beginTransaction(ConsistencyLevel consistencyLevel) throws StorageException;

    /**
     * Closes the Storage Manager and all databases that have been opened.
     */
    public void close() throws StorageException;


    /**
     * Deletes and clears all database in this storage manager.
     *
     * ATTENTION: Invoking this method will delete ALL your data!!
     */
    public void clearStorage() throws StorageException;


    /**
     * Returns the features supported by this storage manager
     *
     * @see StoreFeatures
     * @return The supported features of this storage manager
     */
    public StoreFeatures getFeatures();

    /**
     * Note: client is responsible of calling setTitanVersionToLatest() method when
     * upgrade process is complete.
     *
     * @return The version of Titan service which was used to access this storage last time,
     *         returns "null" if database was created by using Titan < 0.3.0.
     *
     */
    public String getLastSeenTitanVersion() throws StorageException;

    /**
     * Reset Titan Version to version provided by Constants.VERSION (possibly persistently).
     */
    public void setTitanVersionToLatest() throws StorageException;
}
