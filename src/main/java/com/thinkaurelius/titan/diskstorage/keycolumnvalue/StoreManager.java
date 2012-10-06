package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.StorageException;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface StoreManager {

    /**
     * Returns a transaction handle for a new transaction.
     *
     * @return New Transaction Hanlde
     */
    public StoreTransactionHandle beginTransaction(ConsistencyLevel consistencyLevel) throws StorageException;

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

}
