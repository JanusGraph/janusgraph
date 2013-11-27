package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.StorageException;

/**
 * Generic interface to a backend storage engine.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface StoreManager {

    /**
     * Returns a transaction handle for a new transaction according to the given configuration.
     *
     * @return New Transaction Handle
     */
    public StoreTransaction beginTransaction(StoreTxConfig config) throws StorageException;

    /**
     * Closes the Storage Manager and all databases that have been opened.
     */
    public void close() throws StorageException;


    /**
     * Deletes and clears all database in this storage manager.
     * <p/>
     * ATTENTION: Invoking this method will delete ALL your data!!
     */
    public void clearStorage() throws StorageException;


    /**
     * Returns the features supported by this storage manager
     *
     * @return The supported features of this storage manager
     * @see StoreFeatures
     */
    public StoreFeatures getFeatures();

    /**
     * Return an identifier for the StoreManager. Two managers with the same
     * name would open databases that read and write the same underlying data;
     * two store managers with different names should be, for data read/write
     * purposes, completely isolated from each other.
     * <p/>
     * Examples:
     * <ul>
     * <li>Cassandra keyspace</li>
     * <li>HBase tablename</li>
     * <li>InMemoryStore heap address (i.e. default toString()).</li>
     * </ul>
     *
     * @return Name for this StoreManager
     */
    public String getName();
}
