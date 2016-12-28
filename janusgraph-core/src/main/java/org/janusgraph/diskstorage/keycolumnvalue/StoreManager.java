package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;

import java.util.List;

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
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException;

    /**
     * Closes the Storage Manager and all databases that have been opened.
     */
    public void close() throws BackendException;


    /**
     * Deletes and clears all database in this storage manager.
     * <p/>
     * ATTENTION: Invoking this method will delete ALL your data!!
     */
    public void clearStorage() throws BackendException;


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

    /**
     * Returns {@code KeyRange}s locally hosted on this machine. The start of
     * each {@code KeyRange} is inclusive. The end is exclusive. The start and
     * end must each be at least 4 bytes in length.
     *
     * @return A list of local key ranges
     * @throws UnsupportedOperationException
     *             if the underlying store does not support this operation.
     *             Check {@link StoreFeatures#hasLocalKeyPartition()} first.
     */
    public List<KeyRange> getLocalKeyPartition() throws BackendException;

}
