package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;

import java.util.Map;

/**
 * KeyColumnValueStoreManager provides the persistence context to the graph database storage backend.
 * <p/>
 * A KeyColumnValueStoreManager provides transaction handles across multiple data stores that
 * are managed by this KeyColumnValueStoreManager.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public interface KeyColumnValueStoreManager extends StoreManager {


    /**
     * Opens an ordered database by the given name. If the database does not exist, it is
     * created. If it has already been opened, the existing handle is returned.
     *
     * @param name Name of database
     * @return Database Handle
     * @throws com.thinkaurelius.titan.diskstorage.StorageException
     *
     */
    public KeyColumnValueStore openDatabase(String name) throws StorageException;


    /**
     * Executes multiple mutations at once. For each store (identified by a string name) there is a map of (key,mutation) pairs
     * that specifies all the mutations to execute against the particular store for that key.
     *
     * This is an optional operation. Check {@link #getFeatures()} if it is supported by a particular implementation.
     *
     * @param mutations
     * @param txh
     * @throws StorageException
     */
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException;


}
