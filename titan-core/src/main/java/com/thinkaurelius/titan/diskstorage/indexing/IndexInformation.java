package com.thinkaurelius.titan.diskstorage.indexing;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface IndexInformation {

    /**
     * It is expected that this method is first called with each new key to inform the index of the expected type.
     *
     * @param store
     * @param key
     * @param dataType
     */
    public void register(String store, String key, Class<?> dataType, TransactionHandle tx) throws StorageException;

    public boolean covers(String store, Class<?> dataType, Relation relation);


}
