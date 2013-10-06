package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.thinkaurelius.titan.diskstorage.StorageException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface CacheStoreManager extends KeyValueStoreManager {
    @Override
    public CacheStore openDatabase(String name) throws StorageException;
}
