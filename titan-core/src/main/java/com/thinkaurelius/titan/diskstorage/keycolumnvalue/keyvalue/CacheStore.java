package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface CacheStore extends KeyValueStore {
    /**
     * Sets the value associated with key "key" to "value" if the current value associated with this key is "oldValue", otherwise
     * it throws an {@link CacheUpdateException}.
     *
     * @param key
     * @param value
     * @param oldValue
     * @param txh
     * @throws StorageException
     */
    public void replace(StaticBuffer key, StaticBuffer value, StaticBuffer oldValue, StoreTransaction txh) throws StorageException;

    /**
     * Returns an iterator over all keys in this store that match the given KeySelector. The keys may be
     * ordered but not necessarily.
     *
     * @return An iterator over all keys in this store.
     */
    public RecordIterator<KeyValueEntry> getKeys(KeySelector selector, StoreTransaction txh) throws StorageException;
}
