package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;

public interface CacheStore extends KeyValueStore {
    /**
     * Sets the value associated with key "key" to "value" if the current value associated with this key is "oldValue", otherwise
     * it throws an {@link CacheUpdateException}.
     * <p/>
     * *Warning*: When "null" value is used as "oldValue" this method acts as putIfAbsent
     *
     * @param key      The key to update.
     * @param newValue The value to replace current with.
     * @param oldValue The current value for the given key.
     * @param txh      The transaction context for the operation.
     * @throws CacheUpdateException If "oldValue" doesn't match currently find in the cache.
     */
    public void replace(StaticBuffer key, StaticBuffer newValue, StaticBuffer oldValue, StoreTransaction txh) throws StorageException;

    /**
     * Returns an iterator over all keys in this store that match the given KeySelector. The keys may be
     * ordered but not necessarily.
     *
     * @param selector The selector to use for key filtering.
     * @param txh      The transaction context for the operation.
     * @return An iterator over all keys in this store.
     */
    public RecordIterator<KeyValueEntry> getKeys(KeySelector selector, StoreTransaction txh) throws StorageException;

    /**
     * Remove all of the items from this store.
     */
    public void clearStore();
}
