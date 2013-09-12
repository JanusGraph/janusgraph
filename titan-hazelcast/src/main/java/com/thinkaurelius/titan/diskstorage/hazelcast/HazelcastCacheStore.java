package com.thinkaurelius.titan.diskstorage.hazelcast;

import java.util.Iterator;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.CacheStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.CacheUpdateException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;

public class HazelcastCacheStore implements CacheStore {
    private final IMap<StaticBuffer, StaticBuffer> cache;

    public HazelcastCacheStore(String name, HazelcastInstance manager) {
        this.cache = manager.getMap(name);
    }

    @Override
    public void replace(StaticBuffer key, StaticBuffer newValue, StaticBuffer oldValue, StoreTransaction txh) throws StorageException {
        if (!cache.replace(key, oldValue, newValue))
            throw new CacheUpdateException(String.format("Key: %s, has current value different from %s, can't replace with %s.", key, oldValue, newValue));
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws StorageException {
        cache.remove(key);
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws StorageException {
        return cache.get(key);
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        return cache.containsKey(key);
    }

    @Override
    public RecordIterator<KeyValueEntry> getKeys(final KeySelector selector, StoreTransaction txh) throws StorageException {
        final Iterator<StaticBuffer> keys = Iterators.filter(cache.keySet().iterator(), new Predicate<StaticBuffer>() {
            @Override
            public boolean apply(@Nullable StaticBuffer key) {
                return selector.include(key) && !selector.reachedLimit();
            }
        });

        return new RecordIterator<KeyValueEntry>() {
            @Override
            public boolean hasNext() throws StorageException {
                return keys.hasNext();
            }

            @Override
            public KeyValueEntry next() throws StorageException {
                StaticBuffer key = keys.next();
                return new KeyValueEntry(key, cache.get(key));
            }

            @Override
            public void close() throws StorageException {
                // nothing to do
            }
        };
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        // not supported
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return cache.getName();
    }

    @Override
    public void close() throws StorageException {
        cache.destroy();
    }
}
