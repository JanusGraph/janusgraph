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
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;

public class HazelcastCacheStore implements CacheStore {

    private static final String UPDATE_EXCEPTION_FORMAT = "Key: %s, has current value different from %s, can't replace with %s.";

    private final IMap<byte[], byte[]> cache;

    public HazelcastCacheStore(String name, HazelcastInstance manager) {
        this.cache = manager.getMap(name);
    }

    @Override
    public void replace(StaticBuffer key, StaticBuffer newValue, StaticBuffer oldValue, StoreTransaction txh) throws CacheUpdateException {
        byte[] rawKey = key.as(StaticArrayBuffer.ARRAY_FACTORY);

        try {
            byte[] rawNewValue = newValue.as(StaticArrayBuffer.ARRAY_FACTORY);

            // Hazelcast doesn't replace a value when old value was null
            // so we have to look and use putIfAbsent(new) if oldValue == null, otherwise use replace(old, new)
            cache.lock(rawKey);

            if (oldValue == null) {
                if (cache.putIfAbsent(rawKey, rawNewValue) != null)
                    throw new CacheUpdateException(String.format(UPDATE_EXCEPTION_FORMAT, key, oldValue, newValue));
            } else if (!cache.replace(rawKey, oldValue.as(StaticArrayBuffer.ARRAY_FACTORY), rawNewValue)) {
                throw new CacheUpdateException(String.format(UPDATE_EXCEPTION_FORMAT, key, oldValue, newValue));
            }
        } finally {
            cache.unlock(rawKey);
        }
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws StorageException {
        cache.remove(key.as(StaticArrayBuffer.ARRAY_FACTORY));
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws StorageException {
        byte[] value = cache.get(key.as(StaticArrayBuffer.ARRAY_FACTORY));
        return value == null ? null : new StaticByteBuffer(value);
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        return cache.containsKey(key.as(StaticArrayBuffer.ARRAY_FACTORY));
    }

    @Override
    public RecordIterator<KeyValueEntry> getKeys(final KeySelector selector, StoreTransaction txh) throws StorageException {
        final Iterator<byte[]> keys = Iterators.filter(cache.keySet().iterator(), new Predicate<byte[]>() {
            @Override
            public boolean apply(@Nullable byte[] key) {
                return selector.include(new StaticArrayBuffer(key)) && !selector.reachedLimit();
            }
        });

        return new RecordIterator<KeyValueEntry>() {
            @Override
            public boolean hasNext() {
                return keys.hasNext();
            }

            @Override
            public KeyValueEntry next() {
                byte[] key = keys.next();
                return new KeyValueEntry(new StaticArrayBuffer(key), new StaticArrayBuffer(cache.get(key)));
            }

            @Override
            public void close() {
                // nothing to do
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
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
    public void clearStore() {
        cache.clear();
    }

    @Override
    public void close() throws StorageException {
        cache.destroy();
    }
}
