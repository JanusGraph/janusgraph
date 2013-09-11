package com.thinkaurelius.titan.diskstorage.ehcache;

import java.util.Iterator;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.CacheStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.CacheUpdateException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration;

public class EhCacheKeyValueStore implements CacheStore {
    private final CacheManager manager; // required for close operation
    private final Ehcache cache;

    public EhCacheKeyValueStore(String name, CacheManager manager) {
        CacheConfiguration config = new CacheConfiguration(name, 0)
                                            .eternal(true) // LOCAL persistence is supported only by Enterprise
                                            .persistence(new PersistenceConfiguration()
                                                                .strategy(PersistenceConfiguration.Strategy.NONE));

        this.manager = manager;
        this.cache = manager.addCacheIfAbsent(new Cache(config));
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction tx) throws StorageException {
        cache.put(new Element(key, value));
    }

    @Override
    public void replace(StaticBuffer key, StaticBuffer newValue, StaticBuffer oldValue, StoreTransaction txh) throws StorageException {
        if (!cache.replace(new Element(key, oldValue), new Element(key, newValue)))
            throw new CacheUpdateException(String.format("Key: %s, has current value different from %s, can't replace with %s.", key, oldValue, newValue));
    }

    @Override
    public RecordIterator<KeyValueEntry> getKeys(final KeySelector selector, StoreTransaction txh) throws StorageException {
        return new RecordIterator<KeyValueEntry>() {
            private Iterator keys = Iterators.filter(cache.getKeys().iterator(), new Predicate() {
                @Override
                public boolean apply(@Nullable Object input) {
                    return input != null && selector.include((StaticBuffer) input) && !selector.reachedLimit();
                }
            });

            @Override

            public boolean hasNext() throws StorageException {
                return keys.hasNext();
            }

            @Override
            public KeyValueEntry next() throws StorageException {
                StaticBuffer key = (StaticBuffer) keys.next();
                return new KeyValueEntry(key, (StaticBuffer) cache.get(key).getObjectValue());
            }

            @Override
            public void close() throws StorageException {
                // Close is not supported
            }
        };
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws StorageException {
        cache.remove(key);
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws StorageException {
        return (StaticBuffer) cache.get(key).getObjectValue();
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        return cache.isKeyInCache(key);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        // This store doesn't support locking
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
        manager.removeCache(getName());
    }
}
