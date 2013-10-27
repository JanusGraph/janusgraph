package com.thinkaurelius.titan.diskstorage.infinispan;

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
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InfinispanCacheStore implements CacheStore {

    private final String name;
    private final EmbeddedCacheManager manager;

    private final Cache<StaticBuffer,StaticBuffer> cache;

    public InfinispanCacheStore(String name, EmbeddedCacheManager manager) {
        this.name = name;
        this.manager = manager;
        cache = manager.getCache(name);
    }


    @Override
    public void replace(StaticBuffer key, StaticBuffer newValue, StaticBuffer oldValue, StoreTransaction txh) throws CacheUpdateException {
        if (oldValue==null) cache.replace(key,newValue);
        else cache.replace(key,oldValue,newValue);
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
            public boolean hasNext() {
                return keys.hasNext();
            }

            @Override
            public KeyValueEntry next() {
                StaticBuffer key = keys.next();
                return new KeyValueEntry(key, cache.get(key));
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
    public void clearStore() {
        cache.clear();
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
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        //Unsupported, or supported through transactions
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void close() throws StorageException {
        //Do nothing
    }
}
