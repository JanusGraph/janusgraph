package com.thinkaurelius.titan.diskstorage.infinispan;

import java.util.Iterator;

import javax.annotation.Nullable;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;

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

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InfinispanCacheStore implements CacheStore {

    private final String shortName;
//    private final EmbeddedCacheManager manager;

    protected final Cache<StaticBuffer,StaticBuffer> cache;
    protected final TransactionManager tm;

    public InfinispanCacheStore(String fullName, String shortName, EmbeddedCacheManager manager) {
        this.shortName = shortName;
//        this.manager = manager;
        cache = manager.getCache(fullName);
        tm = cache.getAdvancedCache().getTransactionManager();
    }


    @Override
    public void replace(StaticBuffer key, StaticBuffer newValue, StaticBuffer oldValue, StoreTransaction txh) throws StorageException {        
        // TODO simplify this if possible without violating the method contract
        if (null == newValue) {
            if (!cache.remove(key, oldValue)) {
                throw new CacheUpdateException("key=" + key + " oldValue=" + oldValue + " newValue=" + newValue);
            }
        } else if (null == oldValue) {
            if (null != cache.putIfAbsent(key, newValue)) {
                throw new CacheUpdateException("key=" + key + " oldValue=" + oldValue + " newValue=" + newValue);
            }
        } else {
            assert null != newValue;
            assert null != oldValue;
            if (!cache.replace(key, oldValue, newValue)) {
                throw new CacheUpdateException("key=" + key + " oldValue=" + oldValue + " newValue=" + newValue);
            }
        }
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
    public synchronized void clearStore() {
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
        return shortName;
    }

    @Override
    public void close() throws StorageException {
        //Do nothing
    }
}
