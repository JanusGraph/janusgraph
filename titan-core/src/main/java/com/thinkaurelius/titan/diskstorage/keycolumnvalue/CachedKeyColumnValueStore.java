package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * TODO: generalize cache to check for subsumption instead of equality of KeySliceQuery
 * and allow having a limit
 *
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class CachedKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger log = LoggerFactory.getLogger(CachedKeyColumnValueStore.class);

    private static final long DEFAULT_CACHE_SIZE = 100000;

    private final KeyColumnValueStore store;
    private final Cache<KeySliceQuery,List<Entry>> cache;

    public CachedKeyColumnValueStore(final KeyColumnValueStore store) {
        this(store,DEFAULT_CACHE_SIZE);
    }

    public CachedKeyColumnValueStore(final KeyColumnValueStore store, long cacheSize) {
        Preconditions.checkNotNull(store);
        this.store=store;
        this.cache = CacheBuilder.newBuilder().weigher(new Weigher<KeySliceQuery, List<Entry>>() {
            @Override
            public int weigh(KeySliceQuery q, List<Entry> r) {
                return 2 + r.size();
            }
        }).maximumWeight(cacheSize)
        .build();
    }

    @Override
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {
        return store.containsKey(key,txh);
    }

    @Override
    public List<Entry> getSlice(final KeySliceQuery query, final StoreTransaction txh) throws StorageException {
        if (query.isStatic() && !query.hasLimit()) {
            try {
                log.debug("Attempting to retrieve query from cache");
                return cache.get(query,new Callable<List<Entry>>() {
                    @Override
                    public List<Entry> call() throws StorageException {
                        log.debug("Cache miss");
                        return store.getSlice(query,txh);
                    }
                });
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause!=null && cause instanceof StorageException) {
                    throw (StorageException)cause;
                } else {
                    throw new TemporaryStorageException("Exception while accessing cache",e);
                }
            }
        } else {
            return store.getSlice(query,txh);
        }
    }

    @Override
    public ByteBuffer get(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        return store.get(key,column,txh);
    }

    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        return store.containsKeyColumn(key,column,txh);
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransaction txh) throws StorageException {
        store.mutate(key,additions,deletions,txh);
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue, StoreTransaction txh) throws StorageException {
        store.acquireLock(key,column,expectedValue,txh);
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction txh) throws StorageException {
        return store.getKeys(txh);
    }

    @Override
    public ByteBuffer[] getLocalKeyPartition() throws StorageException {
        return store.getLocalKeyPartition();
    }

    @Override
    public String getName() {
        return store.getName();
    }

    @Override
    public void close() throws StorageException {
        store.close();
    }
}
