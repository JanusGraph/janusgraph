package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wraps a {@link KeyColumnValueStore} and caches KeySliceQuery results which are marked <i>static</i> and hence do not change.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @see SliceQuery
 *      <p/>
 *      TODO: generalize cache to check for subsumption instead of equality of KeySliceQuery and allow having a limit
 */

public class CachedKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger log = LoggerFactory.getLogger(CachedKeyColumnValueStore.class);

    private static final long DEFAULT_CACHE_SIZE = 100000;

    private final KeyColumnValueStore store;
    private final Cache<KeySliceQuery, List<Entry>> cache;

    private final AtomicLong cacheRetrieval = new AtomicLong(0);
    private final AtomicLong cacheMiss = new AtomicLong(0);

    public CachedKeyColumnValueStore(final KeyColumnValueStore store) {
        this(store, DEFAULT_CACHE_SIZE);
    }

    public CachedKeyColumnValueStore(final KeyColumnValueStore store, long cacheSize) {
        Preconditions.checkNotNull(store);
        this.store = store;
        this.cache = CacheBuilder.newBuilder().weigher(new Weigher<KeySliceQuery, List<Entry>>() {
            @Override
            public int weigh(KeySliceQuery q, List<Entry> r) {
                return 2 + r.size();
            }
        }).maximumWeight(cacheSize)
                .build();
    }

    public final double getCacheHitRatio() {
        if (cacheRetrieval.get() == 0) return Double.NaN;
        else return (cacheRetrieval.get() - cacheMiss.get()) * 1.0 / cacheRetrieval.get();
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        return store.containsKey(key, txh);
    }

    @Override
    public List<Entry> getSlice(final KeySliceQuery query, final StoreTransaction txh) throws StorageException {
        if (query.isStatic() && !query.hasLimit()) {
            try {
                if (log.isDebugEnabled())
                    log.debug("Cache Retrieval on " + store.getName() + ". Attempts: {} | Misses: {}", cacheRetrieval.get(), cacheMiss.get());
                cacheRetrieval.incrementAndGet();
                List<Entry> result = cache.get(query, new Callable<List<Entry>>() {
                    @Override
                    public List<Entry> call() throws StorageException {
                        cacheMiss.incrementAndGet();
                        return store.getSlice(query, txh);
                    }
                });
                if (result.isEmpty()) cache.invalidate(query);
                return result;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause != null && cause instanceof StorageException) {
                    throw (StorageException) cause;
                } else {
                    throw new TemporaryStorageException("Exception while accessing cache", e);
                }
            }
        } else {
            return store.getSlice(query, txh);
        }
    }

    @Override
    public List<List<Entry>> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
        if (query.isStatic() && !query.hasLimit()) {
            List<List<Entry>> results = new ArrayList<List<Entry>>(keys.size());
            for (StaticBuffer key : keys) {
                results.add(getSlice(new KeySliceQuery(key, query), txh));
            }
            return results;
        } else {
            return store.getSlice(keys, query, txh);
        }
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws StorageException {
        store.mutate(key, additions, deletions, txh);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        store.acquireLock(key, column, expectedValue, txh);
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery keyQuery, StoreTransaction txh) throws StorageException {
        return store.getKeys(keyQuery, txh);
    }

    @Override
    public KeyIterator getKeys(SliceQuery columnQuery, StoreTransaction txh) throws StorageException {
        return store.getKeys(columnQuery, txh);
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
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
