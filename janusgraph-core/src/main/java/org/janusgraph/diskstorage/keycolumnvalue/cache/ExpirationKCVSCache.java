// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.keycolumnvalue.cache;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.CacheMetricsAction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.janusgraph.util.datastructures.ByteSize.GUAVA_CACHE_ENTRY_SIZE;
import static org.janusgraph.util.datastructures.ByteSize.OBJECT_HEADER;
import static org.janusgraph.util.datastructures.ByteSize.OBJECT_REFERENCE;
import static org.janusgraph.util.datastructures.ByteSize.STATICARRAYBUFFER_RAW_SIZE;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ExpirationKCVSCache extends KCVSCache {

    //Weight estimation
    private static final int STATIC_ARRAY_BUFFER_SIZE = STATICARRAYBUFFER_RAW_SIZE + 10; // 10 = last number is average length
    private static final int KEY_QUERY_SIZE = OBJECT_HEADER + 4 + 1 + 3 * (OBJECT_REFERENCE + STATIC_ARRAY_BUFFER_SIZE); // object_size + int + boolean + 3 static buffers

    private static final int INVALIDATE_KEY_FRACTION_PENALTY = 1000;
    private static final int PENALTY_THRESHOLD = 5;

    private volatile CountDownLatch penaltyCountdown;

    private final Cache<KeySliceQuery,EntryList> cache;
    private final ConcurrentHashMap<StaticBuffer,Long> expiredKeys;

    private final long cacheTimeMS;
    private final long invalidationGracePeriodMS;
    private final CleanupThread cleanupThread;


    public ExpirationKCVSCache(final KeyColumnValueStore store, String metricsName, final long cacheTimeMS, final long invalidationGracePeriodMS, final long maximumByteSize) {
        super(store, metricsName);
        Preconditions.checkArgument(cacheTimeMS > 0, "Cache expiration must be positive: %s", cacheTimeMS);
        Preconditions.checkArgument(System.currentTimeMillis()+1000L*3600*24*365*100+cacheTimeMS>0,"Cache expiration time too large, overflow may occur: %s",cacheTimeMS);
        this.cacheTimeMS = cacheTimeMS;
        final int concurrencyLevel = Runtime.getRuntime().availableProcessors();
        Preconditions.checkArgument(invalidationGracePeriodMS >=0,"Invalid expiration grace period: %s", invalidationGracePeriodMS);
        this.invalidationGracePeriodMS = invalidationGracePeriodMS;
        CacheBuilder<KeySliceQuery,EntryList> cachebuilder = CacheBuilder.newBuilder()
                .maximumWeight(maximumByteSize)
                .concurrencyLevel(concurrencyLevel)
                .initialCapacity(1000)
                .expireAfterWrite(cacheTimeMS, TimeUnit.MILLISECONDS)
                .weigher((keySliceQuery, entries) -> GUAVA_CACHE_ENTRY_SIZE + KEY_QUERY_SIZE + entries.getByteSize());

        cache = cachebuilder.build();
        expiredKeys = new ConcurrentHashMap<>(50, 0.75f, concurrencyLevel);
        penaltyCountdown = new CountDownLatch(PENALTY_THRESHOLD);

        cleanupThread = new CleanupThread();
        cleanupThread.start();
    }

    @Override
    public EntryList getSlice(final KeySliceQuery query, final StoreTransaction txh) throws BackendException {
        incActionBy(1, CacheMetricsAction.RETRIEVAL,txh);
        if (isExpired(query)) {
            incActionBy(1, CacheMetricsAction.MISS,txh);
            return store.getSlice(query, unwrapTx(txh));
        }

        try {
            return cache.get(query, () -> {
                incActionBy(1, CacheMetricsAction.MISS,txh);
                return store.getSlice(query, unwrapTx(txh));
            });
        } catch (Exception e) {
            if (e instanceof JanusGraphException) throw (JanusGraphException)e;
            else if (e.getCause() instanceof JanusGraphException) throw (JanusGraphException)e.getCause();
            else throw new JanusGraphException(e);
        }
    }

    @Override
    public Map<StaticBuffer,EntryList> getSlice(final List<StaticBuffer> keys, final SliceQuery query, final StoreTransaction txh) throws BackendException {
        final Map<StaticBuffer,EntryList> results = new HashMap<>(keys.size());
        final List<StaticBuffer> remainingKeys = new ArrayList<>(keys.size());
        KeySliceQuery[] ksqs = new KeySliceQuery[keys.size()];
        incActionBy(keys.size(), CacheMetricsAction.RETRIEVAL,txh);
        //Find all cached queries
        for (int i=0;i<keys.size();i++) {
            final StaticBuffer key = keys.get(i);
            ksqs[i] = new KeySliceQuery(key,query);
            EntryList result = null;
            if (!isExpired(ksqs[i])) result = cache.getIfPresent(ksqs[i]);
            else ksqs[i]=null;
            if (result!=null) results.put(key,result);
            else remainingKeys.add(key);
        }
        //Request remaining ones from backend
        if (!remainingKeys.isEmpty()) {
            incActionBy(remainingKeys.size(), CacheMetricsAction.MISS,txh);
            Map<StaticBuffer,EntryList> subresults = store.getSlice(remainingKeys, query, unwrapTx(txh));
            for (int i=0;i<keys.size();i++) {
                StaticBuffer key = keys.get(i);
                EntryList subresult = subresults.get(key);
                if (subresult!=null) {
                    results.put(key,subresult);
                    if (ksqs[i]!=null) cache.put(ksqs[i],subresult);
                }
            }
        }
        return results;
    }

    @Override
    public void clearCache() {
        cache.invalidateAll();
        expiredKeys.clear();
        penaltyCountdown = new CountDownLatch(PENALTY_THRESHOLD);
    }

    @Override
    public void invalidate(StaticBuffer key, List<CachableStaticBuffer> entries) {
        Preconditions.checkArgument(!hasValidateKeysOnly() || entries.isEmpty());
        expiredKeys.put(key,getExpirationTime());
        if (Math.random()<1.0/INVALIDATE_KEY_FRACTION_PENALTY) penaltyCountdown.countDown();
    }

    @Override
    public void close() throws BackendException {
        cleanupThread.stopThread();
        super.close();
    }

    private boolean isExpired(final KeySliceQuery query) {
        Long until = expiredKeys.get(query.getKey());
        if (until==null) return false;
        if (isBeyondExpirationTime(until)) {
            expiredKeys.remove(query.getKey(),until);
            return false;
        }
        //We suffer a cache miss, hence decrease the count down
        penaltyCountdown.countDown();
        return true;
    }

    private long getExpirationTime() {
        return System.currentTimeMillis()+cacheTimeMS;
    }

    private boolean isBeyondExpirationTime(long until) {
        return until<System.currentTimeMillis();
    }

    private long getAge(long until) {
        long age = System.currentTimeMillis() - (until-cacheTimeMS);
        assert age>=0;
        return age;
    }

    private class CleanupThread extends Thread {

        private boolean stop = false;

        public CleanupThread() {
            this.setDaemon(true);
            this.setName("ExpirationStoreCache-" + getId());
        }

        @Override
        public void run() {
            while (true) {
                if (stop) return;
                try {

                    penaltyCountdown.await();
                } catch (InterruptedException e) {
                    if (stop) return;
                    else throw new RuntimeException("Cleanup thread got interrupted",e);
                }
                //Do clean up work by invalidating all entries for expired keys
                final Map<StaticBuffer,Long> expiredKeysCopy = new HashMap<>(expiredKeys.size());
                for (Map.Entry<StaticBuffer,Long> expKey : expiredKeys.entrySet()) {
                    if (isBeyondExpirationTime(expKey.getValue()))
                        expiredKeys.remove(expKey.getKey(), expKey.getValue());
                    else if (getAge(expKey.getValue())>= invalidationGracePeriodMS)
                        expiredKeysCopy.put(expKey.getKey(),expKey.getValue());
                }
                for (KeySliceQuery ksq : cache.asMap().keySet()) {
                    if (expiredKeysCopy.containsKey(ksq.getKey())) cache.invalidate(ksq);
                }
                penaltyCountdown = new CountDownLatch(PENALTY_THRESHOLD);
                for (Map.Entry<StaticBuffer,Long> expKey : expiredKeysCopy.entrySet()) {
                    expiredKeys.remove(expKey.getKey(),expKey.getValue());
                }
            }
        }

        void stopThread() {
            stop = true;
            this.interrupt();
        }
    }




}
