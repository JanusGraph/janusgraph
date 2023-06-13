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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeysQueriesGroup;
import org.janusgraph.diskstorage.keycolumnvalue.MultiKeysQueryGroups;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.CacheMetricsAction;
import org.janusgraph.graphdb.util.MultiSliceQueriesGroupingUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.janusgraph.util.datastructures.ByteSize.CAFFEINE_CACHE_ENTRY_SIZE;
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
        Caffeine<KeySliceQuery,EntryList> cachebuilder = Caffeine.newBuilder()
                .maximumWeight(maximumByteSize)
                .initialCapacity(1000)
                .expireAfterWrite(cacheTimeMS, TimeUnit.MILLISECONDS)
                .weigher((keySliceQuery, entries) -> CAFFEINE_CACHE_ENTRY_SIZE + KEY_QUERY_SIZE + entries.getByteSize());

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

        return cache.get(query, key -> {
            incActionBy(1, CacheMetricsAction.MISS,txh);
            try {
                return store.getSlice(query, unwrapTx(txh));
            } catch (BackendException e) {
                if (e.getCause() instanceof JanusGraphException) throw (JanusGraphException)e.getCause();
                else throw new JanusGraphException(e);
            }
        });
    }

    @Override
    public Map<StaticBuffer,EntryList> getSlice(final List<StaticBuffer> keys, final SliceQuery query, final StoreTransaction txh) throws BackendException {
        final Map<StaticBuffer,EntryList> results = new HashMap<>(keys.size());
        incActionBy(keys.size(), CacheMetricsAction.RETRIEVAL,txh);
        Pair<List<StaticBuffer>, Map<StaticBuffer, KeySliceQuery>> misses = fillResultAndReturnMisses(results, query, keys);
        final List<StaticBuffer> remainingKeys = misses.getKey();
        final Map<StaticBuffer, KeySliceQuery> keySliceQueries = misses.getValue();

        //Request remaining ones from backend
        if (!remainingKeys.isEmpty()) {
            incActionBy(remainingKeys.size(), CacheMetricsAction.MISS,txh);
            Map<StaticBuffer,EntryList> subresults = store.getSlice(remainingKeys, query, unwrapTx(txh));
            subresults.forEach((key, subresult) -> {
                KeySliceQuery ksqs = keySliceQueries.get(key);
                if(ksqs != null){
                    cache.put(ksqs,subresult);
                }
                results.put(key,subresult);
            });
        }
        return results;
    }

    @Override
    public Map<SliceQuery, Map<StaticBuffer, EntryList>> getMultiSlices(final MultiKeysQueryGroups<StaticBuffer, SliceQuery> multiKeysQueryGroups,
                                                                        final StoreTransaction txh) throws BackendException {
        Map<SliceQuery, Map<StaticBuffer, EntryList>> result = new HashMap<>(multiKeysQueryGroups.getMultiQueryContext().getTotalAmountOfQueries());
        Map<SliceQuery, Map<StaticBuffer, KeySliceQuery>> remainingKeysPerQuery = new HashMap<>(multiKeysQueryGroups.getMultiQueryContext().getTotalAmountOfQueries());

        List<KeysQueriesGroup<StaticBuffer, SliceQuery>> allQueryGroups = multiKeysQueryGroups.getQueryGroups();
        List<Pair<SliceQuery, List<StaticBuffer>>> updatedQueryGroups = new ArrayList<>();

        for(KeysQueriesGroup<StaticBuffer, SliceQuery> keyQueriesGroup : allQueryGroups){
            List<StaticBuffer> currentKeys = keyQueriesGroup.getKeysGroup();
            List<SliceQuery> currentQueries = keyQueriesGroup.getQueries();
            if(currentKeys.isEmpty() || currentQueries.isEmpty()){
                continue;
            }

            incActionBy(currentKeys.size()*currentQueries.size(), CacheMetricsAction.RETRIEVAL,txh);
            List<SliceQuery> remainingCurrentGroupQueries = new ArrayList<>(currentQueries.size());

            for(SliceQuery query : currentQueries){
                Map<StaticBuffer, EntryList> currentQueryResult = result.computeIfAbsent(query, q -> new HashMap<>(currentKeys.size()));
                Pair<List<StaticBuffer>, Map<StaticBuffer, KeySliceQuery>> misses = fillResultAndReturnMisses(currentQueryResult, query, currentKeys);
                final List<StaticBuffer> remainingCurrentGroupKeys = misses.getKey();
                remainingKeysPerQuery.put(query, misses.getValue());
                if(remainingCurrentGroupKeys.size() == currentKeys.size()){
                    remainingCurrentGroupQueries.add(query);
                } else if(!remainingCurrentGroupKeys.isEmpty()){
                    updatedQueryGroups.add(Pair.of(query, remainingCurrentGroupKeys));
                }
            }

            if(remainingCurrentGroupQueries.size() != currentQueries.size()){
                keyQueriesGroup.setQueries(remainingCurrentGroupQueries);
            }
        }

        // move queries with updated groups to new existing or new groups.
        MultiSliceQueriesGroupingUtil.moveQueriesToNewLeafNode(updatedQueryGroups,
            multiKeysQueryGroups.getMultiQueryContext().getAllKeysArr(),
            multiKeysQueryGroups.getMultiQueryContext().getGroupingRootTreeNode(),
            allQueryGroups);

        allQueryGroups = filterEmptyGroups(allQueryGroups);
        multiKeysQueryGroups.setQueryGroups(allQueryGroups);

        //Request remaining ones from backend
        if(!allQueryGroups.isEmpty()){
            Map<SliceQuery, Map<StaticBuffer, EntryList>> subresults = store.getMultiSlices(multiKeysQueryGroups, unwrapTx(txh));
            subresults.forEach((sliceQuery, sliceQueryResultsPerKey) -> {

                if(!sliceQueryResultsPerKey.isEmpty()){
                    incActionBy(sliceQueryResultsPerKey.size(), CacheMetricsAction.MISS,txh);
                }

                // populate cache with new results for any key with non-expired keySliceQuery
                Map<StaticBuffer, KeySliceQuery> queryKeySliceQueriesPerVertexKey = remainingKeysPerQuery.get(sliceQuery);
                if(queryKeySliceQueriesPerVertexKey != null){
                    sliceQueryResultsPerKey.forEach((key, keyResult) -> {
                        KeySliceQuery ksqs = queryKeySliceQueriesPerVertexKey.get(key);
                        if(ksqs != null){
                            cache.put(ksqs,keyResult);
                        }
                    });
                }

                // add requested results into a final resulting map
                Map<StaticBuffer, EntryList> currentSliceQueryResults = result.get(sliceQuery);
                if(currentSliceQueryResults == null){
                    currentSliceQueryResults = sliceQueryResultsPerKey;
                    result.put(sliceQuery, currentSliceQueryResults);
                } else {
                    currentSliceQueryResults.putAll(sliceQueryResultsPerKey);
                }
            });
        }

        return result;
    }

    private List<KeysQueriesGroup<StaticBuffer, SliceQuery>> filterEmptyGroups(List<KeysQueriesGroup<StaticBuffer, SliceQuery>> originalGroups){
        List<KeysQueriesGroup<StaticBuffer, SliceQuery>> filteredGroups = new ArrayList<>(originalGroups.size());
        for(KeysQueriesGroup<StaticBuffer, SliceQuery> group : originalGroups){
            if(!group.getKeysGroup().isEmpty() && !group.getQueries().isEmpty()){
                filteredGroups.add(group);
            }
        }
        return filteredGroups;
    }


    /**
     * Fills the result with non-expired cached data. Returns any keys which are missed as well as their optional `KeySliceQuery` in case it's not expired.
     * If `KeySliceQuery` is currently considered to be expired then `null` will be returned for respective `StaticBuffer`.
     * This method doesn't execute any slice queries to the storage backend.
     */
    private Pair<List<StaticBuffer>, Map<StaticBuffer, KeySliceQuery>> fillResultAndReturnMisses(final Map<StaticBuffer,EntryList> results, final SliceQuery query, final Collection<StaticBuffer> keys){
        final Map<StaticBuffer, KeySliceQuery> keySliceQueries = new HashMap<>(keys.size());
        final List<StaticBuffer> remainingKeys = new ArrayList<>(keys.size());
        //Find all cached queries
        for (StaticBuffer key : keys) {
            KeySliceQuery ksqs = new KeySliceQuery(key,query);
            if (isExpired(ksqs)){
                remainingKeys.add(key);
                keySliceQueries.put(key, null);
            } else {
                EntryList result = cache.getIfPresent(ksqs);
                if (result == null){
                    remainingKeys.add(key);
                    keySliceQueries.put(key, ksqs);
                } else {
                    results.put(key, result);
                }
            }
        }
        return Pair.of(remainingKeys, keySliceQueries);
    }

    @Override
    public void clearCache() {
        // We should not call `expiredKeys.clear();` directly because there could be a race condition
        // where already invalidated cache but then added new entries into it and made some mutation before `expiredKeys.clear();`
        // is finished which may result in getSlice to return previously cached result and not a new mutated result.
        // Thus, we are clearing expired entries first, and then we are safe to invalidate the rest of non-expired entries.
        // Moreover, we shouldn't create `penaltyCountdown = new CountDownLatch(PENALTY_THRESHOLD);` because the cleaning thread
        // may await on the previous `penaltyCountdown` which may result in that thread never wake up to proceed with
        // probabilistic cleaning. Thus, only that cleaning thread have to have a right to reinitialize `penaltyCountdown`.
        forceClearExpiredCache();
        // It's always safe to invalidate full cache
        cache.invalidateAll();
    }

    @Override
    public void invalidate(StaticBuffer key, List<CachableStaticBuffer> entries) {
        Preconditions.checkArgument(!hasValidateKeysOnly() || entries.isEmpty());
        expiredKeys.put(key,getExpirationTime());
        if (Math.random()<1.0/INVALIDATE_KEY_FRACTION_PENALTY) penaltyCountdown.countDown();
    }

    @Override
    public void forceClearExpiredCache() {
        clearExpiredCache(false);
    }

    private synchronized void clearExpiredCache(boolean withNewPenaltyCountdown) {
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
        if(withNewPenaltyCountdown){
            penaltyCountdown = new CountDownLatch(PENALTY_THRESHOLD);
        }
        for (Map.Entry<StaticBuffer,Long> expKey : expiredKeysCopy.entrySet()) {
            expiredKeys.remove(expKey.getKey(),expKey.getValue());
        }
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
                clearExpiredCache(true);
            }
        }

        void stopThread() {
            stop = true;
            this.interrupt();
        }
    }

}
