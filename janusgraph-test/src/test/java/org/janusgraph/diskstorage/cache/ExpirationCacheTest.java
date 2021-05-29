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

package org.janusgraph.diskstorage.cache;

import com.google.common.collect.Lists;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.cache.CacheTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.cache.ExpirationKCVSCache;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVSCache;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ExpirationCacheTest extends KCVSCacheTest {

    public static final String METRICS_STRING = "metrics";
    public static final long CACHE_SIZE = 1024*1024*48; //48 MB

    @Override
    public KCVSCache getCache(KeyColumnValueStore store) {
        return getCache(store,Duration.ofDays(1), Duration.ZERO);
    }

    private static KCVSCache getCache(KeyColumnValueStore store, Duration expirationTime, Duration graceWait) {
        return new ExpirationKCVSCache(store,METRICS_STRING,expirationTime.toMillis(),graceWait.toMillis(),CACHE_SIZE);
    }


    @Test
    public void testExpiration() throws Exception {
        testExpiration(Duration.ofMillis(200));
        testExpiration(Duration.ofSeconds(4));
        testExpiration(Duration.ofSeconds(1));
    }

    private void testExpiration(Duration expirationTime) throws Exception {
        final int numKeys = 100, numCols = 10;
        loadStore(numKeys,numCols);
        //Replace cache with proper times
        cache = getCache(store,expirationTime, Duration.ZERO);

        final StaticBuffer key = BufferUtil.getIntBuffer(81);
        final List<StaticBuffer> keys = new ArrayList<>();
        keys.add(key);
        keys.add(BufferUtil.getIntBuffer(37));
        keys.add(BufferUtil.getIntBuffer(2));
        SliceQuery query = getQuery(2,8);

        verifyResults(key,keys,query,6);
        //Modify store directly
        StoreTransaction txs = getStoreTx();
        store.mutate(key,KeyColumnValueStore.NO_ADDITIONS, Lists.newArrayList(BufferUtil.getIntBuffer(5)),txs);
        txs.commit();
        Instant utime = times.getTime();

        //Should still see cached results
        verifyResults(key,keys,query,6);
        times.sleepPast(utime.plus(expirationTime.dividedBy(2))); //Sleep half way through expiration time
        verifyResults(key, keys, query, 6);
        times.sleepPast(utime.plus(expirationTime)); //Sleep past expiration time...
        times.sleepFor(Duration.ofMillis(5)); //...and just a little bit longer
        //Now the results should be different
        verifyResults(key, keys, query, 5);
        //If we modify through cache store...
        CacheTransaction tx = getCacheTx();
        cache.mutateEntries(key, KeyColumnValueStore.NO_ADDITIONS, Lists.newArrayList(getEntry(4, 4)), tx);
        tx.commit();
        store.resetCounter();
        //...invalidation should happen and the result set is updated immediately
        verifyResults(key, keys, query, 4);
    }

    @Test
    public void testGracePeriod() throws Exception {
        testGracePeriod(Duration.ofMillis(200));
        testGracePeriod(Duration.ZERO);
        testGracePeriod(Duration.ofSeconds(1));
    }

    private void testGracePeriod(Duration graceWait) throws Exception {
        final int minCleanupTriggerCalls = 5;
        final int numKeys = 100, numCols = 10;
        loadStore(numKeys,numCols);
        //Replace cache with proper times
        cache = getCache(store,Duration.ofDays(200),graceWait);

        final StaticBuffer key = BufferUtil.getIntBuffer(81);
        final List<StaticBuffer> keys = new ArrayList<>();
        keys.add(key);
        keys.add(BufferUtil.getIntBuffer(37));
        keys.add(BufferUtil.getIntBuffer(2));
        SliceQuery query = getQuery(2,8);

        verifyResults(key,keys,query,6);
        //If we modify through cache store...
        CacheTransaction tx = getCacheTx();
        cache.mutateEntries(key,KeyColumnValueStore.NO_ADDITIONS, Lists.newArrayList(getEntry(4,4)),tx);
        tx.commit();
        Instant utime = times.getTime();
        store.resetCounter();
        //...invalidation should happen and the result set is updated immediately
        verifyResults(key, keys, query, 5);
        assertEquals(2,store.getSliceCalls());
        //however, the key is expired and hence repeated calls need to go through to the store
        verifyResults(key, keys, query, 5);
        assertEquals(4,store.getSliceCalls());

        //however, when we sleep past the grace wait time and trigger a cleanup...
        times.sleepPast(utime.plus(graceWait));
        for (int t=0; t<minCleanupTriggerCalls;t++) {
            assertEquals(5,cache.getSlice(new KeySliceQuery(key,query),tx).size());
            times.sleepFor(Duration.ofMillis(5));
        }
        //...the cache should cache results again
        store.resetCounter();
        verifyResults(key, keys, query, 5);
        assertEquals(0,store.getSliceCalls());
        verifyResults(key, keys, query, 5);
        assertEquals(0,store.getSliceCalls());
    }

    private void verifyResults(StaticBuffer key, List<StaticBuffer> keys, SliceQuery query, int expectedResults) throws Exception {
        CacheTransaction tx = getCacheTx();
        assertEquals(expectedResults,cache.getSlice(new KeySliceQuery(key,query),tx).size());
        Map<StaticBuffer,EntryList> results = cache.getSlice(keys,query,tx);
        assertEquals(keys.size(),results.size());
        assertEquals(expectedResults, results.get(key).size());
        tx.commit();
    }


}
