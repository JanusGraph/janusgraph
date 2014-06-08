package com.thinkaurelius.titan.diskstorage.cache;

import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache.CacheTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache.ExpirationKCVSCache;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache.KCVSCache;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.diskstorage.util.time.Timepoint;
import com.thinkaurelius.titan.diskstorage.util.time.ZeroDuration;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ExpirationCacheTest extends KCVSCacheTest {

    public static final String METRICS_STRING = "metrics";
    public static final long CACHE_SIZE = 1024*1024*48; //48 MB

    @Override
    public KCVSCache getCache(KeyColumnValueStore store) {
        return getCache(store,new StandardDuration(1,TimeUnit.DAYS),ZeroDuration.INSTANCE);
    }

    private static KCVSCache getCache(KeyColumnValueStore store, Duration expirationTime, Duration graceWait) {
        return new ExpirationKCVSCache(store,METRICS_STRING,expirationTime.getLength(TimeUnit.MILLISECONDS),graceWait.getLength(TimeUnit.MILLISECONDS),CACHE_SIZE);
    }


    @Test
    public void testExpiration() throws Exception {
        testExpiration(new StandardDuration(200,TimeUnit.MILLISECONDS));
        testExpiration(new StandardDuration(4,TimeUnit.SECONDS));
        testExpiration(new StandardDuration(1,TimeUnit.SECONDS));
    }

    private void testExpiration(Duration expirationTime) throws Exception {
        final int numKeys = 100, numCols = 10;
        loadStore(numKeys,numCols);
        //Replace cache with proper times
        cache = getCache(store,expirationTime,ZeroDuration.INSTANCE);

        StaticBuffer key = BufferUtil.getIntBuffer(81);
        List<StaticBuffer> keys = new ArrayList<StaticBuffer>();
        keys.add(key);
        keys.add(BufferUtil.getIntBuffer(37));
        keys.add(BufferUtil.getIntBuffer(2));
        SliceQuery query = getQuery(2,8);

        verifyResults(key,keys,query,6);
        //Modify store directly
        StoreTransaction txs = getStoreTx();
        store.mutate(key,KeyColumnValueStore.NO_ADDITIONS, Lists.newArrayList(BufferUtil.getIntBuffer(5)),txs);
        txs.commit();
        Timepoint utime = times.getTime();

        //Should still see cached results
        verifyResults(key,keys,query,6);
        times.sleepPast(utime.add(expirationTime.multiply(0.5))); //Sleep half way through expiration time
        verifyResults(key, keys, query, 6);
        times.sleepPast(utime.add(expirationTime)); //Sleep past expiration time...
        times.sleepFor(new StandardDuration(5,TimeUnit.MILLISECONDS)); //...and just a little bit longer
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
        testGracePeriod(new StandardDuration(200,TimeUnit.MILLISECONDS));
        testGracePeriod(ZeroDuration.INSTANCE);
        testGracePeriod(new StandardDuration(1,TimeUnit.SECONDS));
    }

    private void testGracePeriod(Duration graceWait) throws Exception {
        final int minCleanupTriggerCalls = 5;
        final int numKeys = 100, numCols = 10;
        loadStore(numKeys,numCols);
        //Replace cache with proper times
        cache = getCache(store,new StandardDuration(200,TimeUnit.DAYS),graceWait);

        StaticBuffer key = BufferUtil.getIntBuffer(81);
        List<StaticBuffer> keys = new ArrayList<StaticBuffer>();
        keys.add(key);
        keys.add(BufferUtil.getIntBuffer(37));
        keys.add(BufferUtil.getIntBuffer(2));
        SliceQuery query = getQuery(2,8);

        verifyResults(key,keys,query,6);
        //If we modify through cache store...
        CacheTransaction tx = getCacheTx();
        cache.mutateEntries(key,KeyColumnValueStore.NO_ADDITIONS, Lists.newArrayList(getEntry(4,4)),tx);
        tx.commit();
        Timepoint utime = times.getTime();
        store.resetCounter();
        //...invalidation should happen and the result set is updated immediately
        verifyResults(key, keys, query, 5);
        assertEquals(2,store.getSliceCalls());
        //however, the key is expired and hence repeated calls need to go through to the store
        verifyResults(key, keys, query, 5);
        assertEquals(4,store.getSliceCalls());

        //however, when we sleep past the grace wait time and trigger a cleanup...
        times.sleepPast(utime.add(graceWait));
        for (int t=0; t<minCleanupTriggerCalls;t++) {
            assertEquals(5,cache.getSlice(new KeySliceQuery(key,query),tx).size());
            times.sleepFor(new StandardDuration(5,TimeUnit.MILLISECONDS));
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
