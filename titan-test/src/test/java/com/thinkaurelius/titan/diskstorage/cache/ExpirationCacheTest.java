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
    public void testExpiration1() throws Exception {
        testExpiration(new StandardDuration(2,TimeUnit.SECONDS),ZeroDuration.INSTANCE);
    }

    @Test
    public void testExpiration2() throws Exception {
        testExpiration(new StandardDuration(4,TimeUnit.SECONDS),new StandardDuration(200,TimeUnit.MILLISECONDS));
    }

    private void testExpiration(Duration expirationTime, Duration graceWait) throws Exception {
        final int numKeys = 100, numCols = 10;
        loadStore(numKeys,numCols);
        //Replace cache with proper times
        cache = getCache(store,expirationTime,graceWait);

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
        cache.mutateEntries(key,KeyColumnValueStore.NO_ADDITIONS, Lists.newArrayList(getEntry(4,4)),tx);
        tx.commit();
        store.resetCounter();
        //...invalidation should happen and the result set is updated immediately
        verifyResults(key, keys, query, 4);
        assertEquals(1,store.getSliceCalls());
        if (!graceWait.isZeroLength()) {
            utime = times.getTime();
            //with a grace wait time, an immidiate call should go through to the store again
            verifyResults(key, keys, query, 4);
            assertEquals(2,store.getSliceCalls());
            times.sleepPast(utime.add(graceWait));
            //but when we wait past that period, the cache should work again
            verifyResults(key, keys, query, 4);
            assertEquals(3,store.getSliceCalls());
            verifyResults(key, keys, query, 4);
            assertEquals(3,store.getSliceCalls());
        }
    }

    private void verifyResults(StaticBuffer key, List<StaticBuffer> keys, SliceQuery query, int expectedResults) throws Exception {
        CacheTransaction tx = getCacheTx();
        assertEquals(expectedResults,cache.getSlice(new KeySliceQuery(key,query),tx));
        Map<StaticBuffer,EntryList> results = cache.getSlice(keys,query,tx);
        assertEquals(keys.size(),results.size());
        assertEquals(expectedResults, results.get(key).size());
        tx.commit();
    }


}
