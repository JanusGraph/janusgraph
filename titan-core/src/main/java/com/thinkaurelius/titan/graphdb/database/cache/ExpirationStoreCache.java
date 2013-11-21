package com.thinkaurelius.titan.graphdb.database.cache;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import static com.thinkaurelius.titan.util.datastructures.ByteSize.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ExpirationStoreCache implements StoreCache {

    private static final Logger log =
            LoggerFactory.getLogger(ExpirationStoreCache.class);

    //Weight estimation
    private static final int STATICARRAYBUFFER_SIZE = STATICARRAYBUFFER_RAW_SIZE + 10; // 10 = last number is average length
    private static final int KEY_QUERY_SIZE = OBJECT_HEADER + 4 + 1 + 3 * (OBJECT_REFERENCE + STATICARRAYBUFFER_SIZE); // object_size + int + boolean + 3 static buffers

    private static final int INVALIDATE_KEY_FRACTION_PENALTY = 1000;
    private static final int PENALTY_THRESHOLD = 5;

    private static final AtomicLong globalCacheRetrievals = new AtomicLong(0);
    private static final AtomicLong globalCacheMisses = new AtomicLong(0);

    private volatile CountDownLatch penaltyCountdown;

    private final Cache<KeySliceQuery,List<Entry>> cache;
    private final ConcurrentHashMap<StaticBuffer,Long> expiredKeys;

    private final long cacheTimeMS;
    private final long expirationGracePeriodMS;
    private final CleanupThread cleanupThread;

    public ExpirationStoreCache(final long cacheTimeMS, final long expirationGracePeriodMS, final long maximumByteSize) {
        Preconditions.checkArgument(cacheTimeMS>0,"Cache expiration must be positive: %s");
        Preconditions.checkArgument(System.currentTimeMillis()+1000l*3600*24*365*100+cacheTimeMS>0,"Cache expiration time too large, overflow may occur: %s",cacheTimeMS);
        this.cacheTimeMS = cacheTimeMS;
        int concurrencyLevel = Runtime.getRuntime().availableProcessors();
        Preconditions.checkArgument(expirationGracePeriodMS>=0,"Invalid expiration grace peiod: %s",expirationGracePeriodMS);
        this.expirationGracePeriodMS = expirationGracePeriodMS;
        CacheBuilder<KeySliceQuery,List<Entry>> cachebuilder = CacheBuilder.newBuilder()
                .maximumWeight(maximumByteSize)
                .concurrencyLevel(concurrencyLevel)
                .initialCapacity(1000)
                .expireAfterWrite(cacheTimeMS, TimeUnit.MILLISECONDS)
                .weigher(new Weigher<KeySliceQuery, List<Entry>>() {
                    @Override
                    public int weigh(KeySliceQuery keySliceQuery, List<Entry> entries) {
                        int size = GUAVA_CACHE_ENTRY_SIZE + KEY_QUERY_SIZE + ARRAYLIST_SIZE;
                        for (Entry e : entries) size+=e.getByteSize();
                        return size;
                    }
                });

        cache = cachebuilder.build();
        expiredKeys = new ConcurrentHashMap<StaticBuffer, Long>(50,0.75f,concurrencyLevel);
        penaltyCountdown = new CountDownLatch(PENALTY_THRESHOLD);

        cleanupThread = new CleanupThread();
        cleanupThread.start();

    }

    public static void resetGlobablCounts() {
        globalCacheRetrievals.set(0);
        globalCacheMisses.set(0);
    }

    public static long getGlobalCacheRetrievals() {
        return globalCacheRetrievals.get();
    }

    public static long getGlobalCacheMisses() {
        return globalCacheMisses.get();
    }

    public static long getGlobalCacheHits() {
        return getGlobalCacheRetrievals()-getGlobalCacheMisses();
    }

    private boolean isExpired(final KeySliceQuery query) {
        Long until = expiredKeys.get(query.getKey());
        if (until==null) return false;
        if (isBeyondExpirationTime(until)) {
            expiredKeys.remove(query.getKey(),until);
            return false;
        }
        //We suffer
        penaltyCountdown.countDown();
        return true;
    }

    @Override
    public List<Entry> query(final KeySliceQuery query, final BackendTransaction tx) {
        if (isExpired(query)) return tx.edgeStoreQuery(query);

        try {
            globalCacheRetrievals.incrementAndGet();
            return cache.get(query,new Callable<List<Entry>>() {
                @Override
                public List<Entry> call() throws Exception {
                    globalCacheMisses.incrementAndGet();
                    return tx.edgeStoreQuery(query);
                }
            });
        } catch (Exception e) {
            if (e instanceof TitanException) throw (TitanException)e;
            else if (e.getCause() instanceof TitanException) throw (TitanException)e.getCause();
            else throw new TitanException(e);
        }
    }

    @Override
    public List<List<Entry>> multiQuery(List<StaticBuffer> keys, SliceQuery query, BackendTransaction tx) {
        List<Entry>[] results = (List<Entry>[])new List[keys.size()];
        List<StaticBuffer> remainingKeys = new ArrayList<StaticBuffer>(keys.size());
        KeySliceQuery[] ksqs = new KeySliceQuery[keys.size()];
        for (int i=0;i<keys.size();i++) {
            StaticBuffer key = keys.get(i);
            ksqs[i] = new KeySliceQuery(key,query);
            List<Entry> result = null;
            if (!isExpired(ksqs[i])) result = cache.getIfPresent(ksqs[i]);
            else ksqs[i]=null;
            if (result!=null) results[i]=result;
            else remainingKeys.add(key);
        }
        List<List<Entry>> subresults = tx.edgeStoreMultiQuery(remainingKeys,query);
        int pos = 0;
        for (int i=0;i<results.length;i++) {
            if (results[i]!=null) continue;
            assert pos<subresults.size();
            List<Entry> subresult = subresults.get(pos);
            assert subresult!=null;
            results[i]=subresult;
            if (ksqs[i]!=null) cache.put(ksqs[i],subresult);
            pos++;
        }
        assert pos==subresults.size();
        return Arrays.asList(results);
    }

    private final long getExpirationTime() {
        return System.currentTimeMillis()+cacheTimeMS;
    }

    private final boolean isBeyondExpirationTime(long until) {
        return until<System.currentTimeMillis();
    }

    private final long getAge(long until) {
        long age = System.currentTimeMillis() - (until-cacheTimeMS);
        assert age>=0;
        return age;
    }

    @Override
    public void invalidate(StaticBuffer key) {
        expiredKeys.put(key,getExpirationTime());
        if (Math.random()<1.0/INVALIDATE_KEY_FRACTION_PENALTY) penaltyCountdown.countDown();
    }

    @Override
    public void close() {
        cleanupThread.stopThread();
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
                HashMap<StaticBuffer,Long> expiredKeysCopy = new HashMap<StaticBuffer,Long>(expiredKeys.size());
                for (Map.Entry<StaticBuffer,Long> expKey : expiredKeys.entrySet()) {
                    if (isBeyondExpirationTime(expKey.getValue()))
                        expiredKeys.remove(expKey.getKey(), expKey.getValue());
                    else if (getAge(expKey.getValue())>=expirationGracePeriodMS)
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
