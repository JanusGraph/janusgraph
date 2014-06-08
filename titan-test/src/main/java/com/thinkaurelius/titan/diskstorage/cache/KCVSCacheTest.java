package com.thinkaurelius.titan.diskstorage.cache;

import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache.CacheTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache.KCVSCache;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StandardBaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.util.time.Timestamps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class KCVSCacheTest {

    public static final String STORE_NAME = "store";
    public static final TimestampProvider times = Timestamps.MICRO;
    public static final Duration MAX_WRITE_TIME = new StandardDuration(100, TimeUnit.MILLISECONDS);

    public KeyColumnValueStoreManager storeManager;
    public CounterKCVS store;
    public KCVSCache cache;

    @Before
    public void setup() throws Exception {
        storeManager = new InMemoryStoreManager();
        store = new CounterKCVS(storeManager.openDatabase(STORE_NAME));
        cache = getCache(store);
    }

    public abstract KCVSCache getCache(KeyColumnValueStore store);

    public StoreTransaction getStoreTx() {
        try {
        return storeManager.beginTransaction(StandardBaseTransactionConfig.of(times));
        } catch (StorageException se) {
            throw new RuntimeException(se);
        }
    }

    public CacheTransaction getCacheTx() {
        CacheTransaction cacheTx = new CacheTransaction(getStoreTx(), storeManager, 1024, MAX_WRITE_TIME, false);
        return cacheTx;
    }

    @After
    public void shutdown() throws Exception {
        cache.close();
        storeManager.close();
    }

    public void loadStore(int numKeys, int numCols) {
        StoreTransaction tx = getStoreTx();
        try {
            for (int i=1;i<=numKeys;i++) {
                List<Entry> adds = new ArrayList<Entry>(numCols);
                for (int j=1;j<=numCols;j++) adds.add(getEntry(j,j));
                store.mutate(BufferUtil.getIntBuffer(i),adds,KeyColumnValueStore.NO_DELETIONS,tx);
            }
            tx.commit();
        } catch (StorageException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSmallCache() throws Exception {
        final int numKeys = 100, numCols = 10;
        final int repeats = 100, clearEvery = 20, numMulti = 10;
        assertTrue(numCols>=10); //Assumed below
        loadStore(numKeys,numCols);

        //Repeatedly read from cache and clear in between
        int calls = 0;
        assertEquals(calls,store.getSliceCalls());
        for (int t=0;t<repeats;t++) {
            if (t%clearEvery==0) {
                cache.clearCache();
                calls+=numKeys*2+1;
            }
            CacheTransaction tx = getCacheTx();
            for (int i=1;i<=numKeys;i++) {
                assertEquals(10,cache.getSlice(getQuery(i,0,numCols+1).setLimit(10),tx).size());
                assertEquals(3,cache.getSlice(getQuery(i,2,5),tx).size());
            }
            //Multi-query
            List<StaticBuffer> keys = new ArrayList<StaticBuffer>();
            for (int i=10;i<10+numMulti;i++) keys.add(BufferUtil.getIntBuffer(i));
            Map<StaticBuffer,EntryList> result = cache.getSlice(keys,getQuery(4,9),tx);
            assertEquals(keys.size(),result.size());
            for (StaticBuffer key : keys) assertTrue(result.containsKey(key));
            for (EntryList r : result.values()) {
                assertEquals(5,r.size());
            }
            tx.commit();
            assertEquals(calls,store.getSliceCalls());
        }
        store.resetCounter();

        //Check invalidation
        StaticBuffer key = BufferUtil.getIntBuffer(23);
        List<StaticBuffer> keys = new ArrayList<StaticBuffer>();
        keys.add(key);
        keys.add(BufferUtil.getIntBuffer(12));
        keys.add(BufferUtil.getIntBuffer(5));

        //Read
        CacheTransaction tx = getCacheTx();
        assertEquals(numCols,cache.getSlice(new KeySliceQuery(key,getQuery(0,numCols+1)),tx).size());
        Map<StaticBuffer,EntryList> result = cache.getSlice(keys,getQuery(2,8),tx);
        assertEquals(keys.size(),result.size());
        assertEquals(6,result.get(key).size());
        //Update
        List<Entry> dels = new ArrayList<Entry>(numCols/2);
        for (int j=1;j<=numCols;j=j+2) dels.add(getEntry(j,j));
        cache.mutateEntries(key, KeyColumnValueStore.NO_ADDITIONS, dels, tx);
        tx.commit();
        assertEquals(2,store.getSliceCalls());

        //Ensure updates are correctly read
        tx = getCacheTx();
        assertEquals(numCols/2,cache.getSlice(new KeySliceQuery(key,getQuery(0,numCols+1)),tx).size());
        result = cache.getSlice(keys,getQuery(2,8),tx);
        assertEquals(keys.size(),result.size());
        assertEquals(3,result.get(key).size());
        tx.commit();
        assertEquals(4,store.getSliceCalls());
    }


    public static KeySliceQuery getQuery(int key, int startCol, int endCol) {
        return new KeySliceQuery(BufferUtil.getIntBuffer(key),getQuery(startCol, endCol));
    }

    public static SliceQuery getQuery(int startCol, int endCol) {
        return new SliceQuery(BufferUtil.getIntBuffer(startCol),BufferUtil.getIntBuffer(endCol));
    }

    public static Entry getEntry(int col, int val) {
        return new StaticArrayEntry(new WriteByteBuffer(4 * 2).putInt(col).putInt(val).getStaticBuffer(), 4);
    }


    public static class CounterKCVS implements KeyColumnValueStore {

        private final KeyColumnValueStore store;
        private final AtomicLong getSliceCounter;

        public CounterKCVS(KeyColumnValueStore store) {
            this.store = store;
            getSliceCounter = new AtomicLong(0);
        }

        public long getSliceCalls() {
            return getSliceCounter.get();
        }

        public void resetCounter() {
            getSliceCounter.set(0);
        }

        @Override
        public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
            getSliceCounter.incrementAndGet();
            return store.getSlice(query,txh);
        }

        @Override
        public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
            getSliceCounter.incrementAndGet();
            return store.getSlice(keys,query,txh);
        }

        @Override
        public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws StorageException {
            store.mutate(key,additions,deletions,txh);
        }

        @Override
        public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
            store.acquireLock(key,column,expectedValue,txh);
        }

        @Override
        public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws StorageException {
            return store.getKeys(query,txh);
        }

        @Override
        public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws StorageException {
            return store.getKeys(query,txh);
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

}
