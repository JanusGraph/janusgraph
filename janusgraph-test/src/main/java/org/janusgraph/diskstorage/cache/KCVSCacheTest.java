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

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.inmemory.InMemoryStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.MultiSlicesQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.cache.CacheTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVSCache;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.WriteByteBuffer;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class KCVSCacheTest {

    public static final String STORE_NAME = "store";
    public static final TimestampProvider times = TimestampProviders.MICRO;
    public static final Duration MAX_WRITE_TIME = Duration.ofMillis(100);

    public KeyColumnValueStoreManager storeManager;
    public CounterKCVS store;
    public KCVSCache cache;

    @BeforeEach
    public void setup() throws Exception {
        storeManager = new InMemoryStoreManager();
        store = new CounterKCVS(storeManager.openDatabase(STORE_NAME));
        cache = getCache(store);
    }

    public abstract KCVSCache getCache(KeyColumnValueStore store);

    public StoreTransaction getStoreTx() {
        try {
        return storeManager.beginTransaction(StandardBaseTransactionConfig.of(times));
        } catch (BackendException se) {
            throw new RuntimeException(se);
        }
    }

    public CacheTransaction getCacheTx() {
        return new CacheTransaction(getStoreTx(), storeManager, 1024, MAX_WRITE_TIME, false);
    }

    @AfterEach
    public void shutdown() throws Exception {
        cache.close();
        storeManager.close();
    }

    public void loadStore(int numKeys, int numCols) {
        StoreTransaction tx = getStoreTx();
        try {
            for (int i=1;i<=numKeys;i++) {
                final List<Entry> adds = new ArrayList<>(numCols);
                for (int j=1;j<=numCols;j++) adds.add(getEntry(j,j));
                store.mutate(BufferUtil.getIntBuffer(i),adds,KeyColumnValueStore.NO_DELETIONS,tx);
            }
            tx.commit();
        } catch (BackendException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSmallCache() throws Exception {
        final int numKeys = 100, numCols = 10; //numCols must be greater than or equal to 10 as it is assumed below
        final int repeats = 100, clearEvery = 20, numMulti = 10;
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
            final List<StaticBuffer> keys = new ArrayList<>();
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
        final List<StaticBuffer> keys = new ArrayList<>();
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
        final List<Entry> deletions = new ArrayList<>(numCols/2);
        for (int j=1;j<=numCols;j=j+2) deletions.add(getEntry(j,j));
        cache.mutateEntries(key, KeyColumnValueStore.NO_ADDITIONS, deletions, tx);
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
        public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
            getSliceCounter.incrementAndGet();
            return store.getSlice(query,txh);
        }

        @Override
        public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
            getSliceCounter.incrementAndGet();
            return store.getSlice(keys,query,txh);
        }

        @Override
        public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
            store.mutate(key,additions,deletions,txh);
        }

        @Override
        public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
            store.acquireLock(key,column,expectedValue,txh);
        }

        @Override
        public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
            return store.getKeys(query,txh);
        }

        @Override
        public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
            return store.getKeys(query,txh);
        }

        @Override
        public KeySlicesIterator getKeys(MultiSlicesQuery queries, StoreTransaction txh) throws BackendException {
            return store.getKeys(queries, txh);
        }

        @Override
        public String getName() {
            return store.getName();
        }

        @Override
        public void close() throws BackendException {
            store.close();
        }
    }

}
