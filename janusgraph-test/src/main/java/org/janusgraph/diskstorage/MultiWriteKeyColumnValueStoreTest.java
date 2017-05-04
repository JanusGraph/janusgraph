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

package org.janusgraph.diskstorage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.keycolumnvalue.cache.CacheTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVEntryMutation;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVSCache;
import org.janusgraph.diskstorage.keycolumnvalue.cache.NoKCVSCache;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;

import static org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore.*;

import org.janusgraph.diskstorage.util.StaticArrayEntry;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public abstract class MultiWriteKeyColumnValueStoreTest extends AbstractKCVSTest {

    private Logger log = LoggerFactory.getLogger(MultiWriteKeyColumnValueStoreTest.class);

    int bufferSize = 20;

    protected String storeName1 = "testStore1";
    private KCVSCache store1;
    protected String storeName2 = "testStore2";
    private KCVSCache store2;


    public KeyColumnValueStoreManager manager;
    public StoreTransaction tx;


    private Random rand = new Random(10);

    @Before
    public void setUp() throws Exception {
        StoreManager m = openStorageManager();
        m.clearStorage();
        m.close();
        open();
    }

    @After
    public void tearDown() throws Exception {
        close();
    }

    public abstract KeyColumnValueStoreManager openStorageManager() throws BackendException;

    public void open() throws BackendException {
        manager = openStorageManager();
        tx = new CacheTransaction(manager.beginTransaction(getTxConfig()), manager, bufferSize, Duration.ofMillis(100), true);
        store1 = new NoKCVSCache(manager.openDatabase(storeName1));
        store2 = new NoKCVSCache(manager.openDatabase(storeName2));

    }

    public void close() throws BackendException {
        if (tx != null) tx.commit();
        if (null != store1) store1.close();
        if (null != store2) store2.close();
        if (null != manager) manager.close();
    }

    public void clopen() throws BackendException {
        close();
        open();
    }

    public void newTx() throws BackendException {
        if (tx!=null) tx.commit();
        tx = new CacheTransaction(manager.beginTransaction(getTxConfig()), manager, bufferSize, Duration.ofMillis(100), true);
    }

    @Test
    public void deletionsAppliedBeforeAdditions() throws BackendException {

        StaticBuffer b1 = KeyColumnValueStoreUtil.longToByteBuffer(1);

        Assert.assertNull(KCVSUtil.get(store1, b1, b1, tx));

        List<Entry> additions = Lists.newArrayList(StaticArrayEntry.of(b1, b1));

        List<Entry> deletions = Lists.newArrayList(additions);

        Map<StaticBuffer, KCVEntryMutation> combination = new HashMap<>(1);
        Map<StaticBuffer, KCVEntryMutation> deleteOnly = new HashMap<>(1);
        Map<StaticBuffer, KCVEntryMutation> addOnly = new HashMap<>(1);

        combination.put(b1, new KCVEntryMutation(additions, deletions));
        deleteOnly.put(b1, new KCVEntryMutation(KeyColumnValueStore.NO_ADDITIONS, deletions));
        addOnly.put(b1, new KCVEntryMutation(additions, KCVSCache.NO_DELETIONS));

        store1.mutateEntries(b1, additions, deletions, tx);
        newTx();

        StaticBuffer result = KCVSUtil.get(store1, b1, b1, tx);

        Assert.assertEquals(b1, result);

        store1.mutateEntries(b1, NO_ADDITIONS, deletions, tx);
        newTx();

        for (int i = 0; i < 100; i++) {
            StaticBuffer n = KCVSUtil.get(store1, b1, b1, tx);
            Assert.assertNull(n);
            store1.mutateEntries(b1, additions, KCVSCache.NO_DELETIONS, tx);
            newTx();
            store1.mutateEntries(b1, NO_ADDITIONS, deletions, tx);
            newTx();
            n = KCVSUtil.get(store1, b1, b1, tx);
            Assert.assertNull(n);
        }

        for (int i = 0; i < 100; i++) {
            store1.mutateEntries(b1, NO_ADDITIONS, deletions, tx);
            newTx();
            store1.mutateEntries(b1, additions, KCVSCache.NO_DELETIONS, tx);
            newTx();
            Assert.assertEquals(b1, KCVSUtil.get(store1, b1, b1, tx));
        }

        for (int i = 0; i < 100; i++) {
            store1.mutateEntries(b1, additions, deletions, tx);
            newTx();
            Assert.assertEquals(b1, KCVSUtil.get(store1, b1, b1, tx));
        }
    }

    @Test
    public void mutateManyWritesSameKeyOnMultipleCFs() throws BackendException {

        final long arbitraryLong = 42;
        assert 0 < arbitraryLong;

        final StaticBuffer key = KeyColumnValueStoreUtil.longToByteBuffer(arbitraryLong * arbitraryLong);
        final StaticBuffer val = KeyColumnValueStoreUtil.longToByteBuffer(arbitraryLong * arbitraryLong * arbitraryLong);
        final StaticBuffer col = KeyColumnValueStoreUtil.longToByteBuffer(arbitraryLong);
        final StaticBuffer nextCol = KeyColumnValueStoreUtil.longToByteBuffer(arbitraryLong + 1);

        final StoreTransaction directTx = manager.beginTransaction(getTxConfig());

        KCVMutation km = new KCVMutation(
                Lists.newArrayList(StaticArrayEntry.of(col, val)),
                Lists.newArrayList());

        Map<StaticBuffer, KCVMutation> keyColumnAndValue = ImmutableMap.of(key, km);

        Map<String, Map<StaticBuffer, KCVMutation>> mutations =
                ImmutableMap.of(
                        storeName1, keyColumnAndValue,
                        storeName2, keyColumnAndValue);

        manager.mutateMany(mutations, directTx);

        directTx.commit();

        KeySliceQuery query = new KeySliceQuery(key, col, nextCol);
        List<Entry> expected = ImmutableList.of(StaticArrayEntry.of(col, val));

        Assert.assertEquals(expected, store1.getSlice(query, tx));
        Assert.assertEquals(expected, store2.getSlice(query, tx));

    }

    @Test
    public void mutateManyStressTest() throws BackendException {

        mutateManyStressTestWithVariableRounds(5);
    }

    protected void mutateManyStressTestWithVariableRounds(int rounds) throws BackendException {
        Map<StaticBuffer, Map<StaticBuffer, StaticBuffer>> state = new HashMap<>();

        int dels = 1024;
        int adds = 4096;

        for (int round = 0; round < rounds; round++) {
            Map<StaticBuffer, KCVEntryMutation> changes = mutateState(state, dels, adds);

            applyChanges(changes, store1, tx);
            applyChanges(changes, store2, tx);
            newTx();

            int deletesExpected = 0 == round ? 0 : dels;

            int stateSizeExpected = adds + (adds - dels) * round;

            Assert.assertEquals(stateSizeExpected, checkThatStateExistsInStore(state, store1, round));
            Assert.assertEquals(deletesExpected, checkThatDeletionsApplied(changes, store1, round));

            Assert.assertEquals(stateSizeExpected, checkThatStateExistsInStore(state, store2, round));
            Assert.assertEquals(deletesExpected, checkThatDeletionsApplied(changes, store2, round));
        }
    }

    public void applyChanges(Map<StaticBuffer, KCVEntryMutation> changes, KCVSCache store, StoreTransaction tx) throws BackendException {
        for (Map.Entry<StaticBuffer, KCVEntryMutation> change : changes.entrySet()) {
            store.mutateEntries(change.getKey(), change.getValue().getAdditions(), change.getValue().getDeletions(), tx);
        }
    }

    public int checkThatStateExistsInStore(Map<StaticBuffer, Map<StaticBuffer, StaticBuffer>> state, KeyColumnValueStore store, int round) throws BackendException {
        int checked = 0;

        for (StaticBuffer key : state.keySet()) {
            for (StaticBuffer col : state.get(key).keySet()) {
                StaticBuffer val = state.get(key).get(col);

                Assert.assertEquals(val, KCVSUtil.get(store, key, col, tx));

                checked++;
            }
        }

        log.debug("Checked existence of {} key-column-value triples on round {}", checked, round);

        return checked;
    }

    public int checkThatDeletionsApplied(Map<StaticBuffer, KCVEntryMutation> changes, KeyColumnValueStore store, int round) throws BackendException {
        int checked = 0;
        int skipped = 0;

        for (StaticBuffer key : changes.keySet()) {
            KCVEntryMutation m = changes.get(key);

            if (!m.hasDeletions())
                continue;

            List<Entry> deletions = m.getDeletions();

            List<Entry> additions = m.getAdditions();

            for (Entry entry : deletions) {
                StaticBuffer col = entry.getColumn();

                if (null != additions && additions.contains(StaticArrayEntry.of(col, col))) {
                    skipped++;
                    continue;
                }

                Assert.assertNull(KCVSUtil.get(store, key, col, tx));

                checked++;
            }
        }

        log.debug("Checked absence of {} key-column-value deletions on round {} (skipped {})", new Object[]{checked, round, skipped});

        return checked;
    }

    /**
     * Pseudorandomly change the supplied {@code state}.
     * <p/>
     * This method removes {@code min(maxDeletionCount, S)} entries from the
     * maps in {@code state.values()}, where {@code S} is the sum of the sizes
     * of the maps in {@code state.values()}; this method then adds
     * {@code additionCount} pseudorandomly generated entries spread across
     * {@code state.values()}, potentially adding new keys to {@code state}
     * since they are randomly generated. This method then returns a map of keys
     * to Mutations representing the changes it has made to {@code state}.
     *
     * @param state            Maps keys -> columns -> values
     * @param maxDeletionCount Remove at most this many entries from state
     * @param additionCount    Add exactly this many entries to state
     * @return A KCVMutation map
     */
    public Map<StaticBuffer, KCVEntryMutation> mutateState(
            Map<StaticBuffer, Map<StaticBuffer, StaticBuffer>> state,
            int maxDeletionCount, int additionCount) {

        final int keyLength = 8;
        final int colLength = 16;

        Map<StaticBuffer, KCVEntryMutation> result = new HashMap<StaticBuffer, KCVEntryMutation>();

        // deletion pass
        int dels = 0;

        StaticBuffer key = null, col = null;
        Entry entry = null;

        Iterator<StaticBuffer> keyIter = state.keySet().iterator();

        while (keyIter.hasNext() && dels < maxDeletionCount) {
            key = keyIter.next();

            Iterator<Map.Entry<StaticBuffer,StaticBuffer>> colIter =
                    state.get(key).entrySet().iterator();

            while (colIter.hasNext() && dels < maxDeletionCount) {
                Map.Entry<StaticBuffer,StaticBuffer> colEntry = colIter.next();
                entry = StaticArrayEntry.of(colEntry.getKey(),colEntry.getValue());

                if (!result.containsKey(key)) {
                    KCVEntryMutation m = new KCVEntryMutation(new LinkedList<>(), new LinkedList<>());
                    result.put(key, m);
                }

                result.get(key).deletion(entry);

                dels++;

                colIter.remove();

                if (state.get(key).isEmpty()) {
                    assert !colIter.hasNext();
                    keyIter.remove();
                }
            }
        }

        // addition pass
        for (int i = 0; i < additionCount; i++) {

            while (true) {
                byte keyBuf[] = new byte[keyLength];
                rand.nextBytes(keyBuf);
                key = new StaticArrayBuffer(keyBuf);

                byte colBuf[] = new byte[colLength];
                rand.nextBytes(colBuf);
                col = new StaticArrayBuffer(colBuf);

                if (!state.containsKey(key) || !state.get(key).containsKey(col)) {
                    break;
                }
            }

            if (!state.containsKey(key)) {
                Map<StaticBuffer, StaticBuffer> m = new HashMap<>();
                state.put(key, m);
            }

            state.get(key).put(col, col);

            if (!result.containsKey(key)) {
                KCVEntryMutation m = new KCVEntryMutation(new LinkedList<>(), new LinkedList<>());
                result.put(key, m);
            }

            result.get(key).addition(StaticArrayEntry.of(col, col));

        }

        return result;
    }
}
