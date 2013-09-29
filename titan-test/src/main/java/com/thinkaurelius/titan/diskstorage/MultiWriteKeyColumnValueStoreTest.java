package com.thinkaurelius.titan.diskstorage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;

import static com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore.*;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class MultiWriteKeyColumnValueStoreTest {

    private Logger log = LoggerFactory.getLogger(MultiWriteKeyColumnValueStoreTest.class);

    int numKeys = 500;
    int numColumns = 50;

    int bufferSize = 20;

    protected String storeName1 = "testStore1";
    private KeyColumnValueStore store1;
    protected String storeName2 = "testStore2";
    private KeyColumnValueStore store2;


    public KeyColumnValueStoreManager manager;
    public StoreTransaction tx;


    private Random rand = new Random(10);

    @Before
    public void setUp() throws Exception {
        openStorageManager().clearStorage();
        open();
    }

    public abstract KeyColumnValueStoreManager openStorageManager() throws StorageException;

    public void open() throws StorageException {
        manager = openStorageManager();
        Assert.assertTrue(manager.getFeatures().supportsBatchMutation());
        tx = new BufferTransaction(manager.beginTransaction(new StoreTxConfig()), manager, bufferSize, 1, 0);
        store1 = new BufferedKeyColumnValueStore(manager.openDatabase(storeName1), true);
        store2 = new BufferedKeyColumnValueStore(manager.openDatabase(storeName2), true);

    }

    public void clopen() throws StorageException {
        close();
        open();
    }

    @After
    public void tearDown() throws Exception {
        close();
    }

    public void close() throws StorageException {
        if (tx != null) tx.commit();
        if (null != store1) store1.close();
        if (null != store2) store2.close();
        if (null != manager) manager.close();
    }

    @Test
    public void deletionsAppliedBeforeAdditions() throws StorageException {

        StaticBuffer b1 = KeyColumnValueStoreUtil.longToByteBuffer(1);

        Assert.assertNull(KCVSUtil.get(store1, b1, b1, tx));

        List<Entry> additions = Arrays.<Entry>asList(new StaticBufferEntry(b1, b1));

        List<StaticBuffer> deletions = Arrays.asList(b1);

        Map<StaticBuffer, KCVMutation> combination = new HashMap<StaticBuffer, KCVMutation>(1);
        Map<StaticBuffer, KCVMutation> deleteOnly = new HashMap<StaticBuffer, KCVMutation>(1);
        Map<StaticBuffer, KCVMutation> addOnly = new HashMap<StaticBuffer, KCVMutation>(1);

        combination.put(b1, new KCVMutation(additions, deletions));
        deleteOnly.put(b1, new KCVMutation(KeyColumnValueStore.NO_ADDITIONS, deletions));
        addOnly.put(b1, new KCVMutation(additions, KeyColumnValueStore.NO_DELETIONS));

        store1.mutate(b1, additions, deletions, tx);
        tx.flush();

        StaticBuffer result = KCVSUtil.get(store1, b1, b1, tx);

        Assert.assertEquals(b1, result);

        store1.mutate(b1, NO_ADDITIONS, deletions, tx);
        tx.flush();

        for (int i = 0; i < 100; i++) {
            StaticBuffer n = KCVSUtil.get(store1, b1, b1, tx);
            Assert.assertNull(n);
            store1.mutate(b1, additions, NO_DELETIONS, tx);
            tx.flush();
            store1.mutate(b1, NO_ADDITIONS, deletions, tx);
            tx.flush();
            n = KCVSUtil.get(store1, b1, b1, tx);
            Assert.assertNull(n);
        }

        for (int i = 0; i < 100; i++) {
            store1.mutate(b1, NO_ADDITIONS, deletions, tx);
            tx.flush();
            store1.mutate(b1, additions, NO_DELETIONS, tx);
            tx.flush();
            Assert.assertEquals(b1, KCVSUtil.get(store1, b1, b1, tx));
        }

        for (int i = 0; i < 100; i++) {
            store1.mutate(b1, additions, deletions, tx);
            tx.flush();
            Assert.assertEquals(b1, KCVSUtil.get(store1, b1, b1, tx));
        }
    }

    @Test
    public void mutateManyWritesSameKeyOnMultipleCFs() throws StorageException {

        final long arbitraryLong = 42;
        assert 0 < arbitraryLong;

        final StaticBuffer key = KeyColumnValueStoreUtil.longToByteBuffer(arbitraryLong * arbitraryLong);
        final StaticBuffer val = KeyColumnValueStoreUtil.longToByteBuffer(arbitraryLong * arbitraryLong * arbitraryLong);
        final StaticBuffer col = KeyColumnValueStoreUtil.longToByteBuffer(arbitraryLong);
        final StaticBuffer nextCol = KeyColumnValueStoreUtil.longToByteBuffer(arbitraryLong + 1);

        final StoreTransaction directTx = manager.beginTransaction(new StoreTxConfig());

        KCVMutation km = new KCVMutation(
                ImmutableList.<Entry>of(new StaticBufferEntry(col, val)),
                ImmutableList.<StaticBuffer>of());

        Map<StaticBuffer, KCVMutation> keyColumnAndValue = ImmutableMap.of(key, km);

        Map<String, Map<StaticBuffer, KCVMutation>> mutations =
                ImmutableMap.of(
                        storeName1, keyColumnAndValue,
                        storeName2, keyColumnAndValue);

        manager.mutateMany(mutations, directTx);

        directTx.commit();

        KeySliceQuery query = new KeySliceQuery(key, col, nextCol);
        List<Entry> expected =
                ImmutableList.<Entry>of(new StaticBufferEntry(col, val));

        Assert.assertEquals(expected, store1.getSlice(query, tx));
        Assert.assertEquals(expected, store2.getSlice(query, tx));

    }

    @Test
    public void mutateManyStressTest() throws StorageException {

        Map<StaticBuffer, Map<StaticBuffer, StaticBuffer>> state =
                new HashMap<StaticBuffer, Map<StaticBuffer, StaticBuffer>>();

        int dels = 1024;
        int adds = 4096;

        for (int round = 0; round < 5; round++) {
            Map<StaticBuffer, KCVMutation> changes = mutateState(state, dels, adds);

            applyChanges(changes, store1, tx);
            applyChanges(changes, store2, tx);
            tx.flush();

            int deletesExpected = 0 == round ? 0 : dels;

            int stateSizeExpected = adds + (adds - dels) * round;

            Assert.assertEquals(stateSizeExpected, checkThatStateExistsInStore(state, store1, round));
            Assert.assertEquals(deletesExpected, checkThatDeletionsApplied(changes, store1, round));

            Assert.assertEquals(stateSizeExpected, checkThatStateExistsInStore(state, store2, round));
            Assert.assertEquals(deletesExpected, checkThatDeletionsApplied(changes, store2, round));
        }
    }

    public void applyChanges(Map<StaticBuffer, KCVMutation> changes, KeyColumnValueStore store, StoreTransaction tx) throws StorageException {
        for (Map.Entry<StaticBuffer, KCVMutation> change : changes.entrySet()) {
            store.mutate(change.getKey(), change.getValue().getAdditions(), change.getValue().getDeletions(), tx);
        }
    }

    public int checkThatStateExistsInStore(Map<StaticBuffer, Map<StaticBuffer, StaticBuffer>> state, KeyColumnValueStore store, int round) throws StorageException {
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

    public int checkThatDeletionsApplied(Map<StaticBuffer, KCVMutation> changes, KeyColumnValueStore store, int round) throws StorageException {
        int checked = 0;
        int skipped = 0;

        for (StaticBuffer key : changes.keySet()) {
            KCVMutation m = changes.get(key);

            if (!m.hasDeletions())
                continue;

            List<StaticBuffer> deletions = m.getDeletions();

            List<Entry> additions = m.getAdditions();

            for (StaticBuffer col : deletions) {

                if (null != additions && additions.contains(new StaticBufferEntry(col, col))) {
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
    public Map<StaticBuffer, KCVMutation> mutateState(
            Map<StaticBuffer, Map<StaticBuffer, StaticBuffer>> state,
            int maxDeletionCount, int additionCount) {

        final int keyLength = 8;
        final int colLength = 16;

        Map<StaticBuffer, KCVMutation> result = new HashMap<StaticBuffer, KCVMutation>();

        // deletion pass
        int dels = 0;

        StaticBuffer key = null, col = null;

        Iterator<StaticBuffer> keyIter = state.keySet().iterator();

        while (keyIter.hasNext() && dels < maxDeletionCount) {
            key = keyIter.next();

            Iterator<StaticBuffer> colIter =
                    state.get(key).keySet().iterator();

            while (colIter.hasNext() && dels < maxDeletionCount) {
                col = colIter.next();

                if (!result.containsKey(key)) {
                    KCVMutation m = new KCVMutation(new LinkedList<Entry>(),
                            new LinkedList<StaticBuffer>());
                    result.put(key, m);
                }

                result.get(key).deletion(col);

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
                Map<StaticBuffer, StaticBuffer> m = new HashMap<StaticBuffer, StaticBuffer>();
                state.put(key, m);
            }

            state.get(key).put(col, col);

            if (!result.containsKey(key)) {
                KCVMutation m = new KCVMutation(new LinkedList<Entry>(),
                        new LinkedList<StaticBuffer>());
                result.put(key, m);
            }

            result.get(key).addition(new StaticBufferEntry(col, col));

        }

        return result;
    }

    public Map<StaticBuffer, KCVMutation> generateMutation(int keyCount, int columnCount, Map<StaticBuffer, KCVMutation> deleteFrom) {
        Map<StaticBuffer, KCVMutation> result = new HashMap<StaticBuffer, KCVMutation>(keyCount);

        Random keyRand = new Random(keyCount);
        Random colRand = new Random(columnCount);

        final int keyLength = 8;
        final int colLength = 6;

        Iterator<Map.Entry<StaticBuffer, KCVMutation>> deleteIter = null;
        List<Entry> lastDeleteIterResult = null;

        if (null != deleteFrom) {
            deleteIter = deleteFrom.entrySet().iterator();
        }

        for (int ik = 0; ik < keyCount; ik++) {
            byte keyBuf[] = new byte[keyLength];
            keyRand.nextBytes(keyBuf);
            StaticBuffer key = new StaticArrayBuffer(keyBuf);

            List<Entry> additions = new LinkedList<Entry>();
            List<StaticBuffer> deletions = new LinkedList<StaticBuffer>();

            for (int ic = 0; ic < columnCount; ic++) {

                boolean deleteSucceeded = false;
                if (null != deleteIter && 1 == ic % 2) {

                    if (null == lastDeleteIterResult || lastDeleteIterResult.isEmpty()) {
                        while (deleteIter.hasNext()) {
                            Map.Entry<StaticBuffer, KCVMutation> ent = deleteIter.next();
                            if (ent.getValue().hasAdditions() && !ent.getValue().getAdditions().isEmpty()) {
                                lastDeleteIterResult = ent.getValue().getAdditions();
                                break;
                            }
                        }
                    }


                    if (null != lastDeleteIterResult && !lastDeleteIterResult.isEmpty()) {
                        Entry e = lastDeleteIterResult.get(0);
                        lastDeleteIterResult.remove(0);
                        deletions.add(e.getColumn());
                        deleteSucceeded = true;
                    }
                }

                if (!deleteSucceeded) {
                    byte colBuf[] = new byte[colLength];
                    colRand.nextBytes(colBuf);
                    StaticBuffer col = new StaticArrayBuffer(colBuf);

                    additions.add(new StaticBufferEntry(col, col));
                }

            }

            KCVMutation m = new KCVMutation(additions, deletions);

            result.put(key, m);
        }

        return result;
    }
}
