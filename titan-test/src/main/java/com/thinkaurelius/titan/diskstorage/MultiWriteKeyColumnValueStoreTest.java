package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
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
        tx = new BufferTransaction(manager.beginTransaction(ConsistencyLevel.DEFAULT), manager, bufferSize, 1, 0);
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

        ByteBuffer b1 = ByteBuffer.allocate(1);
        b1.put((byte) 1).rewind();

        Assert.assertNull(store1.get(b1, b1, tx));

        List<Entry> additions = Arrays.asList(SimpleEntry.of(b1, b1));

        List<ByteBuffer> deletions = Arrays.asList(b1);

        Map<ByteBuffer, KCVMutation> combination = new HashMap<ByteBuffer, KCVMutation>(1);
        Map<ByteBuffer, KCVMutation> deleteOnly = new HashMap<ByteBuffer, KCVMutation>(1);
        Map<ByteBuffer, KCVMutation> addOnly = new HashMap<ByteBuffer, KCVMutation>(1);

        combination.put(b1, new KCVMutation(additions, deletions));
        deleteOnly.put(b1, new KCVMutation(null, deletions));
        addOnly.put(b1, new KCVMutation(additions, null));

        store1.mutate(b1, additions, deletions, tx);
        tx.flush();

        ByteBuffer result = store1.get(b1, b1, tx);

        Assert.assertEquals(b1, result);

        store1.mutate(b1, null, deletions, tx);
        tx.flush();

        for (int i = 0; i < 100; i++) {
            ByteBuffer n = store1.get(b1, b1, tx);
            Assert.assertNull(n);
            store1.mutate(b1, additions, null, tx);
            tx.flush();
            store1.mutate(b1, null, deletions, tx);
            tx.flush();
            n = store1.get(b1, b1, tx);
            Assert.assertNull(n);
        }

        for (int i = 0; i < 100; i++) {
            store1.mutate(b1, null, deletions, tx);
            tx.flush();
            store1.mutate(b1, additions, null, tx);
            tx.flush();
            Assert.assertEquals(b1, store1.get(b1, b1, tx));
        }

        for (int i = 0; i < 100; i++) {
            store1.mutate(b1, additions, deletions, tx);
            tx.flush();
            Assert.assertEquals(b1, store1.get(b1, b1, tx));
        }
    }

    @Test
    public void mutateManyStressTest() throws StorageException {

        Map<ByteBuffer, Map<ByteBuffer, ByteBuffer>> state =
                new HashMap<ByteBuffer, Map<ByteBuffer, ByteBuffer>>();

        int dels = 1024;
        int adds = 4096;

        for (int round = 0; round < 5; round++) {
            Map<ByteBuffer, KCVMutation> changes = mutateState(state, dels, adds);

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

    public void applyChanges(Map<ByteBuffer, KCVMutation> changes, KeyColumnValueStore store, StoreTransaction tx) throws StorageException {
        for (Map.Entry<ByteBuffer, KCVMutation> change : changes.entrySet()) {
            store.mutate(change.getKey(), change.getValue().getAdditions(), change.getValue().getDeletions(), tx);
        }
    }

    public int checkThatStateExistsInStore(Map<ByteBuffer, Map<ByteBuffer, ByteBuffer>> state, KeyColumnValueStore store, int round) throws StorageException {
        int checked = 0;

        for (ByteBuffer key : state.keySet()) {
            for (ByteBuffer col : state.get(key).keySet()) {
                ByteBuffer val = state.get(key).get(col);

                Assert.assertEquals(val, store.get(key, col, tx));

                checked++;
            }
        }

        log.debug("Checked existence of {} key-column-value triples on round {}", checked, round);

        return checked;
    }

    public int checkThatDeletionsApplied(Map<ByteBuffer, KCVMutation> changes, KeyColumnValueStore store, int round) throws StorageException {
        int checked = 0;
        int skipped = 0;

        for (ByteBuffer key : changes.keySet()) {
            KCVMutation m = changes.get(key);

            if (!m.hasDeletions())
                continue;

            List<ByteBuffer> deletions = m.getDeletions();

            List<Entry> additions = m.getAdditions();

            for (ByteBuffer col : deletions) {

                if (null != additions && additions.contains(SimpleEntry.of(col, col))) {
                    skipped++;
                    continue;
                }

                Assert.assertNull(store.get(key, col, tx));

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
    public Map<ByteBuffer, KCVMutation> mutateState(
            Map<ByteBuffer, Map<ByteBuffer, ByteBuffer>> state,
            int maxDeletionCount, int additionCount) {

        final int keyLength = 8;
        final int colLength = 16;

        Map<ByteBuffer, KCVMutation> result = new HashMap<ByteBuffer, KCVMutation>();

        // deletion pass
        int dels = 0;

        ByteBuffer key = null, col = null;

        Iterator<ByteBuffer> keyIter = state.keySet().iterator();

        while (keyIter.hasNext() && dels < maxDeletionCount) {
            key = keyIter.next();

            Iterator<ByteBuffer> colIter =
                    state.get(key).keySet().iterator();

            while (colIter.hasNext() && dels < maxDeletionCount) {
                col = colIter.next();

                if (!result.containsKey(key)) {
                    KCVMutation m = new KCVMutation(new LinkedList<Entry>(),
                            new LinkedList<ByteBuffer>());
                    result.put(key, m);
                }

                result.get(key).getDeletions().add(col);

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
                key = ByteBuffer.wrap(keyBuf);

                byte colBuf[] = new byte[colLength];
                rand.nextBytes(colBuf);
                col = ByteBuffer.wrap(colBuf);

                if (!state.containsKey(key) || !state.get(key).containsKey(col)) {
                    break;
                }
            }

            if (!state.containsKey(key)) {
                Map<ByteBuffer, ByteBuffer> m = new HashMap<ByteBuffer, ByteBuffer>();
                state.put(key, m);
            }

            state.get(key).put(col, col);

            if (!result.containsKey(key)) {
                KCVMutation m = new KCVMutation(new LinkedList<Entry>(),
                        new LinkedList<ByteBuffer>());
                result.put(key, m);
            }

            result.get(key).getAdditions().add(SimpleEntry.of(col, col));

        }

        return result;
    }

    public Map<ByteBuffer, KCVMutation> generateMutation(int keyCount, int columnCount, Map<ByteBuffer, KCVMutation> deleteFrom) {
        Map<ByteBuffer, KCVMutation> result = new HashMap<ByteBuffer, KCVMutation>(keyCount);

        Random keyRand = new Random(keyCount);
        Random colRand = new Random(columnCount);

        final int keyLength = 8;
        final int colLength = 6;

        Iterator<Map.Entry<ByteBuffer, KCVMutation>> deleteIter = null;
        List<Entry> lastDeleteIterResult = null;

        if (null != deleteFrom) {
            deleteIter = deleteFrom.entrySet().iterator();
        }

        for (int ik = 0; ik < keyCount; ik++) {
            byte keyBuf[] = new byte[keyLength];
            keyRand.nextBytes(keyBuf);
            ByteBuffer key = ByteBuffer.wrap(keyBuf);

            List<Entry> additions = new LinkedList<Entry>();
            List<ByteBuffer> deletions = new LinkedList<ByteBuffer>();

            for (int ic = 0; ic < columnCount; ic++) {

                boolean deleteSucceeded = false;
                if (null != deleteIter && 1 == ic % 2) {

                    if (null == lastDeleteIterResult || lastDeleteIterResult.isEmpty()) {
                        while (deleteIter.hasNext()) {
                            Map.Entry<ByteBuffer, KCVMutation> ent = deleteIter.next();
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
                    ByteBuffer col = ByteBuffer.wrap(colBuf);

                    additions.add(SimpleEntry.of(col, col));
                }

            }

            KCVMutation m = new KCVMutation(additions, deletions);

            result.put(key, m);
        }

        return result;
    }
}
