package com.thinkaurelius.titan.diskstorage;


import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class KeyValueStoreTest {

    private Logger log = LoggerFactory.getLogger(KeyValueStoreTest.class);

    private int numKeys = 2000;
    private String storeName = "testStore1";


    protected KeyValueStoreManager manager;
    protected StoreTransaction tx;
    protected KeyValueStore store;

    @Before
    public void setUp() throws Exception {
        openStorageManager().clearStorage();
        open();
    }

    public void open() throws StorageException {
        manager = openStorageManager();
        store = manager.openDatabase(storeName);
        tx = manager.beginTransaction(ConsistencyLevel.DEFAULT);
    }

    public abstract KeyValueStoreManager openStorageManager() throws StorageException;

    @After
    public void tearDown() throws Exception {
        close();
    }

    public void close() throws StorageException {
        if (tx != null) tx.commit();
        store.close();
        manager.close();
    }

    public void clopen() throws StorageException {
        close();
        open();
    }

    @Test
    public void createDatabase() {
        //Just setup and shutdown
    }


    public String[] generateValues() {
        return KeyValueStoreUtil.generateData(numKeys);
    }

    public void loadValues(String[] values) throws StorageException {
        List<KeyValueEntry> entries = new ArrayList<KeyValueEntry>();
        for (int i = 0; i < numKeys; i++) {
            store.insert(KeyValueStoreUtil.getBuffer(i), KeyValueStoreUtil.getBuffer(values[i]), tx);
        }
    }

    public Set<Integer> deleteValues(int start, int every) throws StorageException {
        Set<Integer> removed = new HashSet<Integer>();
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        for (int i = start; i < numKeys; i = i + every) {
            removed.add(i);
            store.delete(KeyValueStoreUtil.getBuffer(i), tx);
        }
        return removed;
    }

    public void checkValueExistence(String[] values) throws StorageException {
        checkValueExistence(values, new HashSet<Integer>());
    }

    public void checkValueExistence(String[] values, Set<Integer> removed) throws StorageException {
        for (int i = 0; i < numKeys; i++) {
            boolean result = store.containsKey(KeyValueStoreUtil.getBuffer(i), tx);
            if (removed.contains(i)) {
                Assert.assertFalse(result);
            } else {
                Assert.assertTrue(result);
            }
        }
    }

    public void checkValues(String[] values) throws StorageException {
        checkValues(values, new HashSet<Integer>());
    }

    public void checkValues(String[] values, Set<Integer> removed) throws StorageException {
        for (int i = 0; i < numKeys; i++) {
            ByteBuffer result = store.get(KeyValueStoreUtil.getBuffer(i), tx);
            if (removed.contains(i)) {
                Assert.assertNull(result);
            } else {
                Assert.assertEquals(values[i], KeyValueStoreUtil.getString(result));
            }
        }
    }

    @Test
    public void storeAndRetrieve() throws StorageException {
        String[] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);

        log.debug("Checking values...");
        checkValueExistence(values);
        checkValues(values);
    }

    @Test
    public void storeAndRetrieveWithClosing() throws StorageException {
        String[] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        clopen();
        log.debug("Checking values...");
        checkValueExistence(values);
        checkValues(values);
    }

    @Test
    public void deletionTest1() throws StorageException {
        String[] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        clopen();
        Set<Integer> deleted = deleteValues(0, 10);
        log.debug("Checking values...");
        checkValueExistence(values, deleted);
        checkValues(values, deleted);
    }

    @Test
    public void deletionTest2() throws StorageException {
        String[] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        Set<Integer> deleted = deleteValues(0, 10);
        clopen();
        log.debug("Checking values...");
        checkValueExistence(values, deleted);
        checkValues(values, deleted);
    }

    @Test
    public void scanTest() throws StorageException {
        if (manager.getFeatures().supportsScan()) {
            String[] values = generateValues();
            loadValues(values);
            RecordIterator<ByteBuffer> iterator0 = store.getKeys(tx);
            Assert.assertEquals(numKeys, KeyValueStoreUtil.count(iterator0));
            clopen();
            RecordIterator<ByteBuffer> iterator1 = store.getKeys(tx);
            RecordIterator<ByteBuffer> iterator2 = store.getKeys(tx);
            RecordIterator<ByteBuffer> iterator3 = store.getKeys(tx);
            Assert.assertEquals(numKeys, KeyValueStoreUtil.count(iterator1));
            Assert.assertEquals(numKeys, KeyValueStoreUtil.count(iterator2));
        }
    }

    public void checkSlice(String[] values, Set<Integer> removed, int start, int end, int limit) throws StorageException {
        List<KeyValueEntry> entries;
        if (limit <= 0)
            entries = store.getSlice(KeyValueStoreUtil.getBuffer(start), KeyValueStoreUtil.getBuffer(end), tx);
        else
            entries = store.getSlice(KeyValueStoreUtil.getBuffer(start), KeyValueStoreUtil.getBuffer(end), limit, tx);

        int pos = 0;
        for (int i = start; i < end; i++) {
            if (removed.contains(i)) continue;
            if (pos < limit) {
                KeyValueEntry entry = entries.get(pos);
                int id = KeyValueStoreUtil.getID(entry.getKey());
                String str = KeyValueStoreUtil.getString(entry.getValue());
                Assert.assertEquals(i, id);
                Assert.assertEquals(values[i], str);
            }
            pos++;
        }
        if (limit > 0 && pos >= limit) Assert.assertEquals(limit, entries.size());
        else {
            Assert.assertNotNull(entries);
            Assert.assertEquals(pos, entries.size());
        }
    }

    @Test
    public void intervalTest1() throws StorageException {
        String[] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        Set<Integer> deleted = deleteValues(0, 10);
        clopen();
        checkSlice(values, deleted, 5, 25, -1);
        checkSlice(values, deleted, 5, 250, 10);
        checkSlice(values, deleted, 500, 1250, -1);
        checkSlice(values, deleted, 500, 1250, 1000);
        checkSlice(values, deleted, 500, 1250, 100);
        checkSlice(values, deleted, 50, 20, 10);
        checkSlice(values, deleted, 50, 20, -1);

    }


}
 