package com.thinkaurelius.titan.diskstorage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.*;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CacheStoreTest {
    protected static final Logger logger = LoggerFactory.getLogger(KeyValueStoreTest.class);

    protected static final int NUM_KEYS = 2000;
    protected static final String STORE_NAME = "testCacheStore1";

    protected final CacheStoreManager manager;
    protected final CacheStore store;

    protected StoreTransaction tx;

    public CacheStoreTest(CacheStoreManager manager) throws StorageException {
        this.manager = manager;
        this.store = manager.openDatabase(STORE_NAME);
    }

    @Before
    public void setUp() throws Exception {
        tx = manager.beginTransaction(new StoreTxConfig());
    }

    @After
    public void tearDown() throws Exception {
        if (tx != null)
            tx.commit();

        store.clearStore();
    }

    @Test
    public void insertAndRetrieve() throws StorageException {
        String[] values = generateValues();
        logger.debug("Loading values...");
        loadValues(values);

        logger.debug("Checking values...");
        checkValueExistence(values);
        checkValues(values);
    }

    @Test
    public void replaceTest() throws StorageException {

        StaticBuffer key = new StaticArrayBuffer("replacement_test_key".getBytes());
        byte[] initialValue = "initial value".getBytes();
        byte[] replacementV = "replacement".getBytes();

        store.replace(key, new StaticArrayBuffer(initialValue), null, tx);

        // let's first check if it was successfully added as a new value
        Assert.assertTrue(store.containsKey(key, tx));
        Assert.assertArrayEquals(initialValue, store.get(key, tx).as(StaticArrayBuffer.ARRAY_FACTORY));

        // now let's commit transaction and check if the key is this around
        commitTransaction();

        // let's first check if value was retained after first transaction
        Assert.assertTrue(store.containsKey(key, tx));
        Assert.assertArrayEquals(initialValue, store.get(key, tx).as(StaticArrayBuffer.ARRAY_FACTORY));

        store.replace(key, new StaticArrayBuffer(replacementV), new StaticArrayBuffer(initialValue), tx);

        // let's check if the value was replaced correctly
        Assert.assertArrayEquals(replacementV, store.get(key, tx).as(StaticArrayBuffer.ARRAY_FACTORY));

        // now let's commit transaction and check if update would be retained
        commitTransaction();

        // let's check if the value was replaced correctly
        Assert.assertArrayEquals(replacementV, store.get(key, tx).as(StaticArrayBuffer.ARRAY_FACTORY));
    }

    @Test(expected = CacheUpdateException.class)
    public void replacementFailureTest() throws StorageException {
        store.replace(new StaticArrayBuffer("no_key".getBytes()), new StaticArrayBuffer("v2".getBytes()), new StaticArrayBuffer("v1".getBytes()), tx);
    }

    @Test
    public void deleteTest() throws StorageException {
        String[] values = generateValues();
        logger.debug("Loading values...");
        loadValues(values);

        Set<Integer> deleted = deleteValues(0, 10);
        logger.debug("Checking values...");
        checkValueExistence(values, deleted);
        checkValues(values, deleted);
    }

    @Test
    public void scanSelectorTest() throws StorageException {
        String[] values = generateValues();
        loadValues(values);

        // basic tests
        Assert.assertEquals(NUM_KEYS, KeyValueStoreUtil.count(store.getKeys(KeySelector.SelectAll, tx)));
        Assert.assertEquals(0, KeyValueStoreUtil.count(store.getKeys(new LimitedToZeroSelector(), tx)));
        Assert.assertEquals(0, KeyValueStoreUtil.count(store.getKeys(new NeverIncludingSelector(), tx)));
        Assert.assertEquals(NUM_KEYS / 2, KeyValueStoreUtil.count(store.getKeys(new HalfOfDataSetSelector(NUM_KEYS), tx)));

        // following tries to determine if we are able to slice portion of the dataset with condition and/or limit

        StaticBuffer startKey = KeyValueStoreUtil.getBuffer(1423);
        StaticBuffer endKey = KeyValueStoreUtil.getBuffer(1433);

        List<StaticBuffer> slicedKeys = new ArrayList<StaticBuffer>(10);
        RecordIterator<KeyValueEntry> keys = store.getKeys(new ComplexSelector(startKey, endKey, Integer.MAX_VALUE), tx);

        while (keys.hasNext())
            slicedKeys.add(keys.next().getKey());

        // let's check the the number of keys is current as well as keys
        Assert.assertEquals(10, slicedKeys.size());
        for (int i = 1423; i < 1433; i++) {
            Assert.assertTrue(slicedKeys.contains(KeyValueStoreUtil.getBuffer(i)));
        }

        Assert.assertEquals(7, KeyValueStoreUtil.count(store.getKeys(new ComplexSelector(startKey, endKey, 7), tx)));
    }

    public String[] generateValues() {
        return KeyValueStoreUtil.generateData(NUM_KEYS);
    }

    public void loadValues(String[] values) throws StorageException {
        for (int i = 0; i < NUM_KEYS; i++) {
            store.replace(KeyValueStoreUtil.getBuffer(i), KeyValueStoreUtil.getBuffer(values[i]), null, tx);
        }
    }

    public Set<Integer> deleteValues(int start, int every) throws StorageException {
        Set<Integer> removed = new HashSet<Integer>();
        for (int i = start; i < NUM_KEYS; i = i + every) {
            removed.add(i);
            store.delete(KeyValueStoreUtil.getBuffer(i), tx);
        }
        return removed;
    }

    public void checkValueExistence(String[] values) throws StorageException {
        checkValueExistence(values, new HashSet<Integer>());
    }

    public void checkValueExistence(String[] values, Set<Integer> removed) throws StorageException {
        for (int i = 0; i < NUM_KEYS; i++) {
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
        for (int i = 0; i < NUM_KEYS; i++) {
            StaticBuffer result = store.get(KeyValueStoreUtil.getBuffer(i), tx);
            if (removed.contains(i)) {
                Assert.assertNull(result);
            } else {
                Assert.assertEquals(values[i], KeyValueStoreUtil.getString(result));
            }
        }
    }

    private void commitTransaction() throws StorageException {
        assert tx != null;

        tx.commit();
        tx = manager.beginTransaction(new StoreTxConfig());
    }

    private static class LimitedToZeroSelector implements KeySelector {
        @Override
        public boolean include(StaticBuffer key) {
            return true;
        }

        @Override
        public boolean reachedLimit() {
            return true;
        }
    }

    private static class NeverIncludingSelector implements KeySelector {
        @Override
        public boolean include(StaticBuffer key) {
            return false;
        }

        @Override
        public boolean reachedLimit() {
            return true;
        }
    }

    private static class HalfOfDataSetSelector implements KeySelector {
        private int count = 0;
        private final int limit;

        public HalfOfDataSetSelector(int numRecords) {
            this.limit = numRecords / 2;
        }

        @Override
        public boolean include(StaticBuffer key) {
            count++;
            return true;
        }

        @Override
        public boolean reachedLimit() {
            return count > limit;
        }
    }

    private static class ComplexSelector implements KeySelector {
        private final StaticBuffer keyStart, keyEnd;
        private final int limit;

        private int count = 0;

        public ComplexSelector(StaticBuffer startKey, StaticBuffer endKey, int limit) {
            this.keyStart = startKey;
            this.keyEnd = endKey;
            this.limit = limit;
        }

        @Override
        public boolean include(StaticBuffer key) {
            if (key.compareTo(keyStart) >= 0 && key.compareTo(keyEnd) < 0) {
                count++;
                return true;
            }

            return false;
        }

        @Override
        public boolean reachedLimit() {
            return count > limit;
        }
    }
}
