package com.thinkaurelius.titan.diskstorage;

import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.testcategory.PerformanceTests;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

@Category({ PerformanceTests.class })
public abstract class KeyColumnValueStorePerformance {

    private Logger log = LoggerFactory.getLogger(KeyColumnValueStoreTest.class);

    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    protected String storeName = "testStore1";

    public KeyColumnValueStoreManager manager;
    public StoreTransaction tx;
    public KeyColumnValueStore store;

    @Before
    public void setUp() throws Exception {
        openStorageManager().clearStorage();
        open();
    }

    public abstract KeyColumnValueStoreManager openStorageManager() throws StorageException;

    public void open() throws StorageException {
        manager = openStorageManager();
        store = manager.openDatabase(storeName);
        tx = startTx();
    }

    public StoreTransaction startTx() throws StorageException {
        return manager.beginTransaction(new StoreTxConfig());
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
        store.close();
        manager.close();
    }

    public static final int numRows = 1000;

    @Test
    public void addRecords() throws StorageException {
        for (int r = 0; r < numRows; r++) {
            int numCols = 10;
            List<Entry> entries = new ArrayList<Entry>();
            for (int c = 0; c < numCols; c++) {
                entries.add(new StaticBufferEntry(KeyValueStoreUtil.getBuffer(c + 1), KeyValueStoreUtil.getBuffer(c + r + 2)));
            }
            store.mutate(KeyValueStoreUtil.getBuffer(r + 1), entries, KeyColumnValueStore.NO_DELETIONS, tx);
        }
        tx.commit();
        tx = null;
    }

}
