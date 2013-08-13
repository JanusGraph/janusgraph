package com.thinkaurelius.titan.diskstorage;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.thinkaurelius.titan.diskstorage.util.ReadArrayBuffer;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.testutil.RandomGenerator;

public abstract class KeyColumnValueStoreTest {
    private Logger log = LoggerFactory.getLogger(KeyColumnValueStoreTest.class);

    int numKeys = 500;
    int numColumns = 50;

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
        tx = manager.beginTransaction(ConsistencyLevel.DEFAULT);
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

    @Test
    public void createDatabase() {
        //Just setup and shutdown
    }


    public String[][] generateValues() {
        return KeyValueStoreUtil.generateData(numKeys, numColumns);
    }

    public void loadValues(String[][] values) throws StorageException {
        for (int i = 0; i < values.length; i++) {
            List<Entry> entries = new ArrayList<Entry>();
            for (int j = 0; j < values[i].length; j++) {
                entries.add(new StaticBufferEntry(KeyValueStoreUtil.getBuffer(j), KeyValueStoreUtil.getBuffer(values[i][j])));
            }
            store.mutate(KeyValueStoreUtil.getBuffer(i), entries, KeyColumnValueStore.NO_DELETIONS, tx);
        }
    }

    public Set<KeyColumn> deleteValues(int every) throws StorageException {
        Set<KeyColumn> removed = new HashSet<KeyColumn>();
        int counter = 0;
        for (int i = 0; i < numKeys; i++) {
            List<StaticBuffer> deletions = new ArrayList<StaticBuffer>();
            for (int j = 0; j < numColumns; j++) {
                counter++;
                if (counter % every == 0) {
                    //remove
                    removed.add(new KeyColumn(i, j));
                    deletions.add(KeyValueStoreUtil.getBuffer(j));
                }
            }
            store.mutate(KeyValueStoreUtil.getBuffer(i), KeyColumnValueStore.NO_ADDITIONS, deletions, tx);
        }
        return removed;
    }

    public Set<Integer> deleteKeys(int every) throws StorageException {
        Set<Integer> removed = new HashSet<Integer>();
        for (int i = 0; i < numKeys; i++) {
            if (i % every == 0) {
                removed.add(i);
                List<StaticBuffer> deletions = new ArrayList<StaticBuffer>();
                for (int j = 0; j < numColumns; j++) {
                    deletions.add(KeyValueStoreUtil.getBuffer(j));
                }
                store.mutate(KeyValueStoreUtil.getBuffer(i), KeyColumnValueStore.NO_ADDITIONS, deletions, tx);
            }
        }
        return removed;
    }

    public void checkKeys(Set<Integer> removed) throws StorageException {
        for (int i = 0; i < numKeys; i++) {
            if (removed.contains(i)) {
                Assert.assertFalse(store.containsKey(KeyValueStoreUtil.getBuffer(i), tx));
            } else {
                Assert.assertTrue(store.containsKey(KeyValueStoreUtil.getBuffer(i), tx));
            }
        }
    }

    public void checkValueExistence(String[][] values) throws StorageException {
        checkValueExistence(values, new HashSet<KeyColumn>());
    }

    public void checkValueExistence(String[][] values, Set<KeyColumn> removed) throws StorageException {
        for (int i = 0; i < numKeys; i++) {
            for (int j = 0; j < numColumns; j++) {
                boolean result = KCVSUtil.containsKeyColumn(store, KeyValueStoreUtil.getBuffer(i), KeyValueStoreUtil.getBuffer(j), tx);
                if (removed.contains(new KeyColumn(i, j))) {
                    Assert.assertFalse(result);
                } else {
                    Assert.assertTrue(result);
                }
            }
        }
    }

    public void checkValues(String[][] values) throws StorageException {
        checkValues(values, new HashSet<KeyColumn>());
    }

    public void checkValues(String[][] values, Set<KeyColumn> removed) throws StorageException {
        for (int i = 0; i < numKeys; i++) {
            for (int j = 0; j < numColumns; j++) {
                StaticBuffer result = KCVSUtil.get(store,KeyValueStoreUtil.getBuffer(i), KeyValueStoreUtil.getBuffer(j), tx);
                if (removed.contains(new KeyColumn(i, j))) {
                    Assert.assertNull(result);
                } else {
                    Assert.assertEquals(values[i][j], KeyValueStoreUtil.getString(result));
                }
            }
        }

    }

    @Test
    public void storeAndRetrieve() throws StorageException {
        String[][] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        //print(values);
        log.debug("Checking values...");
        checkValueExistence(values);
        checkValues(values);
    }

    @Test
    public void storeAndRetrievePerformance() throws StorageException {
        int multiplier = 4;
        int keys = 50*multiplier, columns = 2000;
        String[][] values = KeyValueStoreUtil.generateData(keys,columns);
        log.debug("Loading values: "+keys+"x"+columns);
        long time = System.currentTimeMillis();
        loadValues(values);
        System.out.println("Loading time (ms): " + (System.currentTimeMillis()-time));
        //print(values);
        Random r = new Random();
        int trials = 500*multiplier;
        int delta = 10;
        log.debug("Reading values: "+trials+" trials");
        time = System.currentTimeMillis();
        for (int t=0;t<trials;t++) {
            int key = r.nextInt(keys);
            int start = r.nextInt(columns-delta);
            store.getSlice(new KeySliceQuery(KeyValueStoreUtil.getBuffer(key), KeyValueStoreUtil.getBuffer(start), KeyValueStoreUtil.getBuffer(start+delta)), tx);
        }
        System.out.println("Reading time (ms): " + (System.currentTimeMillis()-time));
    }

    @Test
    public void storeAndRetrieveWithClosing() throws StorageException {
        String[][] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        clopen();
        log.debug("Checking values...");
        checkValueExistence(values);
        checkValues(values);
    }

    @Test
    public void deleteColumnsTest1() throws StorageException {
        String[][] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        clopen();
        Set<KeyColumn> deleted = deleteValues(7);
        log.debug("Checking values...");
        checkValueExistence(values, deleted);
        checkValues(values, deleted);
    }

    @Test
    public void deleteColumnsTest2() throws StorageException {
        String[][] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        Set<KeyColumn> deleted = deleteValues(7);
        clopen();
        log.debug("Checking values...");
        checkValueExistence(values, deleted);
        checkValues(values, deleted);
    }

    @Test
    public void deleteKeys() throws StorageException {
        String[][] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        Set<Integer> deleted = deleteKeys(11);
        clopen();
        checkKeys(deleted);
    }

    @Test
    public void scanTest() throws StorageException {
        if (manager.getFeatures().supportsScan()) {
            String[][] values = generateValues();
            loadValues(values);
            RecordIterator<StaticBuffer> iterator0 = store.getKeys(tx);
            Assert.assertEquals(numKeys, KeyValueStoreUtil.count(iterator0));
            clopen();
            RecordIterator<StaticBuffer> iterator1 = store.getKeys(tx);
            RecordIterator<StaticBuffer> iterator2 = store.getKeys(tx);
            // The idea is to open an iterator without using it
            // to make sure that closing a transaction will clean it up.
            // (important for BerkeleyJE where leaving cursors open causes exceptions)
            @SuppressWarnings("unused")
            RecordIterator<StaticBuffer> iterator3 = store.getKeys(tx);
            Assert.assertEquals(numKeys, KeyValueStoreUtil.count(iterator1));
            Assert.assertEquals(numKeys, KeyValueStoreUtil.count(iterator2));
        }
    }

    public void checkSlice(String[][] values, Set<KeyColumn> removed, int key, int start, int end, int limit) throws StorageException {
        List<Entry> entries;
        if (limit <= 0)
            entries = store.getSlice(new KeySliceQuery(KeyValueStoreUtil.getBuffer(key), KeyValueStoreUtil.getBuffer(start), KeyValueStoreUtil.getBuffer(end)), tx);
        else
            entries = store.getSlice(new KeySliceQuery(KeyValueStoreUtil.getBuffer(key), KeyValueStoreUtil.getBuffer(start), KeyValueStoreUtil.getBuffer(end)).setLimit(limit), tx);

        int pos = 0;
        for (int i = start; i < end; i++) {
            if (removed.contains(new KeyColumn(key, i))) continue;
            if (limit <= 0 || pos < limit) {
                Assert.assertTrue(entries.size()>pos);
                Entry entry = entries.get(pos);
                int col = KeyValueStoreUtil.getID(entry.getColumn());
                String str = KeyValueStoreUtil.getString(entry.getValue());
                Assert.assertEquals(i, col);
                Assert.assertEquals(values[key][i], str);
            }
            pos++;
        }
        Assert.assertNotNull(entries);
        if (limit > 0 && pos > limit) Assert.assertEquals(limit, entries.size());
        else Assert.assertEquals(pos, entries.size());
    }

    @Test
    public void intervalTest1() throws StorageException {
        String[][] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        Set<KeyColumn> deleted = Sets.newHashSet();
        clopen();
        int trails = 5000;
        for (int t = 0; t < trails; t++) {
            int key = RandomGenerator.randomInt(0, numKeys);
            int start = RandomGenerator.randomInt(0, numColumns);
            int end = RandomGenerator.randomInt(start, numColumns);
            int limit = RandomGenerator.randomInt(1, 30);
            checkSlice(values, deleted, key, start, end, limit);
            checkSlice(values, deleted, key, start, end, -1);
        }
    }

    @Test
    public void intervalTest2() throws StorageException {
        String[][] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        Set<KeyColumn> deleted = deleteValues(7);
        clopen();
        int trails = 5000;
        for (int t = 0; t < trails; t++) {
            int key = RandomGenerator.randomInt(0, numKeys);
            int start = RandomGenerator.randomInt(0, numColumns);
            int end = RandomGenerator.randomInt(start, numColumns);
            int limit = RandomGenerator.randomInt(1, 30);
            checkSlice(values, deleted, key, start, end, limit);
            checkSlice(values, deleted, key, start, end, -1);
        }
    }


    @Test
    public void getNonExistentKeyReturnsNull() throws Exception {
        StoreTransaction txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        Assert.assertEquals(null, KeyColumnValueStoreUtil.get(store, txn, 0, "col0"));
        Assert.assertEquals(null, KeyColumnValueStoreUtil.get(store, txn, 0, "col1"));
        txn.commit();
    }

    @Test
    public void insertingGettingAndDeletingSimpleDataWorks() throws Exception {
        StoreTransaction txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        KeyColumnValueStoreUtil.insert(store, txn, 0, "col0", "val0");
        KeyColumnValueStoreUtil.insert(store, txn, 0, "col1", "val1");
        txn.commit();

        txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        Assert.assertEquals("val0", KeyColumnValueStoreUtil.get(store, txn, 0, "col0"));
        Assert.assertEquals("val1", KeyColumnValueStoreUtil.get(store, txn, 0, "col1"));
        KeyColumnValueStoreUtil.delete(store, txn, 0, "col0");
        KeyColumnValueStoreUtil.delete(store, txn, 0, "col1");
        txn.commit();

        txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        Assert.assertEquals(null, KeyColumnValueStoreUtil.get(store, txn, 0, "col0"));
        Assert.assertEquals(null, KeyColumnValueStoreUtil.get(store, txn, 0, "col1"));
        txn.commit();
    }

//	@Test
//	public void getSliceNoLimit() throws Exception {
//		CassandraThriftStoreManager manager = new CassandraThriftStoreManager(keyspace);
//		CassandraThriftKeyColumnValueStore store =
//			manager.openDatabase(dbName);
//		
//		StoreTransaction txn = manager.beginTransaction();
//		KeyColumnValueStoreUtil.insert(store, txn, "key0", "col0", "val0");
//		KeyColumnValueStoreUtil.insert(store, txn, "key0", "col1", "val1");
//		txn.commit();
//		
//		txn = manager.beginTransaction();
//		ByteBuffer key0 = KeyColumnValueStoreUtil.stringToByteBuffer("key0");
//		ByteBuffer col0 = KeyColumnValueStoreUtil.stringToByteBuffer("col0");
//		ByteBuffer col2 = KeyColumnValueStoreUtil.stringToByteBuffer("col2");
//		List<Entry> entries = store.getSlice(key0, col0, col2, txn);
//		assertNotNull(entries);
//		assertEquals(2, entries.size());
//		assertEquals("col0", KeyColumnValueStoreUtil.byteBufferToString(entries.get(0).getColumn()));
//		assertEquals("val0", KeyColumnValueStoreUtil.byteBufferToString(entries.get(0).getValue()));
//		assertEquals("col1", KeyColumnValueStoreUtil.byteBufferToString(entries.get(1).getColumn()));
//		assertEquals("val1", KeyColumnValueStoreUtil.byteBufferToString(entries.get(1).getValue()));
//		
//		txn.commit();
//		
//		store.shutdown();
//		manager.shutdown();
//	}

    @Test
    public void getSliceRespectsColumnLimit() throws Exception {
        StoreTransaction txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        StaticBuffer key = KeyColumnValueStoreUtil.longToByteBuffer(0);

        final int cols = 1024;

        List<Entry> entries = new LinkedList<Entry>();
        for (int i = 0; i < cols; i++) {
            StaticBuffer col = KeyColumnValueStoreUtil.longToByteBuffer(i);
            entries.add(new StaticBufferEntry(col, col));
        }
        store.mutate(key, entries, KeyColumnValueStore.NO_DELETIONS, txn);
        txn.commit();

        txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        StaticBuffer columnStart = KeyColumnValueStoreUtil.longToByteBuffer(0);
        StaticBuffer columnEnd = KeyColumnValueStoreUtil.longToByteBuffer(cols);        /*
		 * When limit is greater than or equal to the matching column count,
		 * all matching columns must be returned.
		 */
        List<Entry> result =
                store.getSlice(new KeySliceQuery(key, columnStart, columnEnd).setLimit(cols), txn);
        Assert.assertEquals(cols, result.size());

        for (int i=0; i<result.size(); i++) {
            Entry src = entries.get(i);
            Entry dst = result.get(i);
            if (!src.equals(dst)) {
                int x = 1;
            }
        }

        Assert.assertEquals(entries, result);
        result =
                store.getSlice(new KeySliceQuery(key, columnStart, columnEnd).setLimit(cols + 10), txn);
        Assert.assertEquals(cols, result.size());
        Assert.assertEquals(entries, result);

		/*
		 * When limit is less the matching column count, the columns up to the
		 * limit (ordered bytewise) must be returned.
		 */
        result =
                store.getSlice(new KeySliceQuery(key, columnStart, columnEnd).setLimit(cols - 1), txn);
        Assert.assertEquals(cols - 1, result.size());
        entries.remove(entries.size() - 1);
        Assert.assertEquals(entries, result);
        result =
                store.getSlice(new KeySliceQuery(key, columnStart, columnEnd).setLimit(1), txn);
        Assert.assertEquals(1, result.size());
        List<Entry> firstEntrySingleton = Arrays.asList(entries.get(0));
        Assert.assertEquals(firstEntrySingleton, result);
        txn.commit();
    }

    @Test
    public void getSliceRespectsAllBoundsInclusionArguments() throws Exception {
        // Test case where endColumn=startColumn+1
        StaticBuffer key = KeyColumnValueStoreUtil.longToByteBuffer(0);
        StaticBuffer columnBeforeStart = KeyColumnValueStoreUtil.longToByteBuffer(776);
        StaticBuffer columnStart = KeyColumnValueStoreUtil.longToByteBuffer(777);
        StaticBuffer columnEnd = KeyColumnValueStoreUtil.longToByteBuffer(778);
        StaticBuffer columnAfterEnd = KeyColumnValueStoreUtil.longToByteBuffer(779);

        // First insert four test Entries
        StoreTransaction txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        List<Entry> entries = Arrays.asList(
                (Entry)new StaticBufferEntry(columnBeforeStart, columnBeforeStart),
                new StaticBufferEntry(columnStart, columnStart),
                new StaticBufferEntry(columnEnd, columnEnd),
                new StaticBufferEntry(columnAfterEnd, columnAfterEnd));
        store.mutate(key, entries, KeyColumnValueStore.NO_DELETIONS, txn);
        txn.commit();

        // getSlice() with only start inclusive
        txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        List<Entry> result = store.getSlice(new KeySliceQuery(key, columnStart, columnEnd), txn);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(777, KeyColumnValueStoreUtil.bufferToLong(result.get(0).getColumn()));
        txn.commit();

    }


    @Test
    public void containsKeyReturnsTrueOnExtantKey() throws Exception {
        StaticBuffer key1 = KeyColumnValueStoreUtil.longToByteBuffer(1);
        StoreTransaction txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        Assert.assertFalse(store.containsKey(key1, txn));
        KeyColumnValueStoreUtil.insert(store, txn, 1, "c", "v");
        txn.commit();

        txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        Assert.assertTrue(store.containsKey(key1, txn));
        txn.commit();
    }


    @Test
    public void containsKeyReturnsFalseOnNonexistentKey() throws Exception {
        StoreTransaction txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        StaticBuffer key1 = KeyColumnValueStoreUtil.longToByteBuffer(1);
        Assert.assertFalse(store.containsKey(key1, txn));
        txn.commit();
    }


    @Test
    public void containsKeyColumnReturnsFalseOnNonexistentInput() throws Exception {
        StoreTransaction txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        StaticBuffer key1 = KeyColumnValueStoreUtil.longToByteBuffer(1);
        StaticBuffer c = KeyColumnValueStoreUtil.stringToByteBuffer("c");
        Assert.assertFalse(KCVSUtil.containsKeyColumn(store,key1, c, txn));
        txn.commit();
    }

    @Test
    public void containsKeyColumnReturnsTrueOnExtantInput() throws Exception {
        StoreTransaction txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        KeyColumnValueStoreUtil.insert(store, txn, 1, "c", "v");
        txn.commit();

        txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        StaticBuffer key1 = KeyColumnValueStoreUtil.longToByteBuffer(1);
        StaticBuffer c = KeyColumnValueStoreUtil.stringToByteBuffer("c");
        Assert.assertTrue(KCVSUtil.containsKeyColumn(store,key1, c, txn));
        txn.commit();
    }

    @Test
    public void testGetSlices() throws Exception {
        populateDBWith100Keys();

        StoreTransaction txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        try {
            List<StaticBuffer> keys = new ArrayList<StaticBuffer>(100);

            for (int i = 1; i <= 100; i++) {
                keys.add(KeyColumnValueStoreUtil.longToByteBuffer(i));
            }

            StaticBuffer start = KeyColumnValueStoreUtil.stringToByteBuffer("a");
            StaticBuffer end   = KeyColumnValueStoreUtil.stringToByteBuffer("d");

            List<List<Entry>> results = store.getSlice(keys, new SliceQuery(start, end), txn);

            Assert.assertEquals(100, results.size());

            for (List<Entry> entries : results) {
                Assert.assertEquals(3, entries.size());
            }
        } finally {
            txn.commit();
        }
    }

    @Test
    public void testGetKeysWithSliceQuery() throws Exception {
        populateDBWith100Keys();

        StoreTransaction txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);

        KeyIterator keyIterator = store.getKeys(new SliceQuery(new ReadArrayBuffer("b".getBytes()),
                                                               new ReadArrayBuffer("c".getBytes())),
                                                txn);

        try {
            examineGetKeysResults(keyIterator, 0, 100, 1);
        } finally {
            txn.commit();
        }
    }

    /**
     * This test is not marked with @Test becuase some of the implementations use random partitioning (passes for BerkeleyDB and HBase)
     */
    protected void testGetKeysWithKeyRange() throws Exception {
        populateDBWith100Keys();

        StoreTransaction txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);

        KeyIterator keyIterator = store.getKeys(new KeyRangeQuery(KeyColumnValueStoreUtil.longToByteBuffer(10), // key start
                                                                  KeyColumnValueStoreUtil.longToByteBuffer(40), // key end
                                                                  new ReadArrayBuffer("b".getBytes()), // column start
                                                                  new ReadArrayBuffer("c".getBytes())),
                                                txn);

        try {
            examineGetKeysResults(keyIterator, 10, 40, 1);
        } finally {
            txn.commit();
        }
    }

    protected void populateDBWith100Keys() throws Exception {
        Random random = new Random();

        StoreTransaction txn = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        for (int i = 1; i <= 100; i++) {
            KeyColumnValueStoreUtil.insert(store, txn, i, "a", "v" + random.nextLong());
            KeyColumnValueStoreUtil.insert(store, txn, i, "b", "v" + random.nextLong());
            KeyColumnValueStoreUtil.insert(store, txn, i, "c", "v" + random.nextLong());
        }
        txn.commit();
    }

    protected void examineGetKeysResults(KeyIterator keyIterator,
                                       long startKey,
                                       long endKey,
                                       int expectedColumns) throws StorageException {
        Assert.assertNotNull(keyIterator);

        int count = 0;
        int expectedNumKeys = (int) (endKey - startKey);
        List<StaticBuffer> existingKeys = new ArrayList<StaticBuffer>(expectedNumKeys);

        for (int i = (int) (startKey == 0 ? 1 : startKey); i <= endKey; i++) {
            existingKeys.add(KeyColumnValueStoreUtil.longToByteBuffer(i));
        }

        while (keyIterator.hasNext()) {
            StaticBuffer key = keyIterator.next();

            Assert.assertNotNull(key);
            Assert.assertTrue(existingKeys.contains(key));

            RecordIterator<Entry> entries = keyIterator.getEntries();

            Assert.assertNotNull(entries);

            int entryCount = 0;
            while (entries.hasNext()) {
                Assert.assertNotNull(entries.next());
                entryCount++;
            }

            Assert.assertEquals(expectedColumns, entryCount);

            count++;
        }

        Assert.assertEquals(expectedNumKeys, count);
    }
}
 
