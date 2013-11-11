package com.thinkaurelius.titan.diskstorage;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.ReadArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.testcategory.OrderedKeyStoreTests;
import com.thinkaurelius.titan.testcategory.UnorderedKeyStoreTests;
import com.thinkaurelius.titan.testutil.RandomGenerator;

public abstract class KeyColumnValueStoreTest {

    @Rule
    public TestName name = new TestName();

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
        tx = startTx();
    }

    public StoreTransaction startTx() throws StorageException {
        return manager.beginTransaction(new StoreTxConfig());
    }

    public StoreFeatures storeFeatures() {
        return manager.getFeatures();
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

    public void newTx() throws StorageException {
        if (tx!=null) tx.commit();
        tx = startTx();
    }

    @Test
    public void createDatabase() {
        //Just setup and shutdown
    }

    public String[][] generateValues() {
        return KeyValueStoreUtil.generateData(numKeys, numColumns);
    }

    public void loadValues(String[][] values) throws StorageException {
        loadValues(values, -1, -1);
    }

    public void loadValues(String[][] values, int shiftEveryNthRow,
                           int shiftSliceLength) throws StorageException {
        for (int i = 0; i < values.length; i++) {

            List<Entry> entries = new ArrayList<Entry>();
            for (int j = 0; j < values[i].length; j++) {
                StaticBuffer col;
                if (0 < shiftEveryNthRow && 0 == i/* +1 */ % shiftEveryNthRow) {
                    ByteBuffer bb = ByteBuffer.allocate(shiftSliceLength + 9);
                    for (int s = 0; s < shiftSliceLength; s++) {
                        bb.put((byte) -1);
                    }
                    bb.put(KeyValueStoreUtil.getBuffer(j + 1).asByteBuffer());
                    bb.flip();
                    col = new StaticByteBuffer(bb);

                    // col = KeyValueStoreUtil.getBuffer(j + values[i].length +
                    // 100);
                } else {
                    col = KeyValueStoreUtil.getBuffer(j);
                }
                entries.add(new StaticBufferEntry(col, KeyValueStoreUtil
                        .getBuffer(values[i][j])));
            }
            store.mutate(KeyValueStoreUtil.getBuffer(i), entries,
                    KeyColumnValueStore.NO_DELETIONS, tx);
        }
    }

    /**
     * Load a bunch of key-column-values in a way that vaguely resembles a lower
     * triangular matrix.
     * <p/>
     * Iterate over key values {@code k} in the half-open long interval
     * {@code [offset, offset + dimension -1)}. For each {@code k}, iterate over
     * the column values {@code c} in the half-open integer interval
     * {@code [offset, k]}.
     * <p/>
     * For each key-column coordinate specified by a {@code (k, c} pair in the
     *iteration, write a value one byte long with all bits set (unsigned -1 or
     *signed 255).
     *
     * @param dimension size of loaded data (must be positive)
     * @param offset    offset (must be positive)
     * @throws StorageException unexpected failure
     */
    public void loadLowerTriangularValues(int dimension, int offset) throws StorageException {

        Preconditions.checkArgument(0 < dimension);
        ByteBuffer val = ByteBuffer.allocate(1);
        val.put((byte) -1);
        StaticBuffer staticVal = new StaticByteBuffer(val);

        List<Entry> rowAdditions = new ArrayList<Entry>(dimension);

        for (int k = 0; k < dimension; k++) {

            rowAdditions.clear();

            ByteBuffer key = ByteBuffer.allocate(8);
            key.putInt(0);
            key.putInt(k + offset);
            key.flip();
            StaticBuffer staticKey = new StaticByteBuffer(key);

            for (int c = 0; c <= k; c++) {
                ByteBuffer col = ByteBuffer.allocate(4);
                col.putInt(c + offset);
                col.flip();
                StaticBuffer staticCol = new StaticByteBuffer(col);
                rowAdditions.add(new StaticBufferEntry(staticCol, staticVal));
            }

            store.mutate(staticKey, rowAdditions, Collections.<StaticBuffer>emptyList(), tx);
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
                StaticBuffer result = KCVSUtil.get(store, KeyValueStoreUtil.getBuffer(i), KeyValueStoreUtil.getBuffer(j), tx);
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
        int keys = 50 * multiplier, columns = 2000;
        String[][] values = KeyValueStoreUtil.generateData(keys, columns);
        log.debug("Loading values: " + keys + "x" + columns);
        long time = System.currentTimeMillis();
        loadValues(values);
        System.out.println("Loading time (ms): " + (System.currentTimeMillis() - time));
        //print(values);
        Random r = new Random();
        int trials = 500 * multiplier;
        int delta = 10;
        log.debug("Reading values: " + trials + " trials");
        time = System.currentTimeMillis();
        for (int t = 0; t < trials; t++) {
            int key = r.nextInt(keys);
            int start = r.nextInt(columns - delta);
            store.getSlice(new KeySliceQuery(KeyValueStoreUtil.getBuffer(key), KeyValueStoreUtil.getBuffer(start), KeyValueStoreUtil.getBuffer(start + delta)), tx);
        }
        System.out.println("Reading time (ms): " + (System.currentTimeMillis() - time));
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
        newTx();
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
        newTx();
        Set<Integer> deleted = deleteKeys(11);
        clopen();
        checkKeys(deleted);
    }

    /**
     * Loads a block of data where keys are longs on [idOffset, idOffset +
     * numKeys) and the columns are longs on [idOffset, idOffset + numColumns).
     * {@code idOffset} is {@link KeyValueStoreUtil#idOffset}. Note that
     * identical columns appear on every key. The loaded values are randomly
     * generated strings converted to bytes.
     * <p/>
     * Calls the store's supported {@code getKeys} method depending on whether
     * it supports ordered or unordered scan. This logic is delegated to
     * {@link KCVSUtil#getKeys(KeyColumnValueStore, StoreFeatures, int, int, StoreTransaction)}
     * . That method uses all-zero and all-one buffers for the key and column
     * limits and retrieves every key.
     * <p/>
     * This method does nothing and returns immediately if the store supports no
     * scans.
     */
    @Test
    public void scanTest() throws StorageException {
        if (manager.getFeatures().supportsScan()) {
            String[][] values = generateValues();
            loadValues(values);
            RecordIterator<StaticBuffer> iterator0 = KCVSUtil.getKeys(store, storeFeatures(), 8, 4, tx);
            Assert.assertEquals(numKeys, KeyValueStoreUtil.count(iterator0));
            clopen();
            RecordIterator<StaticBuffer> iterator1 = KCVSUtil.getKeys(store, storeFeatures(), 8, 4, tx);
            RecordIterator<StaticBuffer> iterator2 = KCVSUtil.getKeys(store, storeFeatures(), 8, 4, tx);
            // The idea is to open an iterator without using it
            // to make sure that closing a transaction will clean it up.
            // (important for BerkeleyJE where leaving cursors open causes exceptions)
            @SuppressWarnings("unused")
            RecordIterator<StaticBuffer> iterator3 = KCVSUtil.getKeys(store, storeFeatures(), 8, 4, tx);
            Assert.assertEquals(numKeys, KeyValueStoreUtil.count(iterator1));
            Assert.assertEquals(numKeys, KeyValueStoreUtil.count(iterator2));
        }
    }

    /**
     * Verify that
     * {@link KeyColumnValueStore#getKeys(KeyRangeQuery, StoreTransaction)}
     * treats the lower key bound as inclusive and the upper key bound as
     * exclusive. Verify that keys less than the start and greater than or equal
     * to the end containing matching columns are not returned.
     *
     * @throws StorageException
     */
    @Test
    @Category({OrderedKeyStoreTests.class})
    public void testOrderedGetKeysRespectsKeyLimit() throws StorageException {
        if (!manager.getFeatures().supportsOrderedScan()) {
            log.warn("Can't test key-ordered features on incompatible store.  "
                    + "This warning could indicate reduced test coverage and "
                    + "a broken JUnit configuration.  Skipping test {}.",
                    name.getMethodName());
            return;
        }

        Preconditions.checkArgument(4 <= numKeys);
        Preconditions.checkArgument(4 <= numColumns);

        final long minKey = KeyValueStoreUtil.idOffset + 1;
        final long maxKey = KeyValueStoreUtil.idOffset + numKeys - 2;
        final long expectedKeyCount = maxKey - minKey;

        String[][] values = generateValues();
        loadValues(values);
        final SliceQuery columnSlice = new SliceQuery(ByteBufferUtil.zeroBuffer(8), ByteBufferUtil.oneBuffer(8)).setLimit(1);

        KeyIterator keys;

        keys = store.getKeys(new KeyRangeQuery(ByteBufferUtil.getLongBuffer(minKey), ByteBufferUtil.getLongBuffer(maxKey), columnSlice), tx);
        assertEquals(expectedKeyCount, KeyValueStoreUtil.count(keys));

        clopen();

        keys = store.getKeys(new KeyRangeQuery(ByteBufferUtil.getLongBuffer(minKey), ByteBufferUtil.getLongBuffer(maxKey), columnSlice), tx);
        assertEquals(expectedKeyCount, KeyValueStoreUtil.count(keys));
    }

    /**
     * Check that {@code getKeys} methods respect column slice bounds. Uses
     * nearly the same data as {@link #testOrderedGetKeysRespectsKeyLimit()},
     * except that all columns on every 10th row exceed the {@code getKeys}
     * slice limit.
     * <p/>
     * For each row in this test, either all columns match the slice bounds or
     * all columns fall outside the slice bounds. For this reason, it could be
     * described as a "coarse-grained" or "simple" test of {@code getKeys}'s
     * column bounds checking.
     *
     * @throws StorageException
     */
    @Test
    public void testGetKeysColumnSlicesSimple()
            throws StorageException {
        if (manager.getFeatures().supportsScan()) {

            final int shiftEveryNthRows = 10;
            final int expectedKeyCount = numKeys / shiftEveryNthRows * (shiftEveryNthRows - 1);

            Preconditions.checkArgument(0 == numKeys % shiftEveryNthRows);
            Preconditions.checkArgument(10 < numKeys / shiftEveryNthRows);

            String[][] values = generateValues();
            loadValues(values, shiftEveryNthRows, 4);

            RecordIterator<StaticBuffer> i;
            i = KCVSUtil.getKeys(store, storeFeatures(), 8, 4, tx);
            Assert.assertEquals(expectedKeyCount, KeyValueStoreUtil.count(i));

            clopen();

            i = KCVSUtil.getKeys(store, storeFeatures(), 8, 4, tx);
            Assert.assertEquals(expectedKeyCount, KeyValueStoreUtil.count(i));
        }
    }


    /**
     * Test {@code getKeys} with columns slice values chosen to trigger
     * potential fencepost bugs.
     * <p/>
     * Description of data generated for and queried by this test:
     * <p/>
     * Generate a sequence of keys as unsigned integers, starting at zero. Each
     * row has as many columns as the key value. The columns are generated in
     * the same way as the keys. This results in a sort of "lower triangular"
     * data space, with no values above the diagonal.
     *
     * @throws StorageException shouldn't happen
     * @throws IOException      shouldn't happen
     */
    @Test
    public void testGetKeysColumnSlicesOnLowerTriangular() throws StorageException, IOException {
        if (manager.getFeatures().supportsScan()) {
            final int offset = 10;
            final int size = 10;
            final int midpoint = size / 2 + offset;
            final int upper = offset + size;
            final int step = 1;
            Preconditions.checkArgument(0 == size % 2);
            Preconditions.checkArgument(0 == offset % 2);
            Preconditions.checkArgument(4 <= size);
            Preconditions.checkArgument(1 <= offset);

            loadLowerTriangularValues(size, offset);

            boolean executed = false;

            if (manager.getFeatures().supportsUnorderedScan()) {

                Collection<StaticBuffer> expected = new HashSet<StaticBuffer>(size);

                for (int start = midpoint; start >= offset - step; start -= step) {
                    for (int end = midpoint + 1; end <= upper + step; end += step) {
                        Preconditions.checkArgument(start < end);

                        // Set column bounds
                        StaticBuffer startCol = ByteBufferUtil.getIntBuffer(start);
                        StaticBuffer endCol = ByteBufferUtil.getIntBuffer(end);
                        SliceQuery sq = new SliceQuery(startCol, endCol);

                        // Compute expectation
                        expected.clear();
                        for (long l = Math.max(start, offset); l < upper; l++) {
                            expected.add(ByteBufferUtil.getLongBuffer(l));
                        }

                        // Compute actual
                        KeyIterator i = store.getKeys(sq, tx);
                        Collection<StaticBuffer> actual = Sets.newHashSet(i);

                        // Check
                        log.error("Checking bounds [{}, {}) (expect {} keys)",
                                new Object[]{startCol, endCol, expected.size()});
                        Assert.assertEquals(expected, actual);
                        i.close();
                        executed = true;
                    }
                }

            } else if (manager.getFeatures().supportsOrderedScan()) {

                Collection<StaticBuffer> expected = new ArrayList<StaticBuffer>(size);

                for (int start = midpoint; start >= offset - step; start -= step) {
                    for (int end = midpoint + 1; end <= upper + step; end += step) {
                        Preconditions.checkArgument(start < end);

                        // Set column bounds
                        StaticBuffer startCol = ByteBufferUtil.getIntBuffer(start);
                        StaticBuffer endCol = ByteBufferUtil.getIntBuffer(end);
                        SliceQuery sq = new SliceQuery(startCol, endCol);

                        // Set key bounds
                        StaticBuffer keyStart = ByteBufferUtil.getLongBuffer(start);
                        StaticBuffer keyEnd = ByteBufferUtil.getLongBuffer(end);
                        KeyRangeQuery krq = new KeyRangeQuery(keyStart, keyEnd, sq);

                        // Compute expectation
                        expected.clear();
                        for (long l = Math.max(start, offset); l < Math.min(upper, end); l++) {
                            expected.add(ByteBufferUtil.getLongBuffer(l));
                        }

                        // Compute actual
                        KeyIterator i = store.getKeys(krq, tx);
                        Collection<StaticBuffer> actual = Lists.newArrayList(i);

                        log.error("Checking bounds key:[{}, {}) & col:[{}, {}) (expect {} keys)",
                                new Object[]{keyStart, keyEnd, startCol, endCol, expected.size()});
                        Assert.assertEquals(expected, actual);
                        i.close();
                        executed = true;
                    }
                }

            } else {
                throw new UnsupportedOperationException(
                        "Illegal store configuration: supportsScan()=true but supportsOrderedScan()=supportsUnorderedScan()=false");
            }

            Preconditions.checkArgument(executed);
        }
    }

    public void checkSlice(String[][] values, Set<KeyColumn> removed, int key,
                           int start, int end, int limit) throws StorageException {
        List<Entry> entries;
        if (limit <= 0)
            entries = store.getSlice(new KeySliceQuery(KeyValueStoreUtil.getBuffer(key), KeyValueStoreUtil.getBuffer(start), KeyValueStoreUtil.getBuffer(end)), tx);
        else
            entries = store.getSlice(new KeySliceQuery(KeyValueStoreUtil.getBuffer(key), KeyValueStoreUtil.getBuffer(start), KeyValueStoreUtil.getBuffer(end)).setLimit(limit), tx);

        int pos = 0;
        for (int i = start; i < end; i++) {
            if (removed.contains(new KeyColumn(key, i))) continue;
            if (limit <= 0 || pos < limit) {
                Assert.assertTrue(entries.size() > pos);
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
        newTx();
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
        Assert.assertEquals(null, KeyColumnValueStoreUtil.get(store, tx, 0, "col0"));
        Assert.assertEquals(null, KeyColumnValueStoreUtil.get(store, tx, 0, "col1"));
    }

    @Test
    public void insertingGettingAndDeletingSimpleDataWorks() throws Exception {
        KeyColumnValueStoreUtil.insert(store, tx, 0, "col0", "val0");
        KeyColumnValueStoreUtil.insert(store, tx, 0, "col1", "val1");
        tx.commit();

        tx = startTx();
        Assert.assertEquals("val0", KeyColumnValueStoreUtil.get(store, tx, 0, "col0"));
        Assert.assertEquals("val1", KeyColumnValueStoreUtil.get(store, tx, 0, "col1"));
        KeyColumnValueStoreUtil.delete(store, tx, 0, "col0");
        KeyColumnValueStoreUtil.delete(store, tx, 0, "col1");
        tx.commit();

        tx = startTx();
        Assert.assertEquals(null, KeyColumnValueStoreUtil.get(store, tx, 0, "col0"));
        Assert.assertEquals(null, KeyColumnValueStoreUtil.get(store, tx, 0, "col1"));
    }

    @Test
    public void getSliceRespectsColumnLimit() throws Exception {
        StaticBuffer key = KeyColumnValueStoreUtil.longToByteBuffer(0);

        final int cols = 1024;

        List<Entry> entries = new LinkedList<Entry>();
        for (int i = 0; i < cols; i++) {
            StaticBuffer col = KeyColumnValueStoreUtil.longToByteBuffer(i);
            entries.add(new StaticBufferEntry(col, col));
        }
        store.mutate(key, entries, KeyColumnValueStore.NO_DELETIONS, tx);
        tx.commit();

        tx = startTx();
        /*
         * When limit is greater than or equal to the matching column count ,
         * all matching columns must be returned.
         */
        StaticBuffer columnStart = KeyColumnValueStoreUtil.longToByteBuffer(0);
        StaticBuffer columnEnd = KeyColumnValueStoreUtil.longToByteBuffer(cols);
        List<Entry> result =
                store.getSlice(new KeySliceQuery(key, columnStart, columnEnd).setLimit(cols), tx);
        Assert.assertEquals(cols, result.size());

        for (int i = 0; i < result.size(); i++) {
            Entry src = entries.get(i);
            Entry dst = result.get(i);
            if (!src.equals(dst)) {
                int x = 1;
            }
        }

        Assert.assertEquals(entries, result);
        result = store.getSlice(new KeySliceQuery(key, columnStart, columnEnd).setLimit(cols + 10), tx);
        Assert.assertEquals(cols, result.size());
        Assert.assertEquals(entries, result);

        /*
         * When limit is less the matching column count, the columns up to the
         * limit (ordered bytewise) must be returned.
         */
        result = store.getSlice(new KeySliceQuery(key, columnStart, columnEnd).setLimit(cols - 1), tx);
        Assert.assertEquals(cols - 1, result.size());
        entries.remove(entries.size() - 1);
        Assert.assertEquals(entries, result);
        result = store.getSlice(new KeySliceQuery(key, columnStart, columnEnd).setLimit(1), tx);
        Assert.assertEquals(1, result.size());
        List<Entry> firstEntrySingleton = Arrays.asList(entries.get(0));
        Assert.assertEquals(firstEntrySingleton, result);
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
        List<Entry> entries = Arrays.asList(
                (Entry) new StaticBufferEntry(columnBeforeStart, columnBeforeStart),
                new StaticBufferEntry(columnStart, columnStart),
                new StaticBufferEntry(columnEnd, columnEnd),
                new StaticBufferEntry(columnAfterEnd, columnAfterEnd));
        store.mutate(key, entries, KeyColumnValueStore.NO_DELETIONS, tx);
        tx.commit();

        // getSlice() with only start inclusive
        tx = startTx();
        List<Entry> result = store.getSlice(new KeySliceQuery(key, columnStart, columnEnd), tx);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(777, KeyColumnValueStoreUtil.bufferToLong(result.get(0).getColumn()));

    }

    @Test
    public void containsKeyReturnsTrueOnExtantKey() throws Exception {
        StaticBuffer key1 = KeyColumnValueStoreUtil.longToByteBuffer(1);
        Assert.assertFalse(store.containsKey(key1, tx));
        KeyColumnValueStoreUtil.insert(store, tx, 1, "c", "v");
        tx.commit();

        tx = startTx();
        Assert.assertTrue(store.containsKey(key1, tx));
    }

    @Test
    public void containsKeyReturnsFalseOnNonexistentKey() throws Exception {
        StaticBuffer key1 = KeyColumnValueStoreUtil.longToByteBuffer(1);
        Assert.assertFalse(store.containsKey(key1, tx));
    }

    @Test
    public void containsKeyColumnReturnsFalseOnNonexistentInput()
            throws Exception {
        StaticBuffer key1 = KeyColumnValueStoreUtil.longToByteBuffer(1);
        StaticBuffer c = KeyColumnValueStoreUtil.stringToByteBuffer("c");
        Assert.assertFalse(KCVSUtil.containsKeyColumn(store, key1, c, tx));
    }

    @Test
    public void containsKeyColumnReturnsTrueOnExtantInput() throws Exception {
        KeyColumnValueStoreUtil.insert(store, tx, 1, "c", "v");
        tx.commit();
        tx = startTx();
        StaticBuffer key1 = KeyColumnValueStoreUtil.longToByteBuffer(1);
        StaticBuffer c = KeyColumnValueStoreUtil.stringToByteBuffer("c");
        Assert.assertTrue(KCVSUtil.containsKeyColumn(store, key1, c, tx));
    }

    @Test
    public void testGetSlices() throws Exception {
        if (!manager.getFeatures().supportsMultiQuery()) return;

        populateDBWith100Keys();

        tx.commit();
        tx = startTx();

        List<StaticBuffer> keys = new ArrayList<StaticBuffer>(100);

        for (int i = 1; i <= 100; i++) {
            keys.add(KeyColumnValueStoreUtil.longToByteBuffer(i));
        }

        StaticBuffer start = KeyColumnValueStoreUtil.stringToByteBuffer("a");
        StaticBuffer end = KeyColumnValueStoreUtil.stringToByteBuffer("d");

        List<List<Entry>> results = store.getSlice(keys, new SliceQuery(start, end), tx);

        Assert.assertEquals(100, results.size());

        for (List<Entry> entries : results) {
            Assert.assertEquals(3, entries.size());
        }
    }

    @Test
    @Category({UnorderedKeyStoreTests.class})
    public void testGetKeysWithSliceQuery() throws Exception {
        if (!manager.getFeatures().supportsUnorderedScan()) {
            log.warn("Can't test key-unordered features on incompatible store.  "
                    + "This warning could indicate reduced test coverage and "
                    + "a broken JUnit configuration.  Skipping test {}.",
                    name.getMethodName());
            return;
        }

        populateDBWith100Keys();

        tx.commit();
        tx = startTx();

        KeyIterator keyIterator = store.getKeys(
                new SliceQuery(new ReadArrayBuffer("b".getBytes()),
                        new ReadArrayBuffer("c".getBytes())), tx);

        examineGetKeysResults(keyIterator, 0, 100, 1);
    }

    @Test
    @Category({OrderedKeyStoreTests.class})
    public void testGetKeysWithKeyRange() throws Exception {
        if (!manager.getFeatures().supportsOrderedScan()) {
            log.warn("Can't test ordered scans on incompatible store.  "
                    + "This warning could indicate reduced test coverage and "
                    + "shouldn't happen in an ideal JUnit configuration.  "
                    + "Skipping test {}.", name.getMethodName());
            return;
        }

        populateDBWith100Keys();

        tx.commit();
        tx = startTx();

        KeyIterator keyIterator = store.getKeys(new KeyRangeQuery(
                KeyColumnValueStoreUtil.longToByteBuffer(10), // key start
                KeyColumnValueStoreUtil.longToByteBuffer(40), // key end
                new ReadArrayBuffer("b".getBytes()), // column start
                new ReadArrayBuffer("c".getBytes())), tx);

        examineGetKeysResults(keyIterator, 10, 40, 1);
    }

    protected void populateDBWith100Keys() throws Exception {
        Random random = new Random();

        for (int i = 1; i <= 100; i++) {
            KeyColumnValueStoreUtil.insert(store, tx, i, "a",
                    "v" + random.nextLong());
            KeyColumnValueStoreUtil.insert(store, tx, i, "b",
                    "v" + random.nextLong());
            KeyColumnValueStoreUtil.insert(store, tx, i, "c",
                    "v" + random.nextLong());
        }
    }

    protected void examineGetKeysResults(KeyIterator keyIterator,
                                         long startKey, long endKey, int expectedColumns)
            throws StorageException {
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
