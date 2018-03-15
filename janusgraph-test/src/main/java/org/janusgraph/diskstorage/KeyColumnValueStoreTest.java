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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import com.google.common.collect.ImmutableList;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJob;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.diskstorage.keycolumnvalue.scan.StandardScanner;
import org.janusgraph.diskstorage.keycolumnvalue.ttl.TTLKCVSManager;
import org.janusgraph.diskstorage.util.*;

import org.janusgraph.testcategory.BrittleTests;
import org.janusgraph.testutil.TestGraphConfigs;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.testcategory.OrderedKeyStoreTests;
import org.janusgraph.testcategory.UnorderedKeyStoreTests;
import org.janusgraph.testutil.RandomGenerator;

import static org.junit.Assert.*;

public abstract class KeyColumnValueStoreTest extends AbstractKCVSTest {

    public static final int TRIALS = 5000;
    @Rule
    public TestName name = new TestName();

    private final Logger log = LoggerFactory.getLogger(KeyColumnValueStoreTest.class);

    int numKeys = 500;
    int numColumns = 50;

    protected String storeName = "testStore1";

    public KeyColumnValueStoreManager manager;
    public StoreTransaction tx;
    public KeyColumnValueStore store;

    @Before
    public void setUp() throws Exception {
        final StoreManager m = openStorageManager();
        m.clearStorage();
        m.close();
        open();
    }

    public abstract KeyColumnValueStoreManager openStorageManager() throws BackendException;

    public void open() throws BackendException {
        manager = openStorageManager();
        store = manager.openDatabase(storeName);
        tx = startTx();
    }

    public StoreTransaction startTx() throws BackendException {
        return manager.beginTransaction(getTxConfig());
    }

    public StoreFeatures storeFeatures() {
        return manager.getFeatures();
    }

    public void clopen() throws BackendException {
        close();
        open();
    }

    @After
    public void tearDown() throws Exception {
        close();
    }

    public void close() throws BackendException {
        if (tx != null) tx.commit();
        if (null != store) store.close();
        if (null != manager) manager.close();
    }

    public void newTx() throws BackendException {
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

    public void loadValues(String[][] values) throws BackendException {
        KeyColumnValueStoreUtil.loadValues(store, tx, values);
    }

    public void loadValues(String[][] values, int shiftEveryNthRow, int shiftSliceLength) throws BackendException {
        KeyColumnValueStoreUtil.loadValues(store, tx, values, shiftEveryNthRow, shiftSliceLength);
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
     * @throws BackendException unexpected failure
     */
    public void loadLowerTriangularValues(int dimension, int offset) throws BackendException {

        Preconditions.checkArgument(0 < dimension);
        final ByteBuffer val = ByteBuffer.allocate(1);
        val.put((byte) -1);
        final StaticBuffer staticVal = StaticArrayBuffer.of(val);

        final List<Entry> rowAdditions = new ArrayList<Entry>(dimension);

        for (int k = 0; k < dimension; k++) {

            rowAdditions.clear();

            final ByteBuffer key = ByteBuffer.allocate(8);
            key.putInt(0);
            key.putInt(k + offset);
            key.flip();
            final StaticBuffer staticKey = StaticArrayBuffer.of(key);

            for (int c = 0; c <= k; c++) {
                final ByteBuffer col = ByteBuffer.allocate(4);
                col.putInt(c + offset);
                col.flip();
                final StaticBuffer staticCol = StaticArrayBuffer.of(col);
                rowAdditions.add(StaticArrayEntry.of(staticCol, staticVal));
            }

            store.mutate(staticKey, rowAdditions, Collections.<StaticBuffer>emptyList(), tx);
        }
    }

    public Set<KeyColumn> deleteValues(int every) throws BackendException {
        final Set<KeyColumn> removed = new HashSet<KeyColumn>();
        int counter = 0;
        for (int i = 0; i < numKeys; i++) {
            final List<StaticBuffer> deletions = new ArrayList<StaticBuffer>();
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

    public Set<Integer> deleteKeys(int every) throws BackendException {
        final Set<Integer> removed = new HashSet<Integer>();
        for (int i = 0; i < numKeys; i++) {
            if (i % every == 0) {
                removed.add(i);
                final List<StaticBuffer> deletions = new ArrayList<StaticBuffer>();
                for (int j = 0; j < numColumns; j++) {
                    deletions.add(KeyValueStoreUtil.getBuffer(j));
                }
                store.mutate(KeyValueStoreUtil.getBuffer(i), KeyColumnValueStore.NO_ADDITIONS, deletions, tx);
            }
        }
        return removed;
    }

    public void checkKeys(Set<Integer> removed) throws BackendException {
        for (int i = 0; i < numKeys; i++) {
            if (removed.contains(i)) {
                Assert.assertFalse(KCVSUtil.containsKey(store, KeyValueStoreUtil.getBuffer(i), tx));
            } else {
                Assert.assertTrue(KCVSUtil.containsKey(store, KeyValueStoreUtil.getBuffer(i), tx));
            }
        }
    }

    public void checkValueExistence(String[][] values) throws BackendException {
        checkValueExistence(values, new HashSet<KeyColumn>());
    }

    public void checkValueExistence(String[][] values, Set<KeyColumn> removed) throws BackendException {
        for (int i = 0; i < numKeys; i++) {
            for (int j = 0; j < numColumns; j++) {
                final boolean result = KCVSUtil.containsKeyColumn(store, KeyValueStoreUtil.getBuffer(i), KeyValueStoreUtil.getBuffer(j), tx);
                if (removed.contains(new KeyColumn(i, j))) {
                    Assert.assertFalse(result);
                } else {
                    Assert.assertTrue(result);
                }
            }
        }
    }

    public void checkValues(String[][] values) throws BackendException {
        checkValues(values, new HashSet<KeyColumn>());
    }

    public void checkValues(String[][] values, Set<KeyColumn> removed) throws BackendException {
        for (int i = 0; i < numKeys; i++) {
            for (int j = 0; j < numColumns; j++) {
                final StaticBuffer result = KCVSUtil.get(store, KeyValueStoreUtil.getBuffer(i), KeyValueStoreUtil.getBuffer(j), tx);
                if (removed.contains(new KeyColumn(i, j))) {
                    Assert.assertNull(result);
                } else {
                    Assert.assertEquals(values[i][j], KeyValueStoreUtil.getString(result));
                }
            }
        }

    }

    @Test
    public void storeAndRetrieve() throws BackendException {
        final String[][] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        //print(values);
        log.debug("Checking values...");
        checkValueExistence(values);
        checkValues(values);
    }

    //@Test
    public void compareStores() throws BackendException {
        final int keys = 1000, columns = 2000; final boolean normalMode=true;
        final String[][] values = new String[keys*2][];
        for (int i = 0; i < keys*2; i++) {
            if(i%2==0) {
                if (normalMode) {
                    values[i]=new String[columns + 4];
                } else {
                    values[i]=new String[4];
                }
            } else {
                if (normalMode) {
                    values[i]=new String[0];
                } else {
                    values[i]=new String[columns];
                }
            }
            for (int j = 0; j < values[i].length; j++) {
                values[i][j] = RandomGenerator.randomString(30,35);
            }
        }
        log.debug("Loading values: " + keys + "x" + columns);
        long time = System.currentTimeMillis();
        loadValues(values);
        clopen();
        System.out.println("Loading time (ms): " + (System.currentTimeMillis() - time));
        //print(values);
        final Random r = new Random();
        final int trials = 500;
        log.debug("Reading values: " + trials + " trials");
        for (int i=0; i<10;i++) {
            time = System.currentTimeMillis();
            for (int t = 0; t < trials; t++) {
                final int key = r.nextInt(keys)*2;
                assertEquals(2,store.getSlice(new KeySliceQuery(KeyValueStoreUtil.getBuffer(key), KeyValueStoreUtil.getBuffer(2002), KeyValueStoreUtil.getBuffer(2004), "compareStores"), tx).size());
            }

            System.out.println("Reading time (ms): " + (System.currentTimeMillis() - time));
        }
    }




    @Test
    public void storeAndRetrievePerformance() throws BackendException {
        final int multiplier = 4;
        final int keys = 50 * multiplier, columns = 200;
        final String[][] values = KeyValueStoreUtil.generateData(keys, columns);
        log.debug("Loading values: " + keys + "x" + columns);
        long time = System.currentTimeMillis();
        loadValues(values);
        clopen();
        System.out.println("Loading time (ms): " + (System.currentTimeMillis() - time));
        //print(values);
        final Random r = new Random();
        final int trials = 500 * multiplier;
        final int delta = 10;
        log.debug("Reading values: " + trials + " trials");
        for (int i=0; i<1;i++) {
            time = System.currentTimeMillis();
            for (int t = 0; t < trials; t++) {
                final int key = r.nextInt(keys);
                final int start = r.nextInt(columns - delta);
                store.getSlice(new KeySliceQuery(KeyValueStoreUtil.getBuffer(key), KeyValueStoreUtil.getBuffer(start), KeyValueStoreUtil.getBuffer(start + delta), "storeAndRetrievePerformance"), tx);
            }
            //multiQuery version
//            List<StaticBuffer> keylist = new ArrayList<StaticBuffer>();
//            for (int t = 0; t < trials; t++) keylist.add(KeyValueStoreUtil.getBuffer(r.nextInt(keys)));
//            int start = r.nextInt(columns - delta);
//            store.getSlice(keylist, new SliceQuery(KeyValueStoreUtil.getBuffer(start), KeyValueStoreUtil.getBuffer(start + delta)), tx);
            System.out.println("Reading time (ms): " + (System.currentTimeMillis() - time));
        }
    }

    @Test
    public void storeAndRetrieveWithClosing() throws BackendException {
        final String[][] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        clopen();
        log.debug("Checking values...");
        checkValueExistence(values);
        checkValues(values);
    }

    @Test
    public void deleteColumnsTest1() throws BackendException {
        final String[][] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        clopen();
        final Set<KeyColumn> deleted = deleteValues(7);
        log.debug("Checking values...");
        checkValueExistence(values, deleted);
        checkValues(values, deleted);
    }

    @Test
    public void deleteColumnsTest2() throws BackendException {
        final String[][] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        newTx();
        final Set<KeyColumn> deleted = deleteValues(7);
        clopen();
        log.debug("Checking values...");
        checkValueExistence(values, deleted);
        checkValues(values, deleted);
    }

    @Test
    public void deleteKeys() throws BackendException {
        final String[][] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        newTx();
        final Set<Integer> deleted = deleteKeys(11);
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
    public void scanTest() throws BackendException {
        if (manager.getFeatures().hasScan()) {
            final String[][] values = generateValues();
            loadValues(values);
            final KeyIterator iterator0 = KCVSUtil.getKeys(store, storeFeatures(), 8, 4, tx);
            verifyIterator(iterator0,numKeys,1);
            clopen();
            final KeyIterator iterator1 = KCVSUtil.getKeys(store, storeFeatures(), 8, 4, tx);
            final KeyIterator iterator2 = KCVSUtil.getKeys(store, storeFeatures(), 8, 4, tx);
            // The idea is to open an iterator without using it
            // to make sure that closing a transaction will clean it up.
            // (important for BerkeleyJE where leaving cursors open causes exceptions)
            @SuppressWarnings("unused")
            final
            KeyIterator iterator3 = KCVSUtil.getKeys(store, storeFeatures(), 8, 4, tx);
            verifyIterator(iterator1,numKeys,1);
            verifyIterator(iterator2,numKeys,1);
        }
    }

    private void verifyIterator(KeyIterator iter, int expectedKeys, int exepctedCols) {
        int keys = 0;
        while (iter.hasNext()) {
            final StaticBuffer b = iter.next();
            assertTrue(b!=null && b.length()>0);
            keys++;
            final RecordIterator<Entry> entries = iter.getEntries();
            int cols = 0;
            while (entries.hasNext()) {
                final Entry e = entries.next();
                assertTrue(e!=null && e.length()>0);
                cols++;
            }
            assertEquals(exepctedCols,cols);
        }
        assertEquals(expectedKeys,keys);
    }

    /**
     * Verify that
     * {@link KeyColumnValueStore#getKeys(KeyRangeQuery, StoreTransaction)}
     * treats the lower key bound as inclusive and the upper key bound as
     * exclusive. Verify that keys less than the start and greater than or equal
     * to the end containing matching columns are not returned.
     *
     * @throws BackendException
     */
    @Test
    @Category({OrderedKeyStoreTests.class})
    public void testOrderedGetKeysRespectsKeyLimit() throws BackendException {
        if (!manager.getFeatures().hasOrderedScan()) {
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

        final String[][] values = generateValues();
        loadValues(values);
        final SliceQuery columnSlice = new SliceQuery(BufferUtil.zeroBuffer(8), BufferUtil.oneBuffer(8), "testOrderedGetKeysRespectsKeyLimit").setLimit(1);

        KeyIterator keys;

        keys = store.getKeys(new KeyRangeQuery(BufferUtil.getLongBuffer(minKey), BufferUtil.getLongBuffer(maxKey), columnSlice), tx);
        assertEquals(expectedKeyCount, KeyValueStoreUtil.count(keys));

        clopen();

        keys = store.getKeys(new KeyRangeQuery(BufferUtil.getLongBuffer(minKey), BufferUtil.getLongBuffer(maxKey), columnSlice), tx);
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
     * @throws BackendException
     */
    @Test
    public void testGetKeysColumnSlicesSimple()
            throws BackendException {
        if (manager.getFeatures().hasScan()) {

            final int shiftEveryNthRows = 10;
            final int expectedKeyCount = numKeys / shiftEveryNthRows * (shiftEveryNthRows - 1);

            Preconditions.checkArgument(0 == numKeys % shiftEveryNthRows);
            Preconditions.checkArgument(10 < numKeys / shiftEveryNthRows);

            final String[][] values = generateValues();
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
     * @throws BackendException shouldn't happen
     * @throws IOException      shouldn't happen
     */
    @Test
    public void testGetKeysColumnSlicesOnLowerTriangular() throws BackendException, IOException {
        if (manager.getFeatures().hasScan()) {
            final int offset = 10;
            final int size = 10;
            final int midpoint = size / 2 + offset;
            final int upper = offset + size;
            final int step = 1;
            Preconditions.checkArgument(4 <= size);
            Preconditions.checkArgument(1 <= offset);

            loadLowerTriangularValues(size, offset);

            boolean executed = false;

            if (manager.getFeatures().hasUnorderedScan()) {

                final Collection<StaticBuffer> expected = new HashSet<StaticBuffer>(size);

                for (int start = midpoint; start >= offset - step; start -= step) {
                    for (int end = midpoint + 1; end <= upper + step; end += step) {
                        Preconditions.checkArgument(start < end);

                        // Set column bounds
                        final StaticBuffer startCol = BufferUtil.getIntBuffer(start);
                        final StaticBuffer endCol = BufferUtil.getIntBuffer(end);
                        final SliceQuery sq = new SliceQuery(startCol, endCol, "testGetKeysColumnSlicesOnLowerTriangular");

                        // Compute expectation
                        expected.clear();
                        for (long l = Math.max(start, offset); l < upper; l++) {
                            expected.add(BufferUtil.getLongBuffer(l));
                        }

                        // Compute actual
                        final KeyIterator i = store.getKeys(sq, tx);
                        final Collection<StaticBuffer> actual = Sets.newHashSet(i);

                        // Check
                        log.debug("Checking bounds [{}, {}) (expect {} keys)",
                                new Object[]{startCol, endCol, expected.size()});
                        Assert.assertEquals(expected, actual);
                        i.close();
                        executed = true;
                    }
                }

            } else if (manager.getFeatures().hasOrderedScan()) {

                final Collection<StaticBuffer> expected = new ArrayList<StaticBuffer>(size);

                for (int start = midpoint; start >= offset - step; start -= step) {
                    for (int end = midpoint + 1; end <= upper + step; end += step) {
                        Preconditions.checkArgument(start < end);

                        // Set column bounds
                        final StaticBuffer startCol = BufferUtil.getIntBuffer(start);
                        final StaticBuffer endCol = BufferUtil.getIntBuffer(end);
                        final SliceQuery sq = new SliceQuery(startCol, endCol, "testGetKeysColumnSlicesOnLowerTriangular");

                        // Set key bounds
                        final StaticBuffer keyStart = BufferUtil.getLongBuffer(start);
                        final StaticBuffer keyEnd = BufferUtil.getLongBuffer(end);
                        final KeyRangeQuery krq = new KeyRangeQuery(keyStart, keyEnd, sq);

                        // Compute expectation
                        expected.clear();
                        for (long l = Math.max(start, offset); l < Math.min(upper, end); l++) {
                            expected.add(BufferUtil.getLongBuffer(l));
                        }

                        // Compute actual
                        final KeyIterator i = store.getKeys(krq, tx);
                        final Collection<StaticBuffer> actual = Lists.newArrayList(i);

                        log.debug("Checking bounds key:[{}, {}) & col:[{}, {}) (expect {} keys)",
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
                           int start, int end, int limit) throws BackendException {
        tx.rollback();
        tx = startTx();
        List<Entry> entries;
        if (limit <= 0)
            entries = store.getSlice(new KeySliceQuery(KeyValueStoreUtil.getBuffer(key), KeyValueStoreUtil.getBuffer(start), KeyValueStoreUtil.getBuffer(end), "checkSlice"), tx);
        else
            entries = store.getSlice(new KeySliceQuery(KeyValueStoreUtil.getBuffer(key), KeyValueStoreUtil.getBuffer(start), KeyValueStoreUtil.getBuffer(end), "checkSlice").setLimit(limit), tx);

        int pos = 0;
        for (int i = start; i < end; i++) {
            if (removed.contains(new KeyColumn(key, i))) {
                log.debug("Skipping deleted ({},{})", key, i);
                continue;
            }
            if (limit <= 0 || pos < limit) {
                log.debug("Checking k={}[c_start={},c_end={}](limit={}): column index={}/pos={}", key, start, end, limit, i, pos);
                Assert.assertTrue(entries.size() > pos);
                final Entry entry = entries.get(pos);
                final int col = KeyValueStoreUtil.getID(entry.getColumn());
                final String str = KeyValueStoreUtil.getString(entry.getValueAs(StaticBuffer.STATIC_FACTORY));
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
    public void intervalTest1() throws BackendException {
        final String[][] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        final Set<KeyColumn> deleted = Sets.newHashSet();
        clopen();
        checkRandomSlices(values, deleted, TRIALS);
    }

    @Test
    public void intervalTest2() throws BackendException {
        final String[][] values = generateValues();
        log.debug("Loading values...");
        loadValues(values);
        newTx();
        final Set<KeyColumn> deleted = deleteValues(7);
        clopen();
        checkRandomSlices(values, deleted, TRIALS);
    }

    protected void checkRandomSlices(String[][] values, Set<KeyColumn> deleted, int trials) throws BackendException {
        for (int t = 0; t < trials; t++) {
            final int key = RandomGenerator.randomInt(0, numKeys);
            final int start = RandomGenerator.randomInt(0, numColumns);
            final int end = RandomGenerator.randomInt(start, numColumns);
            final int limit = RandomGenerator.randomInt(1, 30);
            checkSlice(values, deleted, key, start, end, limit);
            checkSlice(values, deleted, key, start, end, -1);
        }
    }

    @Test
    public void testConcurrentGetSlice() throws ExecutionException, InterruptedException, BackendException {
        testConcurrentStoreOps(false, TRIALS);
    }

    @Test
    public void testConcurrentGetSliceAndMutate() throws BackendException, ExecutionException, InterruptedException {
        testConcurrentStoreOps(true, TRIALS);
    }

    protected void testConcurrentStoreOps(boolean deletionEnabled, int trials) throws BackendException, ExecutionException, InterruptedException {
        // Load data fixture
        final String[][] values = generateValues();
        loadValues(values);

        /*
         * Must reopen transaction prior to deletes.
         *
         * This is due to the tx timestamps semantics.  The timestamp is set once
         * during the lifetime of the transaction, and multiple calls to mutate will
         * use the same timestamp on each call.  This causes deletions and additions of the
         * same k-v coordinates made in the same tx to conflict.  On Cassandra, the
         * addition will win and the delete will appear to be dropped.
         *
         * The transaction open right now has already loaded the test fixtures, so any
         * attempt to delete some of the fixture will appear to fail if carried out in this
         * transaction.
         */
        tx.commit();
        tx = startTx();

        // Setup executor and runnables
        final int NUM_THREADS = 64;
        final ExecutorService es = Executors.newFixedThreadPool(NUM_THREADS);
        final List<Runnable> tasks = new ArrayList<>(NUM_THREADS);
        for (int i = 0; i < NUM_THREADS; i++) {
            final Set<KeyColumn> deleted = Sets.newHashSet();
            if (!deletionEnabled) {
                tasks.add(new ConcurrentRandomSliceReader(values, deleted, trials));
            } else {
                tasks.add(new ConcurrentRandomSliceReader(values, deleted, i, trials));
            }
        }
        final List<Future<?>> futures = new ArrayList<>(NUM_THREADS);

        // Execute
        for (final Runnable r : tasks) {
            futures.add(es.submit(r));
        }

        // Block to completion (and propagate any ExecutionExceptions that fall out of get)
        int collected = 0;
        for (final Future<?> f : futures) {
            f.get();
            collected++;
        }

        assertEquals(NUM_THREADS, collected);
    }

    protected class ConcurrentRandomSliceReader implements Runnable {

        private final String[][] values;
        private final Set<KeyColumn> d;
        private final int startKey;
        private final int endKey;
        private final boolean deletionEnabled;
        private final int trials;

        public ConcurrentRandomSliceReader(String[][] values, Set<KeyColumn> deleted, int trials) {
            this.values = values;
            this.d = deleted;
            this.startKey = 0;
            this.endKey = values.length;
            this.deletionEnabled = false;
            this.trials = trials;
        }

        public ConcurrentRandomSliceReader(String[][] values, Set<KeyColumn> deleted, int key, int trials) {
            this.values = values;
            this.d = deleted;
            this.startKey = key % values.length;
            this.endKey = startKey + 1;
            this.deletionEnabled = true;
            this.trials = trials;
        }

        @Override
        public void run() {
            for (int t = 0; t < trials; t++) {
                final int key = RandomGenerator.randomInt(startKey, endKey);
                log.debug("Random key chosen: {} (start={}, end={})", key, startKey, endKey);
                int start = RandomGenerator.randomInt(0, numColumns);
                if (start == numColumns - 1) {
                    start = numColumns - 2;
                }
                final int end = RandomGenerator.randomInt(start + 1, numColumns);
                final int limit = RandomGenerator.randomInt(1, 30);
                try {
                    if (deletionEnabled) {
                        final int delCol = RandomGenerator.randomInt(start, end);
                        final ImmutableList<StaticBuffer> deletions = ImmutableList.of(KeyValueStoreUtil.getBuffer(delCol));
                        store.mutate(KeyValueStoreUtil.getBuffer(key), KeyColumnValueStore.NO_ADDITIONS, deletions, tx);
                        log.debug("Deleting ({},{})", key, delCol);
                        d.add(new KeyColumn(key, delCol));
                        tx.commit();
                        tx = startTx();
                    }
                    //clopen();
                    checkSlice(values, d, key, start, end, limit);
                    checkSlice(values, d, key, start, end, -1);
                } catch (final BackendException e) {
                    throw new RuntimeException(e);
                }
            }
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
        final StaticBuffer key = KeyColumnValueStoreUtil.longToByteBuffer(0);

        final int cols = 1024;

        final List<Entry> entries = new LinkedList<Entry>();
        for (int i = 0; i < cols; i++) {
            final StaticBuffer col = KeyColumnValueStoreUtil.longToByteBuffer(i);
            entries.add(StaticArrayEntry.of(col, col));
        }
        store.mutate(key, entries, KeyColumnValueStore.NO_DELETIONS, tx);
        tx.commit();

        tx = startTx();
        /*
         * When limit is greater than or equal to the matching column count ,
         * all matching columns must be returned.
         */
        final StaticBuffer columnStart = KeyColumnValueStoreUtil.longToByteBuffer(0);
        final StaticBuffer columnEnd = KeyColumnValueStoreUtil.longToByteBuffer(cols);
        List<Entry> result =
                store.getSlice(new KeySliceQuery(key, columnStart, columnEnd, "getSliceRespectsColumnLimit").setLimit(cols), tx);
        Assert.assertEquals(cols, result.size());

        for (int i = 0; i < result.size(); i++) {
            final Entry src = entries.get(i);
            final Entry dst = result.get(i);
            if (!src.equals(dst)) {
                final int x = 1;
            }
        }

        Assert.assertEquals(entries, result);
        result = store.getSlice(new KeySliceQuery(key, columnStart, columnEnd, "getSliceRespectsColumnLimit").setLimit(cols + 10), tx);
        Assert.assertEquals(cols, result.size());
        Assert.assertEquals(entries, result);

        /*
         * When limit is less the matching column count, the columns up to the
         * limit (ordered bytewise) must be returned.
         */
        result = store.getSlice(new KeySliceQuery(key, columnStart, columnEnd, "getSliceRespectsColumnLimit").setLimit(cols - 1), tx);
        Assert.assertEquals(cols - 1, result.size());
        entries.remove(entries.size() - 1);
        Assert.assertEquals(entries, result);
        result = store.getSlice(new KeySliceQuery(key, columnStart, columnEnd, "getSliceRespectsColumnLimit").setLimit(1), tx);
        Assert.assertEquals(1, result.size());
        final List<Entry> firstEntrySingleton = Arrays.asList(entries.get(0));
        Assert.assertEquals(firstEntrySingleton, result);
    }

    @Test
    public void getSliceRespectsAllBoundsInclusionArguments() throws Exception {
        // Test case where endColumn=startColumn+1
        final StaticBuffer key = KeyColumnValueStoreUtil.longToByteBuffer(0);
        final StaticBuffer columnBeforeStart = KeyColumnValueStoreUtil.longToByteBuffer(776);
        final StaticBuffer columnStart = KeyColumnValueStoreUtil.longToByteBuffer(777);
        final StaticBuffer columnEnd = KeyColumnValueStoreUtil.longToByteBuffer(778);
        final StaticBuffer columnAfterEnd = KeyColumnValueStoreUtil.longToByteBuffer(779);

        // First insert four test Entries
        final List<Entry> entries = Arrays.asList(
                StaticArrayEntry.of(columnBeforeStart, columnBeforeStart),
                StaticArrayEntry.of(columnStart, columnStart),
                StaticArrayEntry.of(columnEnd, columnEnd),
                StaticArrayEntry.of(columnAfterEnd, columnAfterEnd));
        store.mutate(key, entries, KeyColumnValueStore.NO_DELETIONS, tx);
        tx.commit();

        // getSlice() with only start inclusive
        tx = startTx();
        final List<Entry> result = store.getSlice(new KeySliceQuery(key, columnStart, columnEnd, "getSliceRespectsAllBoundsInclusionArguments"), tx);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(777, KeyColumnValueStoreUtil.bufferToLong(result.get(0).getColumn()));

    }

    @Test
    public void containsKeyReturnsTrueOnExtantKey() throws Exception {
        final StaticBuffer key1 = KeyColumnValueStoreUtil.longToByteBuffer(1);
        Assert.assertFalse(KCVSUtil.containsKey(store, key1, tx));
        KeyColumnValueStoreUtil.insert(store, tx, 1, "c", "v");
        tx.commit();

        tx = startTx();
        Assert.assertTrue(KCVSUtil.containsKey(store, key1, tx));
    }

    @Test
    public void containsKeyReturnsFalseOnNonexistentKey() throws Exception {
        final StaticBuffer key1 = KeyColumnValueStoreUtil.longToByteBuffer(1);
        Assert.assertFalse(KCVSUtil.containsKey(store, key1, tx));
    }

    @Test
    public void containsKeyColumnReturnsFalseOnNonexistentInput()
            throws Exception {
        final StaticBuffer key1 = KeyColumnValueStoreUtil.longToByteBuffer(1);
        final StaticBuffer c = KeyColumnValueStoreUtil.stringToByteBuffer("c");
        Assert.assertFalse(KCVSUtil.containsKeyColumn(store, key1, c, tx));
    }

    @Test
    public void containsKeyColumnReturnsTrueOnExtantInput() throws Exception {
        KeyColumnValueStoreUtil.insert(store, tx, 1, "c", "v");
        tx.commit();
        tx = startTx();
        final StaticBuffer key1 = KeyColumnValueStoreUtil.longToByteBuffer(1);
        final StaticBuffer c = KeyColumnValueStoreUtil.stringToByteBuffer("c");
        Assert.assertTrue(KCVSUtil.containsKeyColumn(store, key1, c, tx));
    }

    @Test
    public void testGetSlices() throws Exception {
        if (!manager.getFeatures().hasMultiQuery()) return;

        populateDBWith100Keys();

        tx.commit();
        tx = startTx();

        final List<StaticBuffer> keys = new ArrayList<StaticBuffer>(100);

        for (int i = 1; i <= 100; i++) {
            keys.add(KeyColumnValueStoreUtil.longToByteBuffer(i));
        }

        final StaticBuffer start = KeyColumnValueStoreUtil.stringToByteBuffer("a");
        final StaticBuffer end = KeyColumnValueStoreUtil.stringToByteBuffer("d");

        final Map<StaticBuffer,EntryList> results = store.getSlice(keys, new SliceQuery(start, end, "testGetSlices"), tx);

        Assert.assertEquals(100, results.size());

        for (final List<Entry> entries : results.values()) {
            Assert.assertEquals(3, entries.size());
        }
    }

    @Test
    @Category({UnorderedKeyStoreTests.class})
    public void testGetKeysWithSliceQuery() throws Exception {
        if (!manager.getFeatures().hasUnorderedScan()) {
            log.warn("Can't test key-unordered features on incompatible store.  "
                    + "This warning could indicate reduced test coverage and "
                    + "a broken JUnit configuration.  Skipping test {}.",
                    name.getMethodName());
            return;
        }

        populateDBWith100Keys();

        tx.commit();
        tx = startTx();

        final KeyIterator keyIterator = store.getKeys(
                new SliceQuery(new ReadArrayBuffer("b".getBytes()),
                        new ReadArrayBuffer("c".getBytes()), "testGetKeysWithSliceQuery"), tx);

        examineGetKeysResults(keyIterator, 0, 100, 1);
    }

    @Test
    @Category({OrderedKeyStoreTests.class})
    public void testGetKeysWithKeyRange() throws Exception {
        if (!manager.getFeatures().hasOrderedScan()) {
            log.warn("Can't test ordered scans on incompatible store.  "
                    + "This warning could indicate reduced test coverage and "
                    + "shouldn't happen in an ideal JUnit configuration.  "
                    + "Skipping test {}.", name.getMethodName());
            return;
        }

        populateDBWith100Keys();

        tx.commit();
        tx = startTx();

        final KeyIterator keyIterator = store.getKeys(new KeyRangeQuery(
                KeyColumnValueStoreUtil.longToByteBuffer(10), // key start
                KeyColumnValueStoreUtil.longToByteBuffer(40), // key end
                new ReadArrayBuffer("b".getBytes()), // column start
                new ReadArrayBuffer("c".getBytes()), "testGetKeysWithKeyRange"), tx);

        examineGetKeysResults(keyIterator, 10, 40, 1);
    }

    @Category({ BrittleTests.class })
    @Test
    public void testTtl() throws Exception {

        if (!manager.getFeatures().hasCellTTL()) {
            return;
        }

        final StaticBuffer key = KeyColumnValueStoreUtil.longToByteBuffer(0);

        final int ttls[] = new int[]{0, 1, 2};
        final List<Entry> additions = new LinkedList<Entry>();
        for (int i = 0; i < ttls.length; i++) {
            final StaticBuffer col = KeyColumnValueStoreUtil.longToByteBuffer(i);
            final StaticArrayEntry entry = (StaticArrayEntry) StaticArrayEntry.of(col, col);
            entry.setMetaData(EntryMetaData.TTL, ttls[i]);
            additions.add(entry);
        }

        store.mutate(key, additions, KeyColumnValueStore.NO_DELETIONS, tx);
        tx.commit();
        // commitTime starts just after the commit, so we won't check for expiration too early
        final long commitTime = System.currentTimeMillis();

        tx = startTx();

        final StaticBuffer columnStart = KeyColumnValueStoreUtil.longToByteBuffer(0);
        final StaticBuffer columnEnd = KeyColumnValueStoreUtil.longToByteBuffer(ttls.length);
        List<Entry> result =
                store.getSlice(new KeySliceQuery(key, columnStart, columnEnd, "testTtl").setLimit(ttls.length), tx);
        Assert.assertEquals(ttls.length, result.size());

        // wait for one cell to expire
        Thread.sleep(commitTime + 1001 - System.currentTimeMillis());

        // cells immediately expire upon TTL, even before rollback()
        result =
                store.getSlice(new KeySliceQuery(key, columnStart, columnEnd, "testTtl").setLimit(ttls.length), tx);
        Assert.assertEquals(ttls.length - 1, result.size());

        tx.rollback();
        result =
                store.getSlice(new KeySliceQuery(key, columnStart, columnEnd, "testTtl").setLimit(ttls.length), tx);
        Assert.assertEquals(ttls.length - 1, result.size());

        Thread.sleep(commitTime + 2001 - System.currentTimeMillis());
        tx.rollback();
        result =
                store.getSlice(new KeySliceQuery(key, columnStart, columnEnd, "testTtl").setLimit(ttls.length), tx);
        Assert.assertEquals(ttls.length - 2, result.size());

        // cell 0 doesn't expire due to TTL of 0 (infinite)
        Thread.sleep(commitTime + 4001 - System.currentTimeMillis());
        tx.rollback();
        result =
                store.getSlice(new KeySliceQuery(key, columnStart, columnEnd, "testTtl").setLimit(ttls.length), tx);
        Assert.assertEquals(ttls.length - 2, result.size());
    }

    @Test
    public void testStoreTTL() throws Exception {
        KeyColumnValueStoreManager storeManager = manager;
        // TTLKCVSManager is only used when a store has cell-level TTL support but does not have store-
        // level TTL.
        // @see TTLKCVSManager
        if (storeManager.getFeatures().hasCellTTL() && !storeManager.getFeatures().hasStoreTTL()) {
            storeManager = new TTLKCVSManager(storeManager);
        } else if (!storeManager.getFeatures().hasStoreTTL()) {
            return;
        }


        assertTrue(storeManager.getFeatures().hasStoreTTL());

        final TimeUnit sec = TimeUnit.SECONDS;
        final int storeTTLSeconds = (int)TestGraphConfigs.getTTL(sec);
        final StoreMetaData.Container opts = new StoreMetaData.Container();
        opts.put(StoreMetaData.TTL, storeTTLSeconds);
        final KeyColumnValueStore storeWithTTL = storeManager.openDatabase("testStore_with_TTL", opts);

        populateDBWith100Keys(storeWithTTL);

        tx.commit();
        tx = startTx();

        final StaticBuffer key = KeyColumnValueStoreUtil.longToByteBuffer(2);

        final StaticBuffer start = KeyColumnValueStoreUtil.stringToByteBuffer("a");
        final StaticBuffer end = KeyColumnValueStoreUtil.stringToByteBuffer("d");

        EntryList results = storeWithTTL.getSlice(new KeySliceQuery(key, new SliceQuery(start, end, "testStoreTTL")), tx);
        Assert.assertEquals(3, results.size());

        Thread.sleep(TimeUnit.MILLISECONDS.convert((long)Math.ceil(storeTTLSeconds * 1.25), sec));

        tx.commit();
        tx = startTx();

        results = storeWithTTL.getSlice(new KeySliceQuery(key, new SliceQuery(start, end, "testStoreTTL")), tx);
        Assert.assertEquals(0, results.size()); // should be empty if TTL was applied properly

        storeWithTTL.close();
    }

    protected void populateDBWith100Keys() throws Exception {
        populateDBWith100Keys(store);
    }

    protected void populateDBWith100Keys(KeyColumnValueStore store) throws Exception {
        final Random random = new Random();

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
            throws BackendException {
        Assert.assertNotNull(keyIterator);

        int count = 0;
        final int expectedNumKeys = (int) (endKey - startKey);
        final List<StaticBuffer> existingKeys = new ArrayList<StaticBuffer>(expectedNumKeys);

        for (int i = (int) (startKey == 0 ? 1 : startKey); i <= endKey; i++) {
            existingKeys.add(KeyColumnValueStoreUtil.longToByteBuffer(i));
        }

        while (keyIterator.hasNext()) {
            final StaticBuffer key = keyIterator.next();

            Assert.assertNotNull(key);
            Assert.assertTrue(existingKeys.contains(key));

            final RecordIterator<Entry> entries = keyIterator.getEntries();

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

    @Test
    public void scanTestWithSimpleJob() throws Exception {
        final int keys = 1000, columns = 40;
        final String[][] values = KeyValueStoreUtil.generateData(keys, columns);
        //Make it only half the number of columns for every 2nd key
        for (int i = 0; i < values.length; i++) {
            if (i%2==0) values[i]=Arrays.copyOf(values[i],columns/2);
        }
        log.debug("Loading values: " + keys + "x" + columns);
        loadValues(values);
        clopen();

        final StandardScanner scanner = new StandardScanner(manager);
        final SimpleScanJobRunner runner = (ScanJob job, Configuration jobConf, String rootNSName) -> runSimpleJob(scanner, job, jobConf);

        SimpleScanJob.runBasicTests(keys, columns, runner);
    }

    private ScanMetrics runSimpleJob(StandardScanner scanner, ScanJob job, Configuration jobConf) throws BackendException, ExecutionException, InterruptedException {
        final StandardScanner.Builder jobBuilder = scanner.build();
        jobBuilder.setStoreName(store.getName());
        jobBuilder.setJobConfiguration(jobConf);
        jobBuilder.setNumProcessingThreads(2);
        jobBuilder.setWorkBlockSize(100);
        jobBuilder.setTimestampProvider(times);
        jobBuilder.setJob(job);
        return jobBuilder.execute().get();
    }

    @Test
    public void testClearStorage() throws Exception {
        final String[][] values = generateValues();
        loadValues(values);
        close();

        manager = openStorageManagerForClearStorageTest();
        assertTrue("storage should exist before clearing", manager.exists());
        manager.clearStorage();
        try {
            assertFalse("storage should not exist after clearing", manager.exists());
        } catch (final Exception e) {
            // Retry to accommodate backends (e.g. BerkeleyDB) which may require a clean manager after clearing storage
            manager.close();
            manager = openStorageManager();
            assertFalse("storage should not exist after clearing", manager.exists());
        }
    }

    protected KeyColumnValueStoreManager openStorageManagerForClearStorageTest() throws Exception {
        return openStorageManager();
    }

}
