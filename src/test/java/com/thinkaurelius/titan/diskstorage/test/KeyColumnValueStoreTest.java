package com.thinkaurelius.titan.diskstorage.test;


import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.cassandra.Util;
import com.thinkaurelius.titan.util.test.RandomGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

import static com.thinkaurelius.titan.diskstorage.test.StorageTest.*;
import static org.junit.Assert.*;

public abstract class KeyColumnValueStoreTest {

	private Logger log = LoggerFactory.getLogger(KeyValueStoreTest.class);

	int numKeys = 500;
	int numColumns = 50;

	
	public StorageManager manager;
	public TransactionHandle tx;
	public OrderedKeyColumnValueStore store;
	
	@Before
	public void setUp() throws Exception {
		DiskgraphTest.deleteHomeDir();
		open();
	}
	
	public abstract void open();
	
	@After
	public void tearDown() throws Exception {
		close();
	}
	
	public abstract void close();

	@Test
	public void createDatabase() {
		//Just setup and shutdown
	}
	
	
	public String[][] generateValues() {
		return generateData(numKeys,numColumns);
	}
	
	public void loadValues(String[][] values) {
		for (int i=0;i<numKeys;i++) {
			List<Entry> entries = new ArrayList<Entry>();
			for (int j=0;j<numColumns;j++) {
				entries.add(new Entry(getBuffer(j),getBuffer(values[i][j])));
			}
			store.insert(getBuffer(i), entries, tx);
		}
	}
	
	public Set<KeyColumn> deleteValues(int every) {
		Set<KeyColumn> removed = new HashSet<KeyColumn>();
		int counter = 0;
		for (int i=0;i<numKeys;i++) {
			List<ByteBuffer> deletions = new ArrayList<ByteBuffer>();
			for (int j=0;j<numColumns;j++) {
				counter++;
				if (counter%every==0) {
					//remove
					removed.add(new KeyColumn(i,j));
					deletions.add(getBuffer(j));
				}
			}
			store.delete(getBuffer(i), deletions, tx);
		}
		return removed;
	}
	
	public Set<Integer> deleteKeys(int every) {
		Set<Integer> removed = new HashSet<Integer>();
		for (int i=0;i<numKeys;i++) {
			if (i%every==0) {
				removed.add(i);
				List<ByteBuffer> deletions = new ArrayList<ByteBuffer>();
				for (int j=0;j<numColumns;j++) {
					deletions.add(getBuffer(j));
				}
				store.delete(getBuffer(i), deletions, tx);
			}
		}
		return removed;
	}
	
	public void checkKeys(Set<Integer> removed) {
		for (int i=0;i<numKeys;i++) {
			if (removed.contains(i)) {
				assertFalse(store.containsKey(getBuffer(i), tx));
			} else {
				assertTrue(store.containsKey(getBuffer(i), tx));
			}
		}
	}
	
	public void checkValueExistence(String[][] values) {
		checkValueExistence(values,new HashSet<KeyColumn>());
	}
	
	public void checkValueExistence(String[][] values, Set<KeyColumn> removed) {
		for (int i=0;i<numKeys;i++) {
			for (int j=0;j<numColumns;j++) {
				boolean result = store.containsKeyColumn(getBuffer(i),getBuffer(j),tx);
				if (removed.contains(new KeyColumn(i,j))) {
					assertFalse(result);
				} else {
					assertTrue(result);
				}
			}
		}
	}
	
	public void checkValues(String[][] values) {
		checkValues(values,new HashSet<KeyColumn>());
	}
	
	public void checkValues(String[][] values, Set<KeyColumn> removed) {
		for (int i=0;i<numKeys;i++) {
			for (int j=0;j<numColumns;j++) {
				ByteBuffer result = store.get(getBuffer(i),getBuffer(j),tx);
				if (removed.contains(new KeyColumn(i,j))) {
					assertNull(result);
				} else {
					assertEquals(values[i][j],getString(result));
				}
			}
		}

	}
	
	@Test
	public void storeAndRetrieve() {
		String[][] values = generateValues();
		log.debug("Loading values...");
		loadValues(values);
		//print(values);
		log.debug("Checking values...");
		checkValueExistence(values);
		checkValues(values);
	}
	
	@Test
	public void storeAndRetrieveWithClosing() {
		String[][] values = generateValues();
		log.debug("Loading values...");
		loadValues(values);
		close();
		open();
		log.debug("Checking values...");
		checkValueExistence(values);
		checkValues(values);
	}
	
	@Test
	public void deleteColumnsTest1() {
		String[][] values = generateValues();
		log.debug("Loading values...");
		loadValues(values);
		close();
		open();
		Set<KeyColumn> deleted = deleteValues(7);
		log.debug("Checking values...");
		checkValueExistence(values,deleted);
		checkValues(values,deleted);
	}
	
	@Test
	public void deleteColumnsTest2() {
		String[][] values = generateValues();
		log.debug("Loading values...");
		loadValues(values);
		Set<KeyColumn> deleted = deleteValues(7);
		close();
		open();
		log.debug("Checking values...");
		checkValueExistence(values,deleted);
		checkValues(values,deleted);
	}
	
	@Test
	public void deleteKeys() {
		String[][] values = generateValues();
		log.debug("Loading values...");
		loadValues(values);
		Set<Integer> deleted = deleteKeys(11);
		close();
		open();
		checkKeys(deleted);
	}
	
	public void checkSlice(String[][] values, Set<KeyColumn> removed, int key, int start, int end, int limit) {
		List<Entry> entries;
		if (limit<=0)
			entries = store.getSlice(getBuffer(key), getBuffer(start), getBuffer(end), true, false, tx);
		else
			entries = store.getLimitedSlice(getBuffer(key), getBuffer(start), getBuffer(end), true, false, limit, tx);
		
		int pos=0;
		for (int i=start;i<end;i++) {
			if (removed.contains(new KeyColumn(key,i))) continue;
			if (entries!=null) {
				Entry entry = entries.get(pos);
				int col = getID(entry.getColumn());
				String str = getString(entry.getValue());
				assertEquals(i,col);
				assertEquals(values[key][i],str);
			}
			pos++;
		}
		if (limit>0 && pos>limit) assertNull(entries);
		else {
			assertNotNull(entries);
			assertEquals(pos,entries.size());
		}		
	}
	
	@Test
	public void intervalTest1() {
		String[][] values = generateValues();
		log.debug("Loading values...");
		loadValues(values);
		Set<KeyColumn> deleted = deleteValues(7);
		close();
		open();
		int trails = 5000;
		for (int t=0;t<trails;t++)  {
			int key = RandomGenerator.randomInt(0, numKeys);
			int start = RandomGenerator.randomInt(0, numColumns);
			int end = RandomGenerator.randomInt(start, numColumns);
			int limit = RandomGenerator.randomInt(1, 30);
			checkSlice(values,deleted,key,start,end,limit);
			checkSlice(values,deleted,key,start,end,-1);
		}

	
	}


	@Test
	public void getNonExistentKeyReturnsNull() throws Exception {
		TransactionHandle txn = manager.beginTransaction();
		assertEquals(null, Util.get(store, txn, 0, "col0"));
		assertEquals(null, Util.get(store, txn, 0, "col1"));
		txn.commit();
	}
	
	@Test
	public void insertingGettingAndDeletingSimpleDataWorks() throws Exception {
		TransactionHandle txn = manager.beginTransaction();
		Util.insert(store, txn, 0, "col0", "val0");
		Util.insert(store, txn, 0, "col1", "val1");
		txn.commit();
		
		txn = manager.beginTransaction();
		assertEquals("val0", Util.get(store, txn, 0, "col0"));
		assertEquals("val1", Util.get(store, txn, 0, "col1"));
		Util.delete(store, txn, 0, "col0");
		Util.delete(store, txn, 0, "col1");
		txn.commit();
		
		txn = manager.beginTransaction();
		assertEquals(null, Util.get(store, txn, 0, "col0"));
		assertEquals(null, Util.get(store, txn, 0, "col1"));
		txn.commit();
	}
	
//	@Test
//	public void getSliceNoLimit() throws Exception {
//		CassandraThriftStorageManager manager = new CassandraThriftStorageManager(keyspace);
//		CassandraThriftOrderedKeyColumnValueStore store =
//			manager.openOrderedDatabase(columnFamily);
//		
//		TransactionHandle txn = manager.beginTransaction();
//		Util.insert(store, txn, "key0", "col0", "val0");
//		Util.insert(store, txn, "key0", "col1", "val1");
//		txn.commit();
//		
//		txn = manager.beginTransaction();
//		ByteBuffer key0 = Util.stringToByteBuffer("key0");
//		ByteBuffer col0 = Util.stringToByteBuffer("col0");
//		ByteBuffer col2 = Util.stringToByteBuffer("col2");
//		List<Entry> entries = store.getSlice(key0, col0, col2, txn);
//		assertNotNull(entries);
//		assertEquals(2, entries.size());
//		assertEquals("col0", Util.byteBufferToString(entries.get(0).getColumn()));
//		assertEquals("val0", Util.byteBufferToString(entries.get(0).getValue()));
//		assertEquals("col1", Util.byteBufferToString(entries.get(1).getColumn()));
//		assertEquals("val1", Util.byteBufferToString(entries.get(1).getValue()));
//		
//		txn.commit();
//		
//		store.close();
//		manager.close();
//	}

	@Test
	public void getSliceRespectsColumnLimit() throws Exception {
		TransactionHandle txn = manager.beginTransaction();
		ByteBuffer key = Util.longToByteBuffer(0);
		
		final int cols = 1024;
		
		List<Entry> entries = new LinkedList<Entry>();
		for (int i = 0; i < cols; i++) {
			ByteBuffer col = Util.longToByteBuffer(i);
			entries.add(new Entry(col, col));
		}
		store.insert(key, entries, txn);
		txn.commit();
				
		txn = manager.beginTransaction();
		ByteBuffer columnStart = Util.longToByteBuffer(0);
		ByteBuffer columnEnd = Util.longToByteBuffer(cols-1);
		/* 
		 * When limit is greater than or equal to the matching column count,
		 * all matching columns must be returned.
		 */
		List<Entry> result =
			store.getSlice(key, columnStart, columnEnd, true, true, cols, txn);
		assertEquals(cols, result.size());
		assertEquals(entries, result);
		result =
			store.getSlice(key, columnStart, columnEnd, true, true, cols+10, txn);
		assertEquals(cols, result.size());
		assertEquals(entries, result);

		/*
		 * When limit is less the matching column count, the columns up to the
		 * limit (ordered bytewise) must be returned.
		 */
		result = 
			store.getSlice(key, columnStart, columnEnd, true, true, cols-1, txn);
		assertEquals(cols-1, result.size());
		entries.remove(entries.size()-1);
		assertEquals(entries, result);
		result = 
			store.getSlice(key, columnStart, columnEnd, true, true, 1, txn);
		assertEquals(1, result.size());
		List<Entry> firstEntrySingleton = Arrays.asList(entries.get(0));
		assertEquals(firstEntrySingleton, result);
		txn.commit();
	}
	
	@Test
	public void getSliceRespectsAllBoundsInclusionArguments() throws Exception {
		// Test case where endColumn=startColumn+1
		ByteBuffer key = Util.longToByteBuffer(0);
		ByteBuffer columnBeforeStart = Util.longToByteBuffer(776);
		ByteBuffer columnStart = Util.longToByteBuffer(777);
		ByteBuffer columnEnd = Util.longToByteBuffer(778);
		ByteBuffer columnAfterEnd = Util.longToByteBuffer(779);
		
		// First insert four test Entries
		TransactionHandle txn = manager.beginTransaction();
		List<Entry> entries = Arrays.asList(
				new Entry(columnBeforeStart, columnBeforeStart),
				new Entry(columnStart, columnStart),
				new Entry(columnEnd, columnEnd),
				new Entry(columnAfterEnd, columnAfterEnd));
		store.insert(key, entries, txn);
		txn.commit();
		
		// getSlice() with startIncl=endIncl=false must return empty
		txn = manager.beginTransaction();
		List<Entry> result = store.getSlice(key, columnStart, columnEnd, false, false, txn);
		assertEquals(0, result.size());
		txn.commit();
		
		// getSlice() with only start inclusive
		txn = manager.beginTransaction();
		result = store.getSlice(key, columnStart, columnEnd, true, false, txn);
		assertEquals(1, result.size());
		assertEquals(777, result.get(0).getColumn().getLong());
		txn.commit();
		
		// getSlice() with only end inclusive
		txn = manager.beginTransaction();
		result = store.getSlice(key, columnStart, columnEnd, false, true, txn);
		assertEquals(1, result.size());
		assertEquals(778, result.get(0).getColumn().getLong());
		txn.commit();
		
		// getSlice() with start and end inclusive must return both data
		txn = manager.beginTransaction();
		result = store.getSlice(key, columnStart, columnEnd, true, true, txn);
		assertEquals(2, result.size());
		assertEquals(777, result.get(0).getColumn().getLong());
		assertEquals(778, result.get(1).getColumn().getLong());
		txn.commit();
	}
	
	@Test
	public void getLimitedSliceReturnsNullWhenLimitIsExceeded() throws Exception {
		TransactionHandle txn = manager.beginTransaction();
		ByteBuffer key = Util.longToByteBuffer(0);
		ByteBuffer c0 = Util.longToByteBuffer(0);
		ByteBuffer c1 = Util.longToByteBuffer(1);
		List<Entry> entries = Arrays.asList(new Entry(c0, c0), new Entry(c1, c1));
		store.insert(key, entries, txn);
		txn.commit();
		
		txn = manager.beginTransaction();
		// Must return normal getSlice() results when limit is not exceeded
		List<Entry> result =
			store.getLimitedSlice(key, c0, c1, true, true, 2, txn);
		assertEquals(2, result.size());
		assertEquals(entries, result);
		result =
			store.getLimitedSlice(key, c0, c1, true, true, 100, txn);
		assertEquals(2, result.size());
		assertEquals(entries, result);
		// Must return null when limit is exceeded
		result =
			store.getLimitedSlice(key, c0, c1, true, true, 1, txn);
		assertNull(result);
		txn.commit();
	}
		
	/*
	 * This unit test is a monster.  It could be broken up into sixteen
	 * little unit tests, each one with a different combination of the
	 * 4 boolean inclusivity arguments to getKeySlice(), but that would
	 * introduce a substantial degree of code duplication.
	 */
	@Test
	public void getKeySliceRespectsAllBoundsInclusionArguments() throws Exception {
		/* Test data:
		 * 
		 * Four contiguous keys, each holding four contiguous columns.
		 * The value of each column is equal to the column itself;
		 * this test case doesn't pay any attention to the values.
		 * 
		 * The byte values of the four keys and four columns are
		 * integers on the interval [776, 779], as determined by
		 * the final variables dataBegin = 776 and dataSize = 4.
		 */
		TransactionHandle txn = manager.beginTransaction();
		final int dataBegin = 776;
		final int dataSize = 4;
		assertTrue("This test case requires dataSize of at least 4", 
				4 <= dataSize); // Required condition
		for (int keyCounter = dataBegin; keyCounter < dataBegin + dataSize;
		     keyCounter++) {
			List<Entry> entries = new LinkedList<Entry>();
			for (int colCounter = dataBegin; colCounter < dataBegin + dataSize;
			     colCounter++) {
				ByteBuffer col = Util.longToByteBuffer(colCounter);
				entries.add(new Entry(col, col));
			}
			ByteBuffer key = Util.longToByteBuffer(keyCounter);
			store.insert(key, entries, txn);
		}
		txn.commit();
		
		// Generate keyStart, keyEnd, colStart, colEnd as ByteBuffers
		ByteBuffer keyStart = Util.longToByteBuffer(dataBegin+1);
		ByteBuffer keyEnd = Util.longToByteBuffer(dataBegin + dataSize - 2);
		ByteBuffer columnStart = keyStart.duplicate();
		ByteBuffer columnEnd = keyEnd.duplicate(); 
		
		/* Iterate over the 16 combinations of
		 * startKeyInc,endKeyInc,startColumnIncl,endColumnIncl booleans
		 */
		for (int i = 0; i < 17; i++) {
			int pow = 0;
			boolean startKeyInc     = 0 == i % (Math.pow(2, pow++));
			boolean endKeyInc       = 0 == i % (Math.pow(2, pow++));
			boolean startColumnIncl = 0 == i % (Math.pow(2, pow++));
			boolean endColumnIncl   = 0 == i % (Math.pow(2, pow++));
			
			Map<ByteBuffer, List<Entry>> expected =
				new HashMap<ByteBuffer, List<Entry>>();
			
			// Generate list of keys expected in output
			List<ByteBuffer> expectedKeys = new LinkedList<ByteBuffer>();
			if (startKeyInc) { // start key
				expectedKeys.add(keyStart.duplicate());
			}
			for (int j = dataBegin + 2; j < dataBegin + dataSize - 2; j++) {
				// Keys between start and end
				ByteBuffer bb = Util.longToByteBuffer(j);
				expectedKeys.add(bb);
			}
			if (endKeyInc) { // end key
				expectedKeys.add(keyEnd.duplicate());
			}
			
			// Associate to each expected key a list of expected columns
			for (ByteBuffer k : expectedKeys) {
				List<Entry> cols = new LinkedList<Entry>();
				if (startColumnIncl) { // start column
					cols.add(new Entry(columnStart, columnStart));
				}
				for (int j = dataBegin + 2; j < dataBegin + dataSize - 2; j++) {
					// columns between start and end
					ByteBuffer bb = Util.longToByteBuffer(j);
					cols.add(new Entry(bb, bb));
				}
				if (endColumnIncl) { // end column
					cols.add(new Entry(columnEnd, columnEnd));
				}
				
				if (0 < cols.size())
					expected.put(k, cols);
			}
			
			// Test expected map vs getKeySlice() result
			// NB: this relies on the DOGMA Entry.equals() method
			txn = manager.beginTransaction();
			Map<ByteBuffer, List<Entry>> result = store.getKeySlice(keyStart,
					keyEnd, startKeyInc, endKeyInc, columnStart, columnEnd,
					startColumnIncl, endColumnIncl, txn);
			assertEquals(expected.size(), result.size());
			assertEquals(expected, result);
			txn.commit();
		}
	}
	
	@Test
	public void getKeySliceArgumentLimitsKeysExaminedByServer() throws Exception {
		TransactionHandle txn = manager.beginTransaction();
		// Setup three keys, each with one column
		// key 0: column 1 (value 1)
		// key 10: column 0 (value 0)
		// key 20: column 1 (value 1)
		int i = 0;
		ByteBuffer key0 = Util.longToByteBuffer(i);
		i += 10;
		ByteBuffer key1 = Util.longToByteBuffer(i);
		i += 10;
		ByteBuffer key2 = Util.longToByteBuffer(i);
		ByteBuffer bb0 = Util.longToByteBuffer(0);
		ByteBuffer bb1 = Util.longToByteBuffer(1);
		List<Entry> entries0 = Arrays.asList(new Entry(bb0, bb0));
		List<Entry> entries1 = Arrays.asList(new Entry(bb1, bb1));
		store.insert(key0, entries1, txn);
		store.insert(key1, entries0, txn);
		store.insert(key2, entries1, txn);
		txn.commit();
		
		txn = manager.beginTransaction();
		Map<ByteBuffer, List<Entry>> expected =
			new HashMap<ByteBuffer, List<Entry>>();
		expected.put(key0, entries1);
		expected.put(key2, entries1);
		/*
		 * Verify that keyLimit = (total number of keys) permits
		 * Cassandra to return all keys matching the get_range_slices
		 * query.  
		 */
		Map<ByteBuffer, List<Entry>> result = 
			store.getKeySlice(key0, key2, true, true, bb1, bb1, true, true, 3, 1, txn);
		assertNotNull(result);
		assertEquals(2, result.size());
		assertEquals(expected, result);
		/*
		 * Verify that the keyLimit parameter (=2 here) limits the
		 * number of keys Cassandra examines while answering the
		 * get_range_slices query, rather than the number of keys
		 * Cassandra ultimately returns to the client.
		 */
		result = 
			store.getKeySlice(key0, key2, true, true, bb1, bb1, true, true, 2, 1, txn);
		assertNotNull(result);
		assertEquals(1, result.size());
		expected.remove(key2);
		assertEquals(expected, result);
		txn.commit();
	}
	
	@Test
	public void getKeyLimitedKeySliceReturnsNullWhenKeyLimitIsExceeded() throws Exception {
		TransactionHandle txn = manager.beginTransaction();
		// Setup data: 1024 keys each with one column
		final int keys = 1024;
		ByteBuffer col = Util.longToByteBuffer(0);
		List<Entry> entries = Arrays.asList(new Entry(col, col));
		Map<ByteBuffer, List<Entry>> expected = 
			new HashMap<ByteBuffer, List<Entry>>();
		for (int i = 0; i < keys; i++) {
			ByteBuffer key = Util.longToByteBuffer(i);
			store.insert(key, entries, txn);
			expected.put(key, entries);
		}
		txn.commit();
		
		txn = manager.beginTransaction();
		ByteBuffer keyStart = Util.longToByteBuffer(0);
		ByteBuffer keyEnd = Util.longToByteBuffer(keys-1);
		// Must return getKeySlice() result when returned keys <= limit
		Map<ByteBuffer, List<Entry>> result =
			store.getLimitedKeySlice(keyStart, keyEnd, true, true, col, col, 
				true, true, keys, 100, txn);
		assertEquals(keys, result.size());
		assertEquals(expected, result);
		result =
			store.getLimitedKeySlice(keyStart, keyEnd, true, true, col, col, 
				true, true, keys+10, 100, txn);
		assertEquals(keys, result.size());
		assertEquals(expected, result);
		// Must return null when returned keys > limit
		assertNull(store.getLimitedKeySlice(keyStart, keyEnd, true, true, 
				col, col, true, true, keys-1, 100, txn));
		assertNull(store.getLimitedKeySlice(keyStart, keyEnd, true, true, 
				col, col, true, true, keys-2, 100, txn));
		assertNull(store.getLimitedKeySlice(keyStart, keyEnd, true, true, 
				col, col, true, true, 1, 100, txn));
		assertNull(store.getLimitedKeySlice(keyStart, keyEnd, true, true, 
				col, col, true, true, 0, 100, txn));
		txn.commit();
	}
	
	@Test
	public void getKeyLimitedKeySliceReturnsNullWhenColumnLimitIsExceededOnAnyKey() throws Exception {
		TransactionHandle txn = manager.beginTransaction();
		// Setup data: 4 contiguous keys, starting with one column
		// on the lowest key and ascending to four columns on the
		// highest key
		final int keys = 4;
		List<Entry> entries = new LinkedList<Entry>();
		Map<ByteBuffer, List<Entry>> expected =
			new HashMap<ByteBuffer, List<Entry>>();
		for (int i = 0; i < keys; i++) {
			ByteBuffer k = Util.longToByteBuffer(i);
			entries.add(new Entry(k, k));
			store.insert(k, entries, txn);
			expected.put(k, new LinkedList<Entry>(entries));
		}
		txn.commit();
		
		txn = manager.beginTransaction();
		ByteBuffer keyStart = Util.longToByteBuffer(0);
		ByteBuffer keyEnd = Util.longToByteBuffer(keys-1);
		ByteBuffer columnStart = keyStart.duplicate();
		ByteBuffer columnEnd = keyEnd.duplicate();
		// Must return getKeySlice() result when column count is under/at limit
		Map<ByteBuffer, List<Entry>> result =
			store.getLimitedKeySlice(keyStart, keyEnd, true, true, 
					columnStart, columnEnd, true, true, keys, keys, txn);
		assertNotNull(result);
		assertEquals(4, result.size());
		assertEquals(expected, result);

		result =
			store.getLimitedKeySlice(keyStart, keyEnd, true, true, 
					columnStart, columnEnd, true, true, keys, keys+100, txn);
		assertNotNull(result);
		assertEquals(4, result.size());
		assertEquals(expected, result);
		
		// Must return null when any per-key column count exceeds limit
		for (int columnLimit = keys - 1; 0 <= columnLimit; columnLimit--) {
		assertNull(store.getLimitedKeySlice(keyStart, keyEnd, true, true, 
				columnStart, columnEnd, true, true, keys, columnLimit, txn));
		}
		txn.commit();
	}
	
	@Test
	public void containsKeyReturnsFalseOnNonexistentKey() throws Exception {
		TransactionHandle txn = manager.beginTransaction();
		ByteBuffer key1 = Util.longToByteBuffer(1);
		assertFalse(store.containsKey(key1.duplicate(), txn));
		txn.commit();
	}
	
	@Test
	public void containsKeyReturnsTrueOnExtantKey() throws Exception {
		TransactionHandle txn = manager.beginTransaction();
		Util.insert(store, txn, 1, "c", "v");
		txn.commit();

		txn = manager.beginTransaction();
		ByteBuffer key1 = Util.longToByteBuffer(1);
		assertTrue(store.containsKey(key1.duplicate(), txn));
		txn.commit();
	}
	
	@Test
	public void containsKeyColumnReturnsFalseOnNonexistentInput() throws Exception {
		TransactionHandle txn = manager.beginTransaction();
		ByteBuffer key1 = Util.longToByteBuffer(1);
		ByteBuffer c = Util.stringToByteBuffer("c");
		assertFalse(store.containsKeyColumn(key1.duplicate(), c.duplicate(), txn));
		txn.commit();
	}
	
	@Test
	public void containsKeyColumnReturnsTrueOnExtantInput() throws Exception {
		TransactionHandle txn = manager.beginTransaction();
		Util.insert(store, txn, 1, "c", "v");
		txn.commit();

		txn = manager.beginTransaction();
		ByteBuffer key1 = Util.longToByteBuffer(1);
		ByteBuffer c = Util.stringToByteBuffer("c");
		assertTrue(store.containsKeyColumn(key1.duplicate(), c.duplicate(), txn));
		txn.commit();
	}


}
 