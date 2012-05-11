package com.thinkaurelius.titan.diskstorage.test;


import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.util.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.util.KeyValueStorageManager;
import com.thinkaurelius.titan.diskstorage.util.OrderedKeyValueStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.thinkaurelius.titan.diskstorage.test.StorageTest.*;
import static org.junit.Assert.*;

public abstract class KeyValueStoreTest {

	private Logger log = LoggerFactory.getLogger(KeyValueStoreTest.class);

	int numKeys = 2000;

	
	KeyValueStorageManager manager;
	TransactionHandle tx;
	OrderedKeyValueStore store;
	
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
	
	
	public String[] generateValues() {
		return generateData(numKeys);
	}
	
	public void loadValues(String[] values) {
		List<KeyValueEntry> entries = new ArrayList<KeyValueEntry>();
		for (int i=0;i<numKeys;i++) {
			entries.add(new KeyValueEntry(getBuffer(i),getBuffer(values[i])));
		}
		store.insert(entries, tx);
	}
	
	public Set<Integer> deleteValues(int start, int every) {
		Set<Integer> removed = new HashSet<Integer>();
		List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
		for (int i=start;i<numKeys;i=i+every) {
			removed.add(i);
			keys.add(getBuffer(i));
		}
		store.delete(keys, tx);
		return removed;
	}
	
	public void checkValueExistence(String[] values) {
		checkValueExistence(values,new HashSet<Integer>());
	}
	
	public void checkValueExistence(String[] values, Set<Integer> removed) {
		for (int i=0;i<numKeys;i++) {
			boolean result = store.containsKey(getBuffer(i), tx);
			if (removed.contains(i)) {
				assertFalse(result);
			} else {
				assertTrue(result);
			}
		}
	}
	
	public void checkValues(String[] values) {
		checkValues(values,new HashSet<Integer>());
	}
	
	public void checkValues(String[] values, Set<Integer> removed) {
		for (int i=0;i<numKeys;i++) {
			ByteBuffer result = store.get(getBuffer(i), tx);
			if (removed.contains(i)) {
				assertNull(result);
			} else {
				assertEquals(values[i],getString(result));
			}
		}
	}
	
	@Test
	public void storeAndRetrieve() {
		String[] values = generateValues();
		log.debug("Loading values...");
		loadValues(values);

		log.debug("Checking values...");
		checkValueExistence(values);
		checkValues(values);
	}
	
	@Test
	public void storeAndRetrieveWithClosing() {
		String[] values = generateValues();
		log.debug("Loading values...");
		loadValues(values);
		close();
		open();
		log.debug("Checking values...");
		checkValueExistence(values);
		checkValues(values);
	}
	
	@Test
	public void deletionTest1() {
		String[] values = generateValues();
		log.debug("Loading values...");
		loadValues(values);
		close();
		open();
		Set<Integer> deleted = deleteValues(0,10);
		log.debug("Checking values...");
		checkValueExistence(values,deleted);
		checkValues(values,deleted);
	}
	
	@Test
	public void deletionTest2() {
		String[] values = generateValues();
		log.debug("Loading values...");
		loadValues(values);
		Set<Integer> deleted = deleteValues(0,10);
		close();
		open();
		log.debug("Checking values...");
		checkValueExistence(values,deleted);
		checkValues(values,deleted);
	}
	
	public void checkSlice(String[] values, Set<Integer> removed, int start, int end, int limit) {
		List<KeyValueEntry> entries;
		if (limit<=0)
			entries = store.getSlice(getBuffer(start), getBuffer(end), tx);
		else
			entries = store.getSlice(getBuffer(start), getBuffer(end), limit, tx);
		
		int pos=0;
		for (int i=start;i<end;i++) {
			if (removed.contains(i)) continue;
			if (pos<limit) {
			KeyValueEntry entry = entries.get(pos);
			int id = getID(entry.getKey());
			String str = getString(entry.getValue());
			assertEquals(i,id);
			assertEquals(values[i],str);
			}
			pos++;
		}
		if (limit>0 && pos>=limit) assertEquals(limit,entries.size());
		else {
			assertNotNull(entries);
			assertEquals(pos,entries.size());
		}		
	}
	
	@Test
	public void intervalTest1() {
		String[] values = generateValues();
		log.debug("Loading values...");
		loadValues(values);
		Set<Integer> deleted = deleteValues(0,10);
		close();
		open();
//		for (int i : deleted) {
//			log.debug("AtomicInterval for : {}",i);
//			assertFalse(store.containsInInterval(getBuffer(i), getBuffer(i+1), tx));
//			if (i+1<numKeys) {
//				assertTrue(store.containsInInterval(getBuffer(i), getBuffer(i+2), tx));
//				assertTrue(store.containsInInterval(getBuffer(i), getBuffer(i+12), tx));
//				assertTrue(store.containsInInterval(getBuffer(i), getBuffer(i+118), tx));
//			}
//		}
		checkSlice(values,deleted,5,25,-1);
		checkSlice(values,deleted,5,250,10);
		checkSlice(values,deleted,500,1250,-1);
		checkSlice(values,deleted,500,1250,1000);
		checkSlice(values,deleted,500,1250,100);
		checkSlice(values,deleted,50,20,10);
		checkSlice(values,deleted,50,20,-1);
	
	}



}
 