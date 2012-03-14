package com.thinkaurelius.titan.diskstorage.test;

import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.diskstorage.berkeleydb.je.BerkeleyDBStorageManager;


public class BerkeleyDBjeKeyValueTest extends KeyValueStoreTest {

	boolean transactional = false;
	boolean readOnly = false;
	boolean privateAccess = true;
	int cachePercent = 60;
	String storeName = "testStore1";	
	
	@Override
	public void open() {
		BerkeleyDBStorageManager sm = new BerkeleyDBStorageManager(DiskgraphTest.homeDirFile,readOnly,transactional,false);
		sm.initialize(cachePercent);
		tx = sm.beginTransaction();
		store = sm.openOrderedDatabase(storeName);
		manager = sm;
	}
	
	@Override
	public void close() {
		if (tx!=null) tx.commit();
		store.close();
		manager.close();
	}
	
}
