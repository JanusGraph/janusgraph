package com.thinkaurelius.titan.diskstorage.test;

import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.diskstorage.berkeleydb.je.BerkeleyJEStorageManager;
import com.thinkaurelius.titan.diskstorage.util.KeyValueStorageManagerAdapter;


public class BerkeleyDBjeKeyColumnValueTest extends KeyColumnValueStoreTest {

	

	boolean transactional = false;
	boolean readOnly = false;
	boolean privateAccess = true;
	int cachePercent = 60;
	String storeName = "testStore1";
	
	@Override
	public void open() {
		BerkeleyJEStorageManager sm = new BerkeleyJEStorageManager(DiskgraphTest.homeDirFile,readOnly,transactional,false);
		sm.initialize(cachePercent);
		tx = sm.beginTransaction();
		manager = new KeyValueStorageManagerAdapter(sm);
		store = ((KeyValueStorageManagerAdapter)manager).openDatabase(storeName, 8);
	}
	
	@Override
	public void close() {
		if (tx!=null) tx.commit();
		store.close();
		manager.close();
	}
	

}
