package com.thinkaurelius.titan.diskstorage.test;

import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.diskstorage.berkeleydb.je.BerkeleyDBStorageManager;
import com.thinkaurelius.titan.diskstorage.util.KeyValueStorageManagerAdapter;


public class BerkeleyDBjeKeyColumnValueVariableTest extends BerkeleyDBjeKeyColumnValueTest {

	
	@Override
	public void open() {
		BerkeleyDBStorageManager sm = new BerkeleyDBStorageManager(DiskgraphTest.homeDirFile,readOnly,transactional,false);
		sm.initialize(cachePercent);
		tx = sm.beginTransaction();
		manager = new KeyValueStorageManagerAdapter(sm);
		store = manager.openOrderedDatabase(storeName);
		
	}

}
