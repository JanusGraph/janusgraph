package com.thinkaurelius.titan.diskstorage.test;

import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.diskstorage.berkeleydb.je.BerkeleyJEStorageManager;
import com.thinkaurelius.titan.diskstorage.util.KeyValueStorageManagerAdapter;


public class BerkeleyDBjeKeyColumnValueVariableTest extends BerkeleyDBjeKeyColumnValueTest {

	
	@Override
	public void open() {
		BerkeleyJEStorageManager sm = new BerkeleyJEStorageManager(DiskgraphTest.getDirectoryStorageConfiguration());
		tx = sm.beginTransaction();
		manager = new KeyValueStorageManagerAdapter(sm);
		store = manager.openDatabase(storeName);
		
	}

}
