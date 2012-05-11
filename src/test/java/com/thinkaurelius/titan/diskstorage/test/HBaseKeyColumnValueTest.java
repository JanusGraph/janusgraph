package com.thinkaurelius.titan.diskstorage.test;

import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStorageManager;

public class HBaseKeyColumnValueTest extends KeyColumnValueStoreTest {

	@Override
	public void open() {
		manager = new HBaseStorageManager(DiskgraphTest.getDirectoryStorageConfiguration());
		store = manager.openDatabase("titantest");
		tx = manager.beginTransaction();
	}

	@Override
	public void close() {
		store.close();
		manager.close();
	}
	
	@Override
	public void setUp() throws Exception {
        new HBaseStorageManager(DiskgraphTest.getDirectoryStorageConfiguration()).deleteAll();
		super.setUp();
	}

}
