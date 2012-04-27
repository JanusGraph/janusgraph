package com.thinkaurelius.titan.diskstorage.test;

import com.thinkaurelius.titan.configuration.HBaseStorageConfiguration;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStorageManager;

public class HBaseKeyColumnValueTest extends KeyColumnValueStoreTest {

	@Override
	public void open() {
		manager = new HBaseStorageManager();
		store = manager.openOrderedDatabase("titantest");
		tx = manager.beginTransaction();
	}

	@Override
	public void close() {
		store.close();
		manager.close();
	}
	
	@Override
	public void setUp() throws Exception {
		(new HBaseStorageConfiguration()).deleteAll();
		super.setUp();
	}

}
