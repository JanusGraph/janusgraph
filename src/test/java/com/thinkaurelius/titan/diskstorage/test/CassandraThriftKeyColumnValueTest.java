package com.thinkaurelius.titan.diskstorage.test;

import org.junit.After;
import org.junit.Before;


public class CassandraThriftKeyColumnValueTest extends KeyColumnValueStoreTest {

	public static final String keyspace = "titantest00";
	public static final String columnFamily = "test";

	public static CassandraLocalhostHelper ch = new CassandraLocalhostHelper("127.0.0.1");
	
	@Override
	public void open() {
	}
	
	@Override
	public void close() {
	}
	
	@Before
	public void cassandraSetUp() {
		ch.startCassandra();
		CassandraStorageConfiguration sc = new CassandraStorageConfiguration();
		sc.setHostname("127.0.0.1");
		sc.getStorageManager(false).dropKeyspace(keyspace);
		sc.setThriftTimeoutMS(10 * 1000);
		manager = sc.getStorageManager(false);
		store = manager.openDatabase(columnFamily);
		tx = manager.beginTransaction();
	}
	
	@After
	public void cassandraTearDown() {
		store.close();
		manager.close();
		ch.stopCassandra();
	}
	

}
