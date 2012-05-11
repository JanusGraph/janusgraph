package com.thinkaurelius.titan.diskstorage.test;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager;
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
        CassandraThriftStorageManager cmanager = new CassandraThriftStorageManager(CassandraLocalhostHelper.getLocalStorageConfiguration());
        cmanager.dropKeyspace(keyspace);
        manager = cmanager;
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
