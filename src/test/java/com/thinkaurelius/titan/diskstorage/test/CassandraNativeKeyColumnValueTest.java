package com.thinkaurelius.titan.diskstorage.test;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;


public class CassandraNativeKeyColumnValueTest extends KeyColumnValueStoreTest {
	
	public static final String keyspace = "titantest00";
	public static final String columnFamily = "test";
	
	private final CassandraNativeLocalhostHelper ch =
		new CassandraNativeLocalhostHelper();
	
	@Before
	public void cassandraSetUp() throws IOException {	
//		manager = ch.start(keyspace, columnFamily);
//		store = manager.openOrderedDatabase(columnFamily);
//		tx = manager.beginTransaction();
	}

	@After
	public void cassandraTearDown() {
		ch.stop();
	}

	@Override
	public void open() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	
}
