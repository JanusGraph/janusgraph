package com.thinkaurelius.titan.graphdb.test;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager;
import com.thinkaurelius.titan.diskstorage.test.CassandraLocalhostHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class CassandraConcurrentGraphDBTest 
	extends AbstractConcurrentGraphDBTest {

	public static CassandraLocalhostHelper ch = new CassandraLocalhostHelper();
	
	public CassandraConcurrentGraphDBTest() {
		super(ch.getConfiguration());
	}

	@BeforeClass
	public static void beforeClass() {
		ch.startCassandra();
	}
	
	@AfterClass
	public static void afterClass() throws InterruptedException {
		ch.stopCassandra();
	}
	
	@Before
	public void setUp() throws Exception {
		CassandraThriftStorageManager.dropKeyspace(
                CassandraThriftStorageManager.DEFAULT_KEYSPACE,
				"127.0.0.1",
                CassandraThriftStorageManager.DEFAULT_PORT);
		super.setUp();
	}
	
	@Override
	public void open() {
		graphdb = ch.openDatabase();
		tx=graphdb.startTransaction();
	}
}
