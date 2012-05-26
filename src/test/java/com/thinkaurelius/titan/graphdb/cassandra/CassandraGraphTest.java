package com.thinkaurelius.titan.graphdb.cassandra;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraLocalhostHelper;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class CassandraGraphTest extends TitanGraphTest {

	public static CassandraLocalhostHelper ch = new CassandraLocalhostHelper();
	
	public CassandraGraphTest() {
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

	public void cleanUp() {
		CassandraThriftStorageManager.dropKeyspace(
                CassandraThriftStorageManager.DEFAULT_KEYSPACE,
				"127.0.0.1",
                CassandraThriftStorageManager.DEFAULT_PORT);
		CassandraThriftStorageManager.dropKeyspace(
                CassandraThriftStorageManager.ID_KEYSPACE,
				"127.0.0.1",
                CassandraThriftStorageManager.DEFAULT_PORT);
	}
}
