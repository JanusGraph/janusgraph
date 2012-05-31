package com.thinkaurelius.titan.graphdb.cassandra;

import org.junit.BeforeClass;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraDaemonWrapper;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraLocalhostHelper;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class InternalCassandraGraphTest extends TitanGraphTest {

	public InternalCassandraGraphTest() {
		super((new CassandraLocalhostHelper()).getConfiguration());
	}

	@BeforeClass
	public static void beforeClass() {
		CassandraDaemonWrapper.start();
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
