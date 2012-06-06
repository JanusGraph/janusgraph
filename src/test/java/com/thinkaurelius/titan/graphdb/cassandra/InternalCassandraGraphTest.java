package com.thinkaurelius.titan.graphdb.cassandra;

import org.junit.BeforeClass;

import com.thinkaurelius.titan.StorageSetup;
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
		CassandraDaemonWrapper.start(StorageSetup.cassandraYamlPath);
	}

	public void cleanUp() {
		CassandraThriftStorageManager.dropKeyspace(
                CassandraThriftStorageManager.KEYSPACE_DEFAULT,
				"127.0.0.1",
                CassandraThriftStorageManager.PORT_DEFAULT);
	}
}
