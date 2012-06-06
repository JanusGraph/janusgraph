package com.thinkaurelius.titan.diskstorage.cassandra;

import org.junit.After;

import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;

public class ExternalCassandraThriftKeyColumnValueTest extends KeyColumnValueStoreTest {


	public static CassandraLocalhostHelper ch = new CassandraLocalhostHelper("127.0.0.1");

    @Override
    public void cleanUp() {
        ch.startCassandra();
        CassandraThriftStorageManager cmanager = new CassandraThriftStorageManager(CassandraLocalhostHelper.getLocalStorageConfiguration());
        cmanager.dropKeyspace(CassandraThriftStorageManager.KEYSPACE_DEFAULT);
    }

    @Override
    public StorageManager openStorageManager() {
        return new CassandraThriftStorageManager(CassandraLocalhostHelper.getLocalStorageConfiguration());
    }

	@After
	public void cassandraTearDown() {
		ch.stopCassandra();
	}
	
}
