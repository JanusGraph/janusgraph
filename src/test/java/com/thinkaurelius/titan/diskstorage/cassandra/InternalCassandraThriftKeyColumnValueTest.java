package com.thinkaurelius.titan.diskstorage.cassandra;

import org.junit.BeforeClass;

import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;


public class InternalCassandraThriftKeyColumnValueTest extends KeyColumnValueStoreTest {

	@BeforeClass
	public static void startCassandra() {
    	CassandraDaemonWrapper.start();
	}
	
    @Override
    public void cleanUp() {
        CassandraThriftStorageManager cmanager =
        		new CassandraThriftStorageManager(CassandraLocalhostHelper.getLocalStorageConfiguration());
        cmanager.dropKeyspace(CassandraThriftStorageManager.DEFAULT_KEYSPACE);
        cmanager.dropKeyspace(CassandraThriftStorageManager.ID_KEYSPACE);
    }

    @Override
    public StorageManager openStorageManager() {
        return new CassandraThriftStorageManager(CassandraLocalhostHelper.getLocalStorageConfiguration());
    }
    
}
