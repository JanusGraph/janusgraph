package com.thinkaurelius.titan.diskstorage.cassandra;

import org.junit.BeforeClass;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;

public class InternalCassandraThriftMultiWriteKeyColumnValueStoreTest extends MultiWriteKeyColumnValueStoreTest {
	
	@BeforeClass
	public static void startCassandra() {
    	CassandraDaemonWrapper.start(StorageSetup.cassandraYamlPath);
	}

    @Override
    public StorageManager openStorageManager() {
        return new CassandraThriftStorageManager(StorageSetup.getCassandraStorageConfiguration());
    }
}
