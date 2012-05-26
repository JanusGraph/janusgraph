package com.thinkaurelius.titan.diskstorage.cassandra;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import org.junit.After;
import org.junit.Before;

public class CassandraLockKeyColumnValueStoreTest 
	extends LockKeyColumnValueStoreTest {

    public static CassandraLocalhostHelper ch = new CassandraLocalhostHelper("127.0.0.1");

    @Override
    public void cleanUp() {
        StorageSetup.deleteHomeDir();
        ch.startCassandra();
        CassandraThriftStorageManager cmanager = new CassandraThriftStorageManager(CassandraLocalhostHelper.getLocalStorageConfiguration());
        cmanager.dropKeyspace(CassandraThriftStorageManager.DEFAULT_KEYSPACE);
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
