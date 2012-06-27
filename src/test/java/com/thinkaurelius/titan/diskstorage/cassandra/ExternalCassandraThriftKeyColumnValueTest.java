package com.thinkaurelius.titan.diskstorage.cassandra;

import com.thinkaurelius.titan.StorageSetup;

import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ExternalCassandraThriftKeyColumnValueTest extends KeyColumnValueStoreTest {


	public static CassandraProcessStarter ch = new CassandraProcessStarter();

    @Override
    public StorageManager openStorageManager() {
        return new CassandraThriftStorageManager(StorageSetup.getCassandraStorageConfiguration());
    }


    @BeforeClass
    public static void startCassandra() {
        ch.startCassandra();
    }

    @AfterClass
    public static void stopCassandra() {
        ch.stopCassandra();
    }
	
}
