package com.thinkaurelius.titan.diskstorage.cassandra;

import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;


public class InternalCassandraThriftKeyColumnValueTest extends KeyColumnValueStoreTest {

	@BeforeClass
	public static void startCassandra() {
    	CassandraDaemonWrapper.start(StorageSetup.cassandraYamlPath);
	}

    @Override
    public StorageManager openStorageManager() {
        return new CassandraThriftStorageManager(getConfiguration());
    }

    private Configuration getConfiguration() {
        Configuration config = StorageSetup.getCassandraStorageConfiguration();
        return config;
    }
}
