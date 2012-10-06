package com.thinkaurelius.titan.diskstorage.cassandra;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;


public class InternalCassandraThriftKeyColumnValueTest extends KeyColumnValueStoreTest {

	@BeforeClass
	public static void startCassandra() {
    	CassandraDaemonWrapper.start(StorageSetup.cassandraYamlPath);
	}

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new CassandraThriftStorageManager(getConfiguration());
    }

    private Configuration getConfiguration() {
        Configuration config = StorageSetup.getCassandraStorageConfiguration();
        return config;
    }
}
