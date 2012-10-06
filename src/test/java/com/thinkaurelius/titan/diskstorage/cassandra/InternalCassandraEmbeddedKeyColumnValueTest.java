package com.thinkaurelius.titan.diskstorage.cassandra;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;

public class InternalCassandraEmbeddedKeyColumnValueTest extends KeyColumnValueStoreTest {

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new CassandraEmbeddedStorageManager(getConfiguration());
    }

    private Configuration getConfiguration() {
        Configuration config = StorageSetup.getEmbeddedCassandraStorageConfiguration();
        return config;
    }
}
