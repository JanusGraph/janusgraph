package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import org.apache.commons.configuration.Configuration;
import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;

public class InternalCassandraEmbeddedMultiWriteKeyColumnValueStoreTest extends MultiWriteKeyColumnValueStoreTest {

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        Configuration config = CassandraStorageSetup.getEmbeddedCassandraStorageConfiguration(getClass().getSimpleName());
        return new CassandraEmbeddedStoreManager(config);
    }
}
