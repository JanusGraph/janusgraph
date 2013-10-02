package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;

import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

public class InternalCassandraThriftMultiWriteKeyColumnValueStoreTest extends MultiWriteKeyColumnValueStoreTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        Configuration config = CassandraStorageSetup.getGenericCassandraStorageConfiguration(getClass().getSimpleName());
        return new CassandraThriftStoreManager(config);
    }
}
