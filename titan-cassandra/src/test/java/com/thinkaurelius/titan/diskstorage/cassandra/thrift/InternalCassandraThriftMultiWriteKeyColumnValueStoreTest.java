package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraDaemonWrapper;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.junit.BeforeClass;

public class InternalCassandraThriftMultiWriteKeyColumnValueStoreTest extends MultiWriteKeyColumnValueStoreTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraDaemonWrapper.start(StorageSetup.cassandraYamlPath);
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new CassandraThriftStoreManager(StorageSetup.getCassandraStorageConfiguration());
    }
}
