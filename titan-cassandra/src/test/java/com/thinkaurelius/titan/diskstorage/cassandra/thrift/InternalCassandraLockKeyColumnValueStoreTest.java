package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;

public class InternalCassandraLockKeyColumnValueStoreTest extends LockKeyColumnValueStoreTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.cassandraYamlPath);
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager(int idx) throws StorageException {
        Configuration sc = CassandraStorageSetup.getCassandraStorageConfiguration();
        return new CassandraThriftStoreManager(sc);
    }
}
