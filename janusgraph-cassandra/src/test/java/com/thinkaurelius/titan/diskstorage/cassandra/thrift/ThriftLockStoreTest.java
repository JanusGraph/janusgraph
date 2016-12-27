package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import com.thinkaurelius.titan.diskstorage.BackendException;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;

public class ThriftLockStoreTest extends LockKeyColumnValueStoreTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager(int idx) throws BackendException {
        return new CassandraThriftStoreManager(CassandraStorageSetup.getCassandraThriftConfiguration(this.getClass().getSimpleName()));
    }
}
