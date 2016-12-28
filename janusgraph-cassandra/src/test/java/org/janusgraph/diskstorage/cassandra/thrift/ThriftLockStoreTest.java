package org.janusgraph.diskstorage.cassandra.thrift;

import org.janusgraph.diskstorage.BackendException;
import org.junit.BeforeClass;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.LockKeyColumnValueStoreTest;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;

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
