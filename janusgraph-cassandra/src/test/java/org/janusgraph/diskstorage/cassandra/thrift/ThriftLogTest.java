package org.janusgraph.diskstorage.cassandra.thrift;

import org.janusgraph.diskstorage.BackendException;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.log.KCVSLogTest;
import org.janusgraph.testcategory.SerialTests;

@Category(SerialTests.class)
public class ThriftLogTest extends KCVSLogTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        return new CassandraThriftStoreManager(CassandraStorageSetup.getCassandraThriftConfiguration(this.getClass().getSimpleName()));
    }

}
