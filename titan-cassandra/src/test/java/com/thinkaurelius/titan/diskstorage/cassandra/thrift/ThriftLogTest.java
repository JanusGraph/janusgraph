package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.log.KCVSLogTest;
import com.thinkaurelius.titan.testcategory.SerialTests;

@Category(SerialTests.class)
public class ThriftLogTest extends KCVSLogTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new CassandraThriftStoreManager(CassandraStorageSetup.getCassandraThriftConfiguration(this.getClass().getSimpleName()));
    }

}
