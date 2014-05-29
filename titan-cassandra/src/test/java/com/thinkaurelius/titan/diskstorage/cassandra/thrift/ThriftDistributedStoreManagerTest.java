package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.DistributedStoreManagerTest;
import com.thinkaurelius.titan.diskstorage.StorageException;

public class ThriftDistributedStoreManagerTest extends DistributedStoreManagerTest<CassandraThriftStoreManager> {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    @Before
    public void setUp() throws StorageException {
        manager = new CassandraThriftStoreManager(
                CassandraStorageSetup.getCassandraThriftConfiguration(this.getClass().getSimpleName()));
        store = manager.openDatabase("distributedcf");
    }

    @After
    public void tearDown() throws StorageException {
        if (null != manager)
            manager.close();
    }
}
