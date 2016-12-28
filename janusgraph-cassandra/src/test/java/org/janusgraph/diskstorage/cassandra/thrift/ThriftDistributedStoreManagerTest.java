package org.janusgraph.diskstorage.cassandra.thrift;

import org.janusgraph.diskstorage.BackendException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.DistributedStoreManagerTest;

public class ThriftDistributedStoreManagerTest extends DistributedStoreManagerTest<CassandraThriftStoreManager> {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Before
    public void setUp() throws BackendException {
        manager = new CassandraThriftStoreManager(
                CassandraStorageSetup.getCassandraThriftConfiguration(this.getClass().getSimpleName()));
        store = manager.openDatabase("distributedcf");
    }

    @After
    public void tearDown() throws BackendException {
        if (null != manager)
            manager.close();
    }
}
