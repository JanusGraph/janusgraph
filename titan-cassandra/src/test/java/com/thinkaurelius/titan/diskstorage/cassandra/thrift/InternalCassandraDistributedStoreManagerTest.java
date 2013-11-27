package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.DistributedStoreManagerTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;

public class InternalCassandraDistributedStoreManagerTest extends DistributedStoreManagerTest {
    
    private CassandraThriftStoreManager sm;
    
    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }
    
    @Before
    public void setUp() throws StorageException {
        sm = new CassandraThriftStoreManager(
                CassandraStorageSetup.getGenericCassandraStorageConfiguration(this.getClass().getSimpleName()));
        manager = sm;
        store = sm.openDatabase("distributedcf");
    }
    
    @After
    public void tearDown() throws StorageException {
        if (null != sm)
            sm.close();
    }
}
