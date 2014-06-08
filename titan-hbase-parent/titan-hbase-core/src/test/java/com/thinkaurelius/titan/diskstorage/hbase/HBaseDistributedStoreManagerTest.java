package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.DistributedStoreManagerTest;
import com.thinkaurelius.titan.diskstorage.StorageException;

public class HBaseDistributedStoreManagerTest extends DistributedStoreManagerTest<HBaseStoreManager> {
    
    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }
    
    @Before
    public void setUp() throws StorageException {
        manager = new HBaseStoreManager(HBaseStorageSetup.getHBaseConfiguration());
        store = manager.openDatabase("distributedStoreTest");
    }
    
    @After
    public void tearDown() {
        if (null != manager)
            manager.close();
    }
}
