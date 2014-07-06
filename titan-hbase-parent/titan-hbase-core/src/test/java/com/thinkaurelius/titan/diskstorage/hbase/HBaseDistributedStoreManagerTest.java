package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.IOException;

import com.thinkaurelius.titan.diskstorage.BackendException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.DistributedStoreManagerTest;

public class HBaseDistributedStoreManagerTest extends DistributedStoreManagerTest<HBaseStoreManager> {
    
    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }
    
    @Before
    public void setUp() throws BackendException {
        manager = new HBaseStoreManager(HBaseStorageSetup.getHBaseConfiguration());
        store = manager.openDatabase("distributedStoreTest");
    }
    
    @After
    public void tearDown() {
        if (null != manager)
            manager.close();
    }
}
