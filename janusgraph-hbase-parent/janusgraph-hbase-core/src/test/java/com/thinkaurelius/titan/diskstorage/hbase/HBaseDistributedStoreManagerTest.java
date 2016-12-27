package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.IOException;

import com.thinkaurelius.titan.diskstorage.BackendException;

import org.apache.hadoop.hbase.util.VersionInfo;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.DistributedStoreManagerTest;


public class HBaseDistributedStoreManagerTest extends DistributedStoreManagerTest<HBaseStoreManager> {

    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    @AfterClass
    public static void stopHBase() {
        // Workaround for https://issues.apache.org/jira/browse/HBASE-10312
        if (VersionInfo.getVersion().startsWith("0.96"))
            HBaseStorageSetup.killIfRunning();
    }

    @Before
    public void setUp() throws BackendException {
        manager = new HBaseStoreManager(HBaseStorageSetup.getHBaseConfiguration());
        store = manager.openDatabase("distributedStoreTest");
    }

    @After
    public void tearDown() throws BackendException {
        store.close();
        manager.close();
    }
}
