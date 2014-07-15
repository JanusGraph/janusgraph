package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.IOException;

import com.thinkaurelius.titan.diskstorage.BackendException;

import org.apache.hadoop.hbase.util.VersionInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.log.KCVSLogTest;

public class HBaseLogTest extends KCVSLogTest {

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

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        return new HBaseStoreManager(HBaseStorageSetup.getHBaseConfiguration());
    }

}
