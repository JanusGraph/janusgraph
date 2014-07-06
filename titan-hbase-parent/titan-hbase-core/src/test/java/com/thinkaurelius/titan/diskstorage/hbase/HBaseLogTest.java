package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.IOException;

import com.thinkaurelius.titan.diskstorage.BackendException;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.log.KCVSLogTest;

public class HBaseLogTest extends KCVSLogTest {

    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        return new HBaseStoreManager(HBaseStorageSetup.getHBaseConfiguration());
    }

}
