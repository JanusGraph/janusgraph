package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.IDAuthorityTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;

import org.junit.BeforeClass;

import java.io.IOException;

public class HBaseIDAuthorityTest extends IDAuthorityTest {

    public HBaseIDAuthorityTest(WriteConfiguration baseConfig) {
        super(baseConfig);
    }

    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new HBaseStoreManager(HBaseStorageSetup.getHBaseConfiguration());
    }
}
