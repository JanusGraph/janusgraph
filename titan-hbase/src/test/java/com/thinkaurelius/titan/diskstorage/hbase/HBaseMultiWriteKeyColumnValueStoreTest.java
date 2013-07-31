package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;

import java.io.IOException;

public class HBaseMultiWriteKeyColumnValueStoreTest extends MultiWriteKeyColumnValueStoreTest {
    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new HBaseStoreManager(getConfig());
    }

    private Configuration getConfig() {
        return HBaseStorageSetup.getHBaseStorageConfiguration();
    }
}
