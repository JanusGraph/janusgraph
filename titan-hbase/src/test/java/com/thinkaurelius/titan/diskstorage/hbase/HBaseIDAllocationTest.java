package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.IDAllocationTest;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;

import java.io.IOException;

public class HBaseIDAllocationTest extends IDAllocationTest {

    public HBaseIDAllocationTest(Configuration baseConfig) {
        super(baseConfig);
    }

    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    public KeyColumnValueStoreManager openStorageManager(int idx) throws StorageException {
        return new HBaseStoreManager(HBaseStorageSetup.getHBaseStorageConfiguration());
    }
}
