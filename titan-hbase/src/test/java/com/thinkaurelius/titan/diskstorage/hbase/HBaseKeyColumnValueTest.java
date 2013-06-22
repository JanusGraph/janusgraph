package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class HBaseKeyColumnValueTest extends KeyColumnValueStoreTest {
    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new HBaseStoreManager(getConfig());
    }

    private Configuration getConfig() {
        Configuration c = HBaseStorageSetup.getHBaseStorageConfiguration();
        c.setProperty("hbase-config.hbase.zookeeper.quorum", "localhost");
        c.setProperty("hbase-config.hbase.zookeeper.property.clientPort", "2181");
        return c;
    }

    @Test
    public void testGetKeysWithKeyRange() throws Exception {
        super.testGetKeysWithKeyRange();
    }
}
