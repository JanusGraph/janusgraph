package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class HBaseStoreTest extends KeyColumnValueStoreTest {
    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        WriteConfiguration config = HBaseStorageSetup.getHBaseGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_NS.getName()+"."+HBaseStoreManager.HBASE_CONFIGURATION_NAMESPACE+
                    ".hbase.zookeeper.quorum","localhost");
        config.set(GraphDatabaseConfiguration.STORAGE_NS.getName()+"."+HBaseStoreManager.HBASE_CONFIGURATION_NAMESPACE+
                "hbase.zookeeper.property.clientPort",2181);
        return new HBaseStoreManager(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,config, BasicConfiguration.Restriction.NONE));
    }

    @Test
    public void testGetKeysWithKeyRange() throws Exception {
        super.testGetKeysWithKeyRange();
    }
}
