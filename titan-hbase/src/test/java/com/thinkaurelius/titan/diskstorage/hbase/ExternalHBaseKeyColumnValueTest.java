package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.apache.commons.configuration.Configuration;

public class ExternalHBaseKeyColumnValueTest extends KeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new HBaseStoreManager(getConfig());
    }

    private Configuration getConfig() {
        Configuration c = HBaseStorageSetup.getHBaseStorageConfiguration();
//		c.setProperty("hbase-config.hbase.zookeeper.quorum", "localhost");
//		c.setProperty("hbase-config.hbase.zookeeper.property.clientPort", "2181");
        return c;
    }

}
