package com.thinkaurelius.titan.diskstorage.hbase;

import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;

public class ExternalHBaseKeyColumnValueTest extends KeyColumnValueStoreTest {

    public StorageManager openStorageManager() {
        return new HBaseStorageManager(getConfig());
    }
	
	private Configuration getConfig() {
		Configuration c = StorageSetup.getHBaseStorageConfiguration();
//		c.setProperty("hbase-config.hbase.zookeeper.quorum", "localhost");
//		c.setProperty("hbase-config.hbase.zookeeper.property.clientPort", "2181");
		return c;
	}

}
