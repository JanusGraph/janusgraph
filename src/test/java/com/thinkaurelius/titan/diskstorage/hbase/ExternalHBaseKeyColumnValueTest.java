package com.thinkaurelius.titan.diskstorage.hbase;

import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;

public class ExternalHBaseKeyColumnValueTest extends KeyColumnValueStoreTest {

    public StorageManager openStorageManager() {
        return new HBaseStorageManager(getConfig());
    }
	
	@Override
	public void cleanUp() {
        HBaseHelper.deleteAll(getConfig());
	}
	
	private Configuration getConfig() {
		Configuration c = StorageSetup.getHBaseStorageConfiguration();
//		c.setProperty("hbconf.hbase.zookeeper.quorum", "localhost");
//		c.setProperty("hbconf.hbase.zookeeper.property.clientPort", "2181");
		return c;
	}

}
