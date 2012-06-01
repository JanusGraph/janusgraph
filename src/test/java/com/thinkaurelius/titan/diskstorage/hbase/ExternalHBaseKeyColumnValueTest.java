package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.berkeleydb.je.BerkeleyJEStorageManager;
import com.thinkaurelius.titan.diskstorage.util.KeyValueStorageManagerAdapter;
import org.apache.commons.configuration.Configuration;

public class ExternalHBaseKeyColumnValueTest extends KeyColumnValueStoreTest {

    public StorageManager openStorageManager() {
        return new HBaseStorageManager(StorageSetup.getHBaseStorageConfiguration());
    }
	
	@Override
	public void cleanUp() {
        HBaseHelper.deleteAll(StorageSetup.getHBaseStorageConfiguration());
	}

}
