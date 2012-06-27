package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyValueStoreTest;
import com.thinkaurelius.titan.diskstorage.berkeleydb.je.BerkeleyJEStorageManager;
import com.thinkaurelius.titan.diskstorage.util.KeyValueStorageManager;


public class BerkeleyJEKeyValueTest extends KeyValueStoreTest {

    @Override
	public KeyValueStorageManager openStorageManager() {
		return new BerkeleyJEStorageManager(StorageSetup.getBerkeleyJEStorageConfiguration());
	}
	

	
}
