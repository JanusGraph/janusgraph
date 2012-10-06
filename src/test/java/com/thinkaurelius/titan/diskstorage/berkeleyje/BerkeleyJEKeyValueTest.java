package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.berkeleydb.je.BerkeleyJEStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManager;


public class BerkeleyJEKeyValueTest extends KeyValueStoreTest {

    @Override
	public KeyValueStoreManager openStorageManager() throws StorageException {
		return new BerkeleyJEStoreManager(StorageSetup.getBerkeleyJEStorageConfiguration());
	}
	

	
}
