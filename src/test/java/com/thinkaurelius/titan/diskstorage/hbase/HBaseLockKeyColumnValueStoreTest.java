package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;

public class HBaseLockKeyColumnValueStoreTest 
	extends LockKeyColumnValueStoreTest {
	
	@Override
	public void cleanUp() {
        HBaseHelper.deleteAll(StorageSetup.getHBaseStorageConfiguration());
	}
	
    public StorageManager openStorageManager() {
        return new HBaseStorageManager(StorageSetup.getHBaseStorageConfiguration());
    }
}
