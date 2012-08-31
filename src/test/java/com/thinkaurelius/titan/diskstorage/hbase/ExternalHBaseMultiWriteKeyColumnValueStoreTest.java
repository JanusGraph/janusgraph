package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.diskstorage.StorageException;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;

public class ExternalHBaseMultiWriteKeyColumnValueStoreTest extends MultiWriteKeyColumnValueStoreTest {

    public StorageManager openStorageManager() throws StorageException {
        return new HBaseStorageManager(getConfig());
    }

	private Configuration getConfig() {
		Configuration c = StorageSetup.getHBaseStorageConfiguration();
		return c;
	}
}
