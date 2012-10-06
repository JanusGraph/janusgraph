package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;

public class ExternalHBaseMultiWriteKeyColumnValueStoreTest extends MultiWriteKeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new HBaseStorageManager(getConfig());
    }

	private Configuration getConfig() {
		Configuration c = StorageSetup.getHBaseStorageConfiguration();
		return c;
	}
}
