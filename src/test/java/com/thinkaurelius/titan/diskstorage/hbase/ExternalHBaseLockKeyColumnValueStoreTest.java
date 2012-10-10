package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockStore;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class ExternalHBaseLockKeyColumnValueStoreTest
	extends LockKeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager(int idx) throws StorageException {
    	Configuration sc = StorageSetup.getHBaseStorageConfiguration();
        return new HBaseStoreManager(sc);
    }
}
