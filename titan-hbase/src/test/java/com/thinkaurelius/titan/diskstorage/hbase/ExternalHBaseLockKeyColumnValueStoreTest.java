package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.apache.commons.configuration.Configuration;

public class ExternalHBaseLockKeyColumnValueStoreTest
        extends LockKeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager(int idx) throws StorageException {
        Configuration sc = HBaseStorageSetup.getHBaseStorageConfiguration();
        return new HBaseStoreManager(sc);
    }
}
