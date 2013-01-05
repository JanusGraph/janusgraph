package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.apache.commons.configuration.Configuration;

public class ExternalHBaseMultiWriteKeyColumnValueStoreTest extends MultiWriteKeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new HBaseStoreManager(getConfig());
    }

    private Configuration getConfig() {
        Configuration c = HBaseStorageSetup.getHBaseStorageConfiguration();
        return c;
    }
}
