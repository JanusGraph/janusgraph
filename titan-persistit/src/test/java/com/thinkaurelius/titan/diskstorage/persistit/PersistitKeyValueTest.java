package com.thinkaurelius.titan.diskstorage.persistit;

import com.thinkaurelius.titan.diskstorage.KeyValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManager;

public class PersistitKeyValueTest extends KeyValueStoreTest {

    @Override
    public KeyValueStoreManager openStorageManager() throws StorageException {
        return new PersistitStoreManager();
    }
}
