package com.thinkaurelius.titan.diskstorage.persistit;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.HashKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStoreManagerAdapter;

public class PersistitHashKeyColumnValueTest extends HashKeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        PersistitStoreManager sm = new PersistitStoreManager();
        return new KeyValueStoreManagerAdapter(sm);
    }

}
