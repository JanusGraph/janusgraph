package com.thinkaurelius.titan.diskstorage.persistit;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.diskstorage.HashKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;

public class PersistitHashKeyColumnValueTest extends HashKeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new OrderedKeyValueStoreManagerAdapter(
                new PersistitStoreManager(PersistitStorageSetup.getPersistitConfig()));
    }

//    @Test
//    @Override
//    public void testGetKeysWithKeyRange() {
//        // Requires ordered keys, but we're using hash prefix
//    }
//
//    @Test
//    @Override
//    public void testOrderedGetKeysRespectsKeyLimit() {
//        // Requires ordered keys, but we are using hash prefix
//    }
}
