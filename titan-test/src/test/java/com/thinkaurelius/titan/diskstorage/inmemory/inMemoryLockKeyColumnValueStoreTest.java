package com.thinkaurelius.titan.diskstorage.inmemory;

import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class inMemoryLockKeyColumnValueStoreTest extends LockKeyColumnValueStoreTest {

    @Override
    public KeyColumnValueStoreManager openStorageManager(int id) throws StorageException {
        return new InMemoryStoreManager();
    }
}
