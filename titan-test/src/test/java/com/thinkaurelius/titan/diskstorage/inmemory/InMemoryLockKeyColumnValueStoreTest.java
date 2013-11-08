package com.thinkaurelius.titan.diskstorage.inmemory;

import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryLockKeyColumnValueStoreTest extends LockKeyColumnValueStoreTest {

    @Override
    public KeyColumnValueStoreManager openStorageManager(int id) throws StorageException {
        return new InMemoryStoreManager();
    }

    @Override
    public void testRemoteLockContention() {
        //Does not apply to non-persisting in-memory store
    }

}
