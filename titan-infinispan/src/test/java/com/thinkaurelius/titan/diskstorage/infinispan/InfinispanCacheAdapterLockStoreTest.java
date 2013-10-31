package com.thinkaurelius.titan.diskstorage.infinispan;

import com.thinkaurelius.titan.InfinispanStorageSetup;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.CacheStoreManagerAdapter;

public class InfinispanCacheAdapterLockStoreTest extends LockKeyColumnValueStoreTest {

    @Override
    public KeyColumnValueStoreManager openStorageManager(int id) throws StorageException {
        return new CacheStoreManagerAdapter(new InfinispanCacheStoreManager(
                InfinispanStorageSetup.getInfinispanBaseConfig()));
    }
    
    @Override
    public void testRemoteLockContention() {
        //Does not apply to non-persisting in-memory store
    }
    
    //TODO: should this test work in-memory?
    @Override
    public void testMultiIDAcquisition() {}
}
