package com.thinkaurelius.titan.diskstorage.infinispan;

import com.thinkaurelius.titan.diskstorage.StorageException;

public class InfinispanCacheStoreTxTest extends AbstractInfinispanCacheStoreTest {

    public InfinispanCacheStoreTxTest() throws StorageException {
        super(true);
    }
}
