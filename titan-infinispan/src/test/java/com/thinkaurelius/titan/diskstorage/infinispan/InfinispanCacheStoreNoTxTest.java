package com.thinkaurelius.titan.diskstorage.infinispan;

import com.thinkaurelius.titan.diskstorage.StorageException;

public class InfinispanCacheStoreNoTxTest extends AbstractInfinispanCacheStoreTest {

    public InfinispanCacheStoreNoTxTest() throws StorageException {
        super(false);
    }
}
