package com.thinkaurelius.titan.diskstorage.infinispan;

import com.thinkaurelius.titan.diskstorage.StorageException;

public class InfinispanCacheAdapterNoTxTest extends AbstractInfinispanCacheAdapterTest {

    public InfinispanCacheAdapterNoTxTest() throws StorageException {
        super(false);
    }
}
