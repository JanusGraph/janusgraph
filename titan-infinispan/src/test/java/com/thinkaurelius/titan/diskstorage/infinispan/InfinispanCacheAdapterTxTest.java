package com.thinkaurelius.titan.diskstorage.infinispan;

import com.thinkaurelius.titan.diskstorage.StorageException;

public class InfinispanCacheAdapterTxTest extends AbstractInfinispanCacheAdapterTest {

    public InfinispanCacheAdapterTxTest() throws StorageException {
        super(true);
    }
}
