package com.thinkaurelius.titan.diskstorage.infinispan;

import com.thinkaurelius.titan.InfinispanStorageSetup;
import com.thinkaurelius.titan.diskstorage.CacheStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractInfinispanCacheStoreTest extends CacheStoreTest {

    public AbstractInfinispanCacheStoreTest(boolean transactional) throws StorageException {
        super(new InfinispanCacheStoreManager(InfinispanStorageSetup.getInfinispanBaseConfig(transactional)));
    }

}
