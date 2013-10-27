package com.thinkaurelius.titan.diskstorage.infinispan;

import com.thinkaurelius.titan.InfinispanStorageSetup;
import com.thinkaurelius.titan.diskstorage.CacheStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InfinispanCacheStoreTest extends CacheStoreTest {

    public InfinispanCacheStoreTest() throws StorageException {
        super(new InfinispanCacheStoreManager(InfinispanStorageSetup.getInfinispanBaseConfig()));
    }

}
