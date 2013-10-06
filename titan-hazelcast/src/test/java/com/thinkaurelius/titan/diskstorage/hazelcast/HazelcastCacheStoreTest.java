package com.thinkaurelius.titan.diskstorage.hazelcast;

import com.thinkaurelius.titan.HazelcastStorageSetup;
import com.thinkaurelius.titan.diskstorage.*;

public class HazelcastCacheStoreTest extends CacheStoreTest {
    public HazelcastCacheStoreTest() throws StorageException {
        super(new HazelcastCacheStoreManager(HazelcastStorageSetup.getHazelcastBaseConfig()));
    }
}
