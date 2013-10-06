package com.thinkaurelius.titan.diskstorage.hazelcast;

import java.util.HashMap;
import java.util.Map;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.CacheStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.CacheStoreManager;

import org.apache.commons.configuration.Configuration;

public class HazelcastCacheStoreManager extends AbstractHazelcastStoreManager implements CacheStoreManager {
    private final Map<String, HazelcastCacheStore> stores = new HashMap<String, HazelcastCacheStore>();

    public HazelcastCacheStoreManager(Configuration config) throws StorageException {
        super(config);
    }

    @Override
    public synchronized CacheStore openDatabase(String name) throws StorageException {
        if (stores.containsKey(name))
            return stores.get(name);

        // manager already keeps caches around, thin wrapper we and is easily GC'ed no need to keep it around
        HazelcastCacheStore newStore = new HazelcastCacheStore(name, manager);
        stores.put(name, newStore);

        return newStore;
    }

    @Override
    public void clearStorage() throws StorageException {
        for (HazelcastCacheStore store : stores.values()) {
            store.clearStore();
        }
        close();
    }

    @Override
    public void close() throws StorageException {
        for (HazelcastCacheStore store : stores.values()) {
            store.close();
        }
    }
}
