package com.thinkaurelius.titan.diskstorage.hazelcast;

import java.util.HashMap;
import java.util.Map;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.common.LocalStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.FileStorageConfiguration;

import org.apache.commons.configuration.Configuration;

@SuppressWarnings("unused")
public class HazelcastKeyColumnValueStoreManager extends AbstractHazelcastStoreManager implements KeyColumnValueStoreManager {
    private final Map<String, HazelcastKeyColumnValueStore> stores = new HashMap<String, HazelcastKeyColumnValueStore>();

    public HazelcastKeyColumnValueStoreManager(Configuration config) throws StorageException {
        super(config);
    }

    @Override
    public KeyColumnValueStore openDatabase(String name) throws StorageException {
        if (stores.containsKey(name))
            return stores.get(name);

        // manager already keeps caches around, thin wrapper we and is easily GC'ed no need to keep it around
        HazelcastKeyColumnValueStore newStore = new HazelcastKeyColumnValueStore(name, manager);
        stores.put(name, newStore);

        return newStore;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        // not supported
    }

    @Override
    public void clearStorage() throws StorageException {
        for (String storeName : stores.keySet()) {
            manager.getMultiMap(storeName).clear();
        }

        close();
    }
}
