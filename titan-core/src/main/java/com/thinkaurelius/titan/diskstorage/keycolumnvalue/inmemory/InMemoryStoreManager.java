package com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory backend storage engine.
 *
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryStoreManager implements KeyColumnValueStoreManager {

    private final ConcurrentHashMap<String,InMemoryKeyColumnValueStore> stores;

    private final StoreFeatures features;
    private final Map<String,String> storeConfig;

    public InMemoryStoreManager() {
        this(new BaseConfiguration());
    }

    public InMemoryStoreManager(final Configuration configuration) {

        stores = new ConcurrentHashMap<String,InMemoryKeyColumnValueStore>();
        storeConfig = new ConcurrentHashMap<String,String>();

        features = new StoreFeatures();
        features.supportsScan = true;
        features.supportsBatchMutation = false;
        features.supportsTransactions = false;
        features.supportsConsistentKeyOperations = true;
        features.supportsLocking = false;
        features.isDistributed = false;

        features.isKeyOrdered = true;
        features.hasLocalKeyPartition = false;
    }

    @Override
    public StoreTransaction beginTransaction(ConsistencyLevel consistencyLevel) throws StorageException {
        return new TransactionHandle(consistencyLevel);
    }

    @Override
    public void close() throws StorageException {
        for (InMemoryKeyColumnValueStore store : stores.values()) {
            store.close();
        }
        stores.clear();
    }

    @Override
    public void clearStorage() throws StorageException {
        close();
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public String getConfigurationProperty(String key) throws StorageException {
        return storeConfig.get(key);
    }

    @Override
    public void setConfigurationProperty(String key, String value) throws StorageException {
        storeConfig.put(key,value);
    }

    @Override
    public KeyColumnValueStore openDatabase(final String name) throws StorageException {
        if (!stores.contains(name)) {
            stores.putIfAbsent(name,new InMemoryKeyColumnValueStore(name));
        }
        KeyColumnValueStore store = stores.get(name);
        Preconditions.checkNotNull(store);
        return store;
    }

    @Override
    public void mutateMany(Map<String, Map<ByteBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        for (Map.Entry<String,Map<ByteBuffer, KCVMutation>> storeMut : mutations.entrySet()) {
            KeyColumnValueStore store = stores.get(storeMut.getKey());
            Preconditions.checkNotNull(store);
            for (Map.Entry<ByteBuffer,KCVMutation> keyMut : storeMut.getValue().entrySet()) {
                store.mutate(keyMut.getKey(),keyMut.getValue().getAdditions(),keyMut.getValue().getDeletions(),txh);
            }
        }
    }


    private class TransactionHandle extends AbstractStoreTransaction {

        public TransactionHandle(ConsistencyLevel level) {
            super(level);
        }
    }
}
