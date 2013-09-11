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
public class HazelcastStoreManager extends LocalStoreManager implements KeyColumnValueStoreManager {
    private final FileStorageConfiguration storageConfig;

    protected final HazelcastInstance manager;
    protected final StoreFeatures features = getDefaultFeatures();

    private Map<String, HazelcastKeyColumnValueStore> stores = new HashMap<String, HazelcastKeyColumnValueStore>();

    public HazelcastStoreManager(Configuration config) throws StorageException {
        super(config);
        manager = Hazelcast.newHazelcastInstance();
        storageConfig = new FileStorageConfiguration(directory);
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
    public StoreTransaction beginTransaction(ConsistencyLevel consistencyLevel) throws StorageException {
        TransactionOptions options = TransactionOptions.getDefault().setTransactionType(TransactionOptions.TransactionType.LOCAL);
        return new HazelCastTransaction(manager.newTransactionContext(options), consistencyLevel);
    }

    @Override
    public void close() throws StorageException {
    }

    @Override
    public void clearStorage() throws StorageException {
        for (String storeName : stores.keySet()) {
            manager.getMultiMap(storeName).clear();
        }

        close();
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public String getConfigurationProperty(String key) throws StorageException {
        return storageConfig.getConfigurationProperty(key);
    }

    @Override
    public void setConfigurationProperty(String key, String value) throws StorageException {
        storageConfig.setConfigurationProperty(key, value);
    }

    @Override
    public String getName() {
        return getClass().getSimpleName() + ":" + directory.toString();
    }

    private StoreFeatures getDefaultFeatures() {
        StoreFeatures features = new StoreFeatures();

        features.supportsTransactions = true;
        features.isDistributed = false;

        features.supportsScan = true;
        features.supportsBatchMutation = false;
        features.supportsConsistentKeyOperations = false;
        features.supportsLocking = false;
        features.isKeyOrdered = true;
        features.hasLocalKeyPartition = false;

        return features;
    }

    public static class HazelCastTransaction extends AbstractStoreTransaction {
        private final TransactionContext context;

        public HazelCastTransaction(TransactionContext context, ConsistencyLevel consistencyLevel) {
            super(consistencyLevel);

            this.context = context;
            this.context.beginTransaction();
        }

        @Override
        public void commit() {
            context.commitTransaction();
        }

        @Override
        public void rollback() {
            context.rollbackTransaction();
        }
    }
}
