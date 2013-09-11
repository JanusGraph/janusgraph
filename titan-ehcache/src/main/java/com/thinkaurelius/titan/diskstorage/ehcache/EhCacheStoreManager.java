package com.thinkaurelius.titan.diskstorage.ehcache;

import java.util.HashMap;
import java.util.Map;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.common.LocalStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.*;
import com.thinkaurelius.titan.diskstorage.util.FileStorageConfiguration;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.TransactionController;
import net.sf.ehcache.config.DiskStoreConfiguration;
import org.apache.commons.configuration.Configuration;

@SuppressWarnings("unused")
public class EhCacheStoreManager extends LocalStoreManager implements CacheStoreManager {
    private final FileStorageConfiguration storageConfig;

    protected final StoreFeatures features = getDefaultFeatures();
    protected final CacheManager manager;

    private Map<String, EhCacheKeyValueStore> stores = new HashMap<String, EhCacheKeyValueStore>();

    public EhCacheStoreManager(Configuration config) throws StorageException {
        super(config);

        net.sf.ehcache.config.Configuration cacheManagerConfig = new net.sf.ehcache.config.Configuration()
                .diskStore(new DiskStoreConfiguration().path(directory.getAbsolutePath()));

        manager = CacheManager.create(cacheManagerConfig);
        storageConfig = new FileStorageConfiguration(directory);
    }

    @Override
    public CacheStore openDatabase(String name) throws StorageException {
        if (stores.containsKey(name))
            return stores.get(name);

        EhCacheKeyValueStore newStore = new EhCacheKeyValueStore(name, manager);
        stores.put(name, newStore);

        return newStore;
    }

    @Override
    public StoreTransaction beginTransaction(ConsistencyLevel consistencyLevel) throws StorageException {
        return new EhCacheTransaction(manager.getTransactionController(), consistencyLevel);
    }

    @Override
    public void close() throws StorageException {
        manager.shutdown();
    }

    @Override
    public void clearStorage() throws StorageException {
        for (String cache : manager.getCacheNames()) {
            manager.getCache(cache).removeAll();
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

    public class EhCacheTransaction extends AbstractStoreTransaction {
        private final TransactionController controller;

        public EhCacheTransaction(TransactionController controller, ConsistencyLevel consistencyLevel) {
            super(consistencyLevel);

            this.controller = controller;

            // If transaction was already started for this thread, let's just pass it on
            if (transactional && controller.getCurrentTransactionContext() == null)
                controller.begin();
        }

        @Override
        public void commit() {
            if (!transactional || controller.getCurrentTransactionContext() == null)
                return;

            controller.getCurrentTransactionContext().commit(true);
        }

        @Override
        public void rollback() {
            if (!transactional || controller.getCurrentTransactionContext() == null)
                return;

            controller.getCurrentTransactionContext().rollback();
        }
    }
}
