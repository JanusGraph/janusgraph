package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CacheStoreManagerAdapter implements KeyColumnValueStoreManager {

    private final CacheStoreManager manager;

    private final Map<String, CacheStoreAdapter> stores;

    public CacheStoreManagerAdapter(CacheStoreManager manager) {
        this.manager = manager;
        this.stores = new HashMap<String, CacheStoreAdapter>();
    }

    @Override
    public StoreFeatures getFeatures() {
        return manager.getFeatures();
    }

    @Override
    public StoreTransaction beginTransaction(StoreTxConfig config) throws StorageException {
        return manager.beginTransaction(config);
    }

    @Override
    public void close() throws StorageException {
        manager.close();
    }

    @Override
    public void clearStorage() throws StorageException {
        manager.clearStorage();
    }

    @Override
    public synchronized CacheStoreAdapter openDatabase(String name)
            throws StorageException {
        if (!stores.containsKey(name)) {
            CacheStoreAdapter store = wrapKeyValueStore(manager.openDatabase(name));
            stores.put(name, store);
        }
        return stores.get(name);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        Map<String, KVMutation> converted = new HashMap<String, KVMutation>(mutations.size());
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> storeEntry : mutations.entrySet()) {
            CacheStoreAdapter store = openDatabase(storeEntry.getKey());
            Preconditions.checkNotNull(store);

            for (Map.Entry<StaticBuffer, KCVMutation> entry : storeEntry.getValue().entrySet()) {
                StaticBuffer key = entry.getKey();
                KCVMutation mutation = entry.getValue();
                store.mutate(key, mutation.getAdditions(), mutation.getDeletions(), txh);
            }
        }
    }

    private static final CacheStoreAdapter wrapKeyValueStore(CacheStore store) {
        return new CacheStoreAdapter(store);
    }

    @Override
    public String getConfigurationProperty(final String key) throws StorageException {
        return manager.getConfigurationProperty(key);
    }

    @Override
    public void setConfigurationProperty(final String key, final String value) throws StorageException {
        manager.setConfigurationProperty(key, value);
    }

    @Override
    public String getName() {
        return manager.getName();
    }

}
