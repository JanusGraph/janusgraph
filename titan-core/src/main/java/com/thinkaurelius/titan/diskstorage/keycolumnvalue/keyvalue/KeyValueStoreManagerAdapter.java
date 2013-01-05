package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Mutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

import java.nio.ByteBuffer;
import java.util.Map;

public class KeyValueStoreManagerAdapter implements KeyColumnValueStoreManager {


    private final KeyValueStoreManager manager;

    private final ImmutableMap<String, Integer> keyLengths;

    private final StoreFeatures features;

    public KeyValueStoreManagerAdapter(KeyValueStoreManager manager) {
        this(manager, null);
    }

    public KeyValueStoreManagerAdapter(KeyValueStoreManager manager, Map<String, Integer> keyLengths) {
        this.manager = manager;
        ImmutableMap.Builder<String, Integer> mb = ImmutableMap.builder();
        if (keyLengths != null && !keyLengths.isEmpty()) mb.putAll(keyLengths);
        this.keyLengths = mb.build();
        features = manager.getFeatures().clone();
        features.supportsBatchMutation = false;
    }

    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public StoreTransaction beginTransaction(ConsistencyLevel level) throws StorageException {
        return manager.beginTransaction(level);
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
    public KeyColumnValueStore openDatabase(String name)
            throws StorageException {
        return wrapKeyValueStore(manager.openDatabase(name), keyLengths);
    }

    @Override
    public void mutateMany(Map<String, Map<ByteBuffer, Mutation>> mutations, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    public static final KeyColumnValueStore wrapKeyValueStore(KeyValueStore store, Map<String, Integer> keyLengths) {
        String name = store.getName();
        if (keyLengths.containsKey(name)) {
            int keyLength = keyLengths.get(name);
            Preconditions.checkArgument(keyLength > 0);
            return new KeyValueStoreAdapter(store, keyLength);
        } else {
            return new KeyValueStoreAdapter(store);
        }
    }

    @Override
    public String getConfigurationProperty(final String key) throws StorageException {
        return manager.getConfigurationProperty(key);
    }

    @Override
    public void setConfigurationProperty(final String key, final String value) throws StorageException {
        manager.setConfigurationProperty(key, value);
    }
}
