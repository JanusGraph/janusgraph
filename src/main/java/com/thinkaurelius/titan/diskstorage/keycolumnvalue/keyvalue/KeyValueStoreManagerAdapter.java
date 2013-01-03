package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;

import java.nio.ByteBuffer;
import java.util.Map;

public class KeyValueStoreManagerAdapter implements KeyColumnValueStoreManager {


	private final KeyValueStoreManager manager;
	
	private final ImmutableMap<String,Integer> keyLengths;

    private final StoreFeatures features;

    public KeyValueStoreManagerAdapter(KeyValueStoreManager manager) {
        this(manager,null);
    }

	public KeyValueStoreManagerAdapter(KeyValueStoreManager manager, Map<String,Integer> keyLengths) {
		this.manager = manager;
        ImmutableMap.Builder<String,Integer> mb = ImmutableMap.builder();
        if (keyLengths!=null && !keyLengths.isEmpty()) mb.putAll(keyLengths);
        this.keyLengths=mb.build();
        features = manager.getFeatures().clone();
        features.supportsBatchMutation=false;
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
        return wrapKeyValueStore(manager.openDatabase(name),keyLengths);
	}

    @Override
    public void mutateMany(Map<String, Map<ByteBuffer, Mutation>> mutations, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }
    
    public static final KeyColumnValueStore wrapKeyValueStore(KeyValueStore store, Map<String,Integer> keyLengths) {
        String name = store.getName();
        if (keyLengths.containsKey(name)) {
            int keyLength = keyLengths.get(name);
            Preconditions.checkArgument(keyLength>0);
            return new KeyValueStoreAdapter(store,keyLength);
        } else {
            return new KeyValueStoreAdapter(store);
        }
    }

    @Override
    public String getLastSeenTitanVersion() throws StorageException {
        return manager.getLastSeenTitanVersion();
    }

    @Override
    public void setTitanVersionToLatest() throws StorageException {
        manager.setTitanVersionToLatest();
    }
}
