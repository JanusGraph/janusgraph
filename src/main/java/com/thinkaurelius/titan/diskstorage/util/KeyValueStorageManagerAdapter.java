package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.exceptions.GraphStorageException;

import java.util.HashMap;
import java.util.Map;

public class KeyValueStorageManagerAdapter implements StorageManager {

	private final KeyValueStorageManager manager;
	
	private final Map<String,Integer> keyLengths;
	
	public KeyValueStorageManagerAdapter(KeyValueStorageManager manager) {
		this.manager = manager;
		keyLengths = new HashMap<String,Integer>();
	}
		
	@Override
	public TransactionHandle beginTransaction() {
		return manager.beginTransaction();
	}

	@Override
	public void close() {
		manager.close();
	}

	private void verifyKeyLength(String name, int length) {
		if (keyLengths.containsKey(name)) {
			if (keyLengths.get(name)!=length) {
				throw new IllegalArgumentException("Key Length does not match original value!");
			}
		} else keyLengths.put(name, Integer.valueOf(length));
	}

	public OrderedKeyColumnValueStore openDatabase(String name, int keyLength)
			throws GraphStorageException {
		verifyKeyLength(name,keyLength);
		return new OrderedKeyValueStoreAdapter(manager.openDatabase(name),keyLength);
	}

	@Override
	public OrderedKeyColumnValueStore openDatabase(String name)
			throws GraphStorageException {
		return openDatabase(name, KeyValueStoreAdapter.variableKeyLength);
	}

    @Override
    public long[] getIDBlock(int partition, int blockSize) {
        return manager.getIDBlock(partition,blockSize);
    }


}
