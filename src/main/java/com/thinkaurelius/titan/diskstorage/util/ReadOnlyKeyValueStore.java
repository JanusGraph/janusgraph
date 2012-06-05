package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.core.GraphStorageException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.nio.ByteBuffer;
import java.util.List;

public class ReadOnlyKeyValueStore implements KeyColumnValueStore {
	
	protected final KeyColumnValueStore store;
	
	public ReadOnlyKeyValueStore(KeyColumnValueStore store) {
		this.store=store;
	}

	@Override
	public void close() throws GraphStorageException {
		store.close();
	}

	@Override
	public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue,
			TransactionHandle txh) {
		throw new UnsupportedOperationException("Cannot lock on a read-only store");
	}

	@Override
	public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column,
			TransactionHandle txh) {
		return store.containsKeyColumn(key, column, txh);
	}

	@Override
	public ByteBuffer get(ByteBuffer key, ByteBuffer column,
			TransactionHandle txh) {
		return store.get(key, column, txh);
	}


    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, TransactionHandle txh) {
		throw new UnsupportedOperationException("Cannot mutate a read-only store");
	}


//	@Override
	public boolean isReadOnly() {
		return true;
	}

	@Override
	public boolean isLocalKey(ByteBuffer key) {
		return store.isLocalKey(key);
	}
}
