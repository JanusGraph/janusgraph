package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.LockType;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.exceptions.GraphStorageException;

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
	public void acquireLock(ByteBuffer key, ByteBuffer column, LockType type,
			TransactionHandle txh) {
		throw new UnsupportedOperationException("Cannot lock on a read-only store!");
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
	public void delete(ByteBuffer key, List<ByteBuffer> columns,
			TransactionHandle txh) {
		throw new UnsupportedOperationException("Cannot delete from a read-only store!");
	}

	@Override
	public void insert(ByteBuffer key, List<Entry> entries,
			TransactionHandle txh) {
		throw new UnsupportedOperationException("Cannot insert into a read-only store!");
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
