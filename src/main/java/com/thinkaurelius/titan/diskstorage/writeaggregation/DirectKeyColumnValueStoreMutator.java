package com.thinkaurelius.titan.diskstorage.writeaggregation;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.nio.ByteBuffer;
import java.util.List;

public class DirectKeyColumnValueStoreMutator implements KeyColumnValueStoreMutator {

	private TransactionHandle txh;
	private final KeyColumnValueStore store;
	
	public DirectKeyColumnValueStoreMutator(TransactionHandle txh, KeyColumnValueStore store) {
		this.txh = txh;
		this.store = store;
	}

	@Override
	public void insert(ByteBuffer key, List<Entry> entries) {
		store.insert(key, entries, txh);
	}

	@Override
	public void delete(ByteBuffer key, List<ByteBuffer> columns) {
		store.delete(key, columns, txh);
	}

	@Override
	public void flushInserts() {
		// Do nothing
	}

	@Override
	public void flushDeletes() {
		// Do nothing
	}

	@Override
	public void flush() {
		// Do nothing
	}
}
