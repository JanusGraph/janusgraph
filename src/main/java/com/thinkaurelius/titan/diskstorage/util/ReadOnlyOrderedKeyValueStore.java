package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class ReadOnlyOrderedKeyValueStore extends ReadOnlyKeyValueStore implements OrderedKeyColumnValueStore{

	protected final OrderedKeyColumnValueStore store;
	
	public ReadOnlyOrderedKeyValueStore(OrderedKeyColumnValueStore store) {
		super(store);
		this.store = store;
	}
	
	@Override
	public boolean containsKey(ByteBuffer key, TransactionHandle txh) {
		return store.containsKey(key, txh);
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, int limit, TransactionHandle txh) {
		return store.getSlice(key, columnStart, columnEnd, limit, txh);
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, TransactionHandle txh) {
		return store.getSlice(key, columnStart, columnEnd, txh);
	}
	
	

}
