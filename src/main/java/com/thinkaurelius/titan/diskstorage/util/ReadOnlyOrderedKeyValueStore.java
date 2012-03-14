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
	public Map<ByteBuffer, List<Entry>> getKeySlice(ByteBuffer keyStart,
			ByteBuffer keyEnd, boolean startKeyInc, boolean endKeyInc,
			ByteBuffer columnStart, ByteBuffer columnEnd,
			boolean startColumnIncl, boolean endColumnIncl, int keyLimit,
			int columnLimit, TransactionHandle txh) {
		return store.getKeySlice(keyStart, keyEnd, startKeyInc, endKeyInc, columnStart, columnEnd, startColumnIncl, endColumnIncl, keyLimit, columnLimit, txh);
	}

	@Override
	public Map<ByteBuffer, List<Entry>> getKeySlice(ByteBuffer keyStart,
			ByteBuffer keyEnd, boolean startKeyInc, boolean endKeyInc,
			ByteBuffer columnStart, ByteBuffer columnEnd,
			boolean startColumnIncl, boolean endColumnIncl,
			TransactionHandle txh) {
		return store.getKeySlice(keyStart, keyEnd, startKeyInc, endKeyInc, columnStart, columnEnd, startColumnIncl, endColumnIncl, txh);
	}

	@Override
	public Map<ByteBuffer, List<Entry>> getLimitedKeySlice(ByteBuffer keyStart,
			ByteBuffer keyEnd, boolean startKeyInc, boolean endKeyInc,
			ByteBuffer columnStart, ByteBuffer columnEnd,
			boolean startColumnIncl, boolean endColumnIncl, int keyLimit,
			int columnLimit, TransactionHandle txh) {
		return store.getLimitedKeySlice(keyStart, keyEnd, startKeyInc, endKeyInc, columnStart, columnEnd, startColumnIncl, endColumnIncl, keyLimit, columnLimit, txh);
	}

	@Override
	public List<Entry> getLimitedSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, boolean startInclusive, boolean endInclusive,
			int limit, TransactionHandle txh) {
		return store.getLimitedSlice(key, columnStart, columnEnd, startInclusive, endInclusive, limit, txh);
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, boolean startInclusive, boolean endInclusive,
			int limit, TransactionHandle txh) {
		return store.getSlice(key, columnStart, columnEnd, startInclusive, endInclusive, limit, txh);
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, boolean startInclusive, boolean endInclusive,
			TransactionHandle txh) {
		return store.getSlice(key, columnStart, columnEnd, startInclusive, endInclusive, txh);
	}
	
	

}
