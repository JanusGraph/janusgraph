package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.nio.ByteBuffer;
import java.util.List;

public interface OrderedKeyValueStore extends KeyValueStore {

	
	//public boolean containsInInterval(ByteBuffer startKey, ByteBuffer endKey, TransactionHandle txh);
	
	
	public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, boolean startInclusive, boolean endInculsive, KeySelector selector, TransactionHandle txh);
	
	
	public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, boolean startInclusive, boolean endInclusive, int limit, TransactionHandle txh);
	
	public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, boolean startInclusive, boolean endInculsive, TransactionHandle txh);

}
