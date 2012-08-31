package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.nio.ByteBuffer;
import java.util.List;

public interface OrderedKeyValueStore extends KeyValueStore {

	public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, KeySelector selector, TransactionHandle txh) throws StorageException;
	
	public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, int limit, TransactionHandle txh) throws StorageException;
	
	public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, TransactionHandle txh) throws StorageException;

}
