package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.nio.ByteBuffer;
import java.util.List;

public interface KeyValueStore {

	public void insert(List<KeyValueEntry> entries, TransactionHandle txh) throws StorageException;
	

	public void delete(List<ByteBuffer> keys, TransactionHandle txh) throws StorageException;
	

	public ByteBuffer get(ByteBuffer key, TransactionHandle txh) throws StorageException;
	

	public boolean containsKey(ByteBuffer key, TransactionHandle txh) throws StorageException;


	public boolean isLocalKey(ByteBuffer key) throws StorageException;
	

	public void acquireLock(ByteBuffer key, ByteBuffer expectedValue, TransactionHandle txh) throws StorageException;


	public void close() throws StorageException;
	
}
