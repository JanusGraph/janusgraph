package com.thinkaurelius.titan.diskstorage;


import com.thinkaurelius.titan.core.GraphStorageException;

import java.nio.ByteBuffer;

public interface KeyColumnValueStore extends ReadKeyColumnValueStore, WriteKeyColumnValueStore {

	public static final ByteBuffer defaultColumn = ByteBuffer.allocate(1).put((byte)0).asReadOnlyBuffer();
	
	/**
	 * Closes the store.
	 * 
	 * @throws GraphStorageException
	 */
	public void close() throws GraphStorageException;
	
}
