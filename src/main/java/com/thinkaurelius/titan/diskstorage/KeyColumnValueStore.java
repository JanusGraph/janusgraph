package com.thinkaurelius.titan.diskstorage;


import java.nio.ByteBuffer;

public interface KeyColumnValueStore extends ReadKeyColumnValueStore, WriteKeyColumnValueStore {

	public static final ByteBuffer defaultColumn = ByteBuffer.allocate(1).put((byte)0).asReadOnlyBuffer();
	
	public void close() throws StorageException;
	
}
