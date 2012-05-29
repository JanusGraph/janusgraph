package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.List;

public interface ReadOrderedKeyColumnValueStore {

	/**
	 * Returns true if the specified key exists in the store, i.e. there is at least one column-value
	 * pair for the key.
	 * 
	 * @param key Key
	 * @param txh Transaction
	 * @return TRUE, if key has at least one column-value pair, else FALSE
	 */
	public boolean containsKey(ByteBuffer key, TransactionHandle txh);
	
	/**
	 * Retrieves the list of entries (i.e. column-value pairs) for a specified key which
	 * lie between the specified start and end columns.
     * The start and end columns are considered to be inclusive and exclusive, respectively.
	 * 
	 * Only retrieves a maximum number of entries as specified by the limit. 
	 * 
	 * @param key Key
	 * @param columnStart Tail Column (inclusive)
	 * @param columnEnd Head Column (exclusive)
	 * @param limit Maximum number of entries to retrieve
	 * @param txh Transaction
	 * @throws com.thinkaurelius.titan.core.GraphStorageException when columnEnd < columnStart as determined in
	 *         {@link ByteBufferUtil#isSmallerThan(ByteBuffer,ByteBuffer)}
	 * @return List of entries up to a maximum of "limit" entries
	 */
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, int limit, TransactionHandle txh);

	
	/**
	 * Retrieves the list of entries (i.e. column-value pairs) for a specified key which
	 * lie between the specified start and end columns.
	 * The start and end columns are considered to be inclusive and exclusive, respectively.
	 * 
	 * Retrieves all entries.
	 * 
	 * @param key Key
	 * @param columnStart Tail Column (inclusive)
	 * @param columnEnd Head Column (exclusive)
	 * @param txh Transaction
	 * @throws com.thinkaurelius.titan.core.GraphStorageException when columnEnd < columnStart as determined in
	 *         {@link ByteBufferUtil#isSmallerThan(ByteBuffer,ByteBuffer)}
	 * @return List of entries
	 */
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, TransactionHandle txh);

	
}
