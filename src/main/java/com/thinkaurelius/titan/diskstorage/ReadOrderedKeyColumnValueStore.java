package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

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
	 * Whether the start and end columns should considered inclusive or exclusive is specified by additional boolean parameters.
	 * 
	 * Only retrieves a maximum number of entries as specified by the limit. If the number of entries
	 * exceeds the limit, NULL is returned.
	 * 
	 * @param key Key
	 * @param columnStart Start Column
	 * @param startInclusive Whether columnStart is inclusive, i.e. part of the result set
	 * @param columnEnd End Column
	 * @param endInclusvie Whether columnEnd is inclusive, i.e. part of the result set
	 * @param limit Maximum number of entries to retrieve
	 * @param txh Transaction
	 * @throws GraphStorageException when columnEnd < columnStart as determined in
	 *         {@link ByteBufferUtil#isSmallerThan()}
	 * @return List of entries, or NULL if the number of entries exceeds the limit
	 */
	public List<Entry> getLimitedSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, boolean startInclusive, boolean endInclusive, int limit, TransactionHandle txh);
	
	/**
	 * Retrieves the list of entries (i.e. column-value pairs) for a specified key which
	 * lie between the specified start and end columns.
	 * Whether the start and end columns should considered inclusive or exclusive is specified by additional boolean parameters.
	 * 
	 * Only retrieves a maximum number of entries as specified by the limit. 
	 * 
	 * @param key Key
	 * @param columnStart Start Column
	 * @param startInclusive Whether columnStart is inclusive, i.e. part of the result set
	 * @param columnEnd End Column
	 * @param endInclusvie Whether columnEnd is inclusive, i.e. part of the result set
	 * @param limit Maximum number of entries to retrieve
	 * @param txh Transaction
	 * @throws GraphStorageException when columnEnd < columnStart as determined in
	 *         {@link ByteBufferUtil#isSmallerThan()}
	 * @return List of entries up to a maximum of "limit" entries
	 */
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, boolean startInclusive, boolean endInclusive, int limit, TransactionHandle txh);

	
	/**
	 * Retrieves the list of entries (i.e. column-value pairs) for a specified key which
	 * lie between the specified start and end columns.
	 * Whether the start and end columns should considered inclusive or exclusive is specified by additional boolean parameters.
	 * 
	 * Retrieves all entries.
	 * 
	 * @param key Key
	 * @param columnStart Start Column
	 * @param startInclusive Whether columnStart is inclusive, i.e. part of the result set
	 * @param columnEnd End Column
	 * @param endInclusvie Whether columnEnd is inclusive, i.e. part of the result set
	 * @param txh Transaction
	 * @throws GraphStorageException when columnEnd < columnStart as determined in
	 *         {@link ByteBufferUtil#isSmallerThan()}
	 * @return List of entries
	 */
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, boolean startInclusive, boolean endInclusive, TransactionHandle txh);
	
	/**
	 * Retrieves the list of entries (i.e. column-value pairs) that lie between the specified start and end columns for all keys which
	 * lie in the specified key range.
	 * Whether the start and end keys and columns should considered inclusive or exclusive is specified by additional boolean parameters.
	 * 
	 * Only retrieves a maximum number of keys as specified by keyLimit and for each key only a maximum number of entries as specified by columnLimit. 
	 * 
	 * @param keyStart Start Key
	 * @param keyEnd End Key
	 * @param startKeyInc Whether the start key is inclusive, i.e. part of the result set
	 * @param endKeyInc Whether the end key is inclusive, i.e. part of the result set
	 * @param columnStart Start Column
	 * @param columnEnd End Column
	 * @param startColumnInc Whether start column is inclusive, i.e. part of the result set
	 * @param endColumnInc Whether end column is inclusive, i.e. part of the result set
	 * @param keyLimit Maximum number of keys to retrieve
	 * @param columnLimit Maximum number of entries to retrieve per key
	 * @param txh Transaction
	 * @throws GraphStorageException when columnEnd < columnStart as determined in
	 *         {@link ByteBufferUtil#isSmallerThan()}
	 * @return List of entries for the key range up to the respective limit number of entries
	 */
	public Map<ByteBuffer,List<Entry>> getKeySlice(ByteBuffer keyStart, ByteBuffer keyEnd, boolean startKeyInc, boolean endKeyInc,
                                                   ByteBuffer columnStart, ByteBuffer columnEnd, boolean startColumnIncl, boolean endColumnIncl,
                                                   int keyLimit, int columnLimit, TransactionHandle txh);
	
	/**
	 * Retrieves the list of entries (i.e. column-value pairs) that lie between the specified start and end columns for all keys which
	 * lie in the specified key range.
	 * Whether the start and end keys and columns should considered inclusive or exclusive is specified by additional boolean parameters.
	 * 
	 * Only retrieves a maximum number of keys as specified by keyLimit and for each key only a maximum number of entries as specified by columnLimit or
	 * NULL if any of those limits are exceeded. 
	 * 
	 * @param keyStart Start Key
	 * @param keyEnd End Key
	 * @param startKeyInc Whether the start key is inclusive, i.e. part of the result set
	 * @param endKeyInc Whether the end key is inclusive, i.e. part of the result set
	 * @param columnStart Start Column
	 * @param columnEnd End Column
	 * @param startColumnInc Whether start column is inclusive, i.e. part of the result set
	 * @param endColumnInc Whether end column is inclusive, i.e. part of the result set
	 * @param keyLimit Maximum number of keys to retrieve
	 * @param columnLimit Maximum number of entries to retrieve per key
	 * @param txh Transaction
	 * @throws GraphStorageException when columnEnd < columnStart as determined in
	 *         {@link ByteBufferUtil#isSmallerThan()}
	 * @return List of entries for the key range up to the respective limit number of entries or NULL if either limit is exceeded
	 */
	public Map<ByteBuffer,List<Entry>> getLimitedKeySlice(ByteBuffer keyStart, ByteBuffer keyEnd, boolean startKeyInc, boolean endKeyInc,
                                                          ByteBuffer columnStart, ByteBuffer columnEnd, boolean startColumnIncl, boolean endColumnIncl,
                                                          int keyLimit, int columnLimit, TransactionHandle txh);
	
	/**
	 * Retrieves the list of entries (i.e. column-value pairs) that lie between the specified start and end columns for all keys which
	 * lie in the specified key range.
	 * Whether the start and end keys and columns should considered inclusive or exclusive is specified by additional boolean parameters.
	 * 
	 * Retrieves all keys and entries in the respective ranges.
	 * 
	 * @param keyStart Start Key
	 * @param keyEnd End Key
	 * @param startKeyInc Whether the start key is inclusive, i.e. part of the result set
	 * @param endKeyInc Whether the end key is inclusive, i.e. part of the result set
	 * @param columnStart Start Column
	 * @param columnEnd End Column
	 * @param startColumnInc Whether start column is inclusive, i.e. part of the result set
	 * @param endColumnInc Whether end column is inclusive, i.e. part of the result set
	 * @param txh Transaction
	 * @throws GraphStorageException when columnEnd < columnStart as determined in
	 *         {@link ByteBufferUtil#isSmallerThan()}
	 * @return List of entries for the key range up to the respective limit number of entries or NULL if either limit is exceeded
	 */
	public Map<ByteBuffer,List<Entry>> getKeySlice(ByteBuffer keyStart, ByteBuffer keyEnd, boolean startKeyInc, boolean endKeyInc,
                                                   ByteBuffer columnStart, ByteBuffer columnEnd, boolean startColumnIncl, boolean endColumnIncl,
                                                   TransactionHandle txh);
	
}
