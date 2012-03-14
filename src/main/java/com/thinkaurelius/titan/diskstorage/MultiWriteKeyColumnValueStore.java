package com.thinkaurelius.titan.diskstorage;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public interface MultiWriteKeyColumnValueStore {

	/**
	 * Insert a batch of Entries.
	 * 
	 * @param insertions Entries to insert, indexed by their keys
	 * @param txh Transaction
	 */
	public void insertMany(Map<ByteBuffer, List<Entry>> insertions, TransactionHandle txh);
	
	/**
	 * Delete a batch of key-column-list pairs.
	 * 
	 * @param deletions Columns to delete, indexed by their keys
	 * @param txh Transaction
	 */
	public void deleteMany(Map<ByteBuffer, List<ByteBuffer>> deletions, TransactionHandle txh);
}
