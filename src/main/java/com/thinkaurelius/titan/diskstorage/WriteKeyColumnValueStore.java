package com.thinkaurelius.titan.diskstorage;

import java.nio.ByteBuffer;
import java.util.List;

public interface WriteKeyColumnValueStore {
	
	/** 
	 * Applies the specified insertion and deletion mutations to the provided key.
	 * Both, the list of additions or deletions, may be empty or NULL if there is nothing to be added and/or deleted.
	 *
	 * @param key Key
	 * @param additions List of entries (column + value) to be added
     * @param deletions List of columns to be removed
	 * @param txh Transaction under which to execute the operation
	 */
	public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, TransactionHandle txh);
	
	/**
	 * Acquires a lock for the key-column pair which ensures that nobody else can take a lock on that 
	 * respective entry for the duration of this lock (but somebody could potentially still overwrite
	 * the key-value entry without taking a lock).
	 *
	 * The lock has to be released when the transaction closes (commits or aborts).
	 * 
	 * @param key Key
	 * @param column Column
	 * @param txh Transaction
	 */
	public void acquireLock(ByteBuffer key, ByteBuffer column, TransactionHandle txh);
	
}
