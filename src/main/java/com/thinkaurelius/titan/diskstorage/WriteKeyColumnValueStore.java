package com.thinkaurelius.titan.diskstorage;

import java.nio.ByteBuffer;
import java.util.List;

public interface WriteKeyColumnValueStore {
	
	/** 
	 * Inserts the entries (i.e. column-value pairs) for the specified key into the store
	 * under the given transaction.
	 * 
	 * TODO: We should ensure that insertions do not overwrite existing data/values.
	 * 
	 * @param key Key
	 * @param entries List of entries
	 * @param txh Transaction under which to execute the operation
	 */
	public void insert(ByteBuffer key, List<Entry> entries, TransactionHandle txh);
	
	/**
	 * Deletes the list of columns for the specified key in the store under the given transaction
	 * 
	 * @param key Key
	 * @param columns List of columns
	 * @param txh Transaction
	 */
	public void delete(ByteBuffer key, List<ByteBuffer> columns, TransactionHandle txh);

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
