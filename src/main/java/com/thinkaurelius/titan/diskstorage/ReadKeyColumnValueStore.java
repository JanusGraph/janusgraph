package com.thinkaurelius.titan.diskstorage;

import java.nio.ByteBuffer;

public interface ReadKeyColumnValueStore {

	/**
	 * Retrieves the value for the specified column and key under the given transaction
	 * from the store if such exists, otherwise NULL
	 * 
	 * @param key Key
	 * @param column Column
	 * @param txh Transaction
	 * @return Value for key and column or NULL
	 */
	public ByteBuffer get(ByteBuffer key, ByteBuffer column, TransactionHandle txh);
	
	/**
	 * Returns true if the specified key-column pair exists in the store.
	 * 
	 * @param key Key
	 * @param column Column
	 * @param txh Transaction
	 * @return TRUE, if key has at least one column-value pair, else FALSE
	 */
	public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column, TransactionHandle txh);

	/**
	 * Returns true if specified key is stored locally. Note that the key may not exist
	 * and the operation does not ensure existence.
	 * This method is only meaningful for distributed stores. For local stores it will
	 * always return true.
	 * 
	 * @param key Key
	 * @return TRUE, if the key is stored locally, else FALSE
	 */
	public boolean isLocalKey(ByteBuffer key);
}
