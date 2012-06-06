package com.thinkaurelius.titan.diskstorage.writeaggregation;

import java.nio.ByteBuffer;
import java.util.Map;

import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

public interface MultiWriteKeyColumnValueStore extends LockKeyColumnValueStore {

	/**
	 * Apply a batch of mutations
	 * 
	 * @param mutations Mutations to apply indexed by their keys
	 * @param txh Transaction
	 */
	public void mutateMany(Map<ByteBuffer, Mutation> mutations, TransactionHandle txh);

}
