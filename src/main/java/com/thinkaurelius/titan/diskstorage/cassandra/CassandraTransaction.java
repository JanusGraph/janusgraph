package com.thinkaurelius.titan.diskstorage.cassandra;

import com.thinkaurelius.titan.diskstorage.TransactionHandle;

public class CassandraTransaction implements TransactionHandle {

	
	/* Dan: Add methods to return the read and write consistency levels to use.
	 * Call those methods when executing an operation.
	 * For now, simply specify a default read and write consistency level. Later, we will
	 * allow those to be specified when startin a transaction.
	 */
	
	@Override
	public void abort() {
		//Do nothing
	}

	@Override
	public void commit() {
		//There is not commit for Cassandra
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

}
