package com.thinkaurelius.titan.diskstorage.cassandra;

import com.thinkaurelius.titan.diskstorage.locking.LockingTransactionHandle;

/**
 * This class overrides and adds nothing compared with
 * {@link com.thinkaurelius.titan.diskstorage.locking.LockingTransactionHandle}; however, it creates a transaction type specific
 * to Cassandra, which lets us check for user errors like passing a HBase
 * transaction into a Cassandra method.
 * 
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class CassandraTransaction extends LockingTransactionHandle {

	CassandraTransaction() {
		super();
	}

}
