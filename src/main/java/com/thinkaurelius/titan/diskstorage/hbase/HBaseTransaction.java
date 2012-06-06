package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.diskstorage.locking.LockingTransaction;

/**
 * This class overrides and adds nothing compared with
 * {@link LockingTransaction}; however, it creates a transaction type specific
 * to HBase, which lets us check for user errors like passing a Cassandra
 * transaction into a HBase method.
 * 
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class HBaseTransaction extends LockingTransaction {

	public HBaseTransaction() {
		super();
	}
	
}
