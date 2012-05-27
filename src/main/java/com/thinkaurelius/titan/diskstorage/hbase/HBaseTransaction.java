package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.locking.LockingTransaction;

/**
 * Stub.  Does nothing.
 * 
 * @author dalaro
 *
 */
public class HBaseTransaction extends LockingTransaction {

	public HBaseTransaction(StorageManager sm, byte[] rid,
			int lockRetryCount, long lockWaitMS, long lockExpireMS) {
		super(sm, rid, lockRetryCount, lockWaitMS, lockExpireMS);
	}
	
}
