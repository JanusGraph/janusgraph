package com.thinkaurelius.titan.diskstorage.cassandra;

import com.thinkaurelius.titan.diskstorage.locking.LockingTransaction;

public class CassandraTransaction extends LockingTransaction {

	CassandraTransaction(CassandraThriftStorageManager sm, byte[] rid,
			int lockRetryCount, long lockWaitMS, long lockExpireMS) {
		super(sm, rid, lockRetryCount, lockWaitMS, lockExpireMS);
	}

}
