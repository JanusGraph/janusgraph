package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.LockConfig;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;


public class SimpleLockConfig implements LockConfig {
	private final OrderedKeyColumnValueStore dataStore;
	private final OrderedKeyColumnValueStore lockStore;
	private final LocalLockMediator localLockMediator;
	private final byte[] rid;
	private final int lockRetryCount;
	private final long lockExpireMS;
	private final long lockWaitMS;
	
	public SimpleLockConfig(OrderedKeyColumnValueStore dataStore,
			OrderedKeyColumnValueStore lockStore,
			LocalLockMediator localLockMediator, byte[] rid,
			int lockRetryCount, long lockWaitMS, long lockExpireMS) {
		super();
		this.dataStore = dataStore;
		this.lockStore = lockStore;
		this.localLockMediator = localLockMediator;
		this.rid = rid;
		this.lockRetryCount = lockRetryCount;
		this.lockWaitMS = lockWaitMS;
		this.lockExpireMS = lockExpireMS;
	}

	@Override
	public OrderedKeyColumnValueStore getDataStore() {
		return dataStore;
	}

	@Override
	public OrderedKeyColumnValueStore getLockStore() {
		return lockStore;
	}

	@Override
	public LocalLockMediator getLocalLockMediator() {
		return localLockMediator;
	}

	@Override
	public byte[] getRid() {
		return rid;
	}

	@Override
	public int getLockRetryCount() {
		return lockRetryCount;
	}

	@Override
	public long getLockExpireMS() {
		return lockExpireMS;
	}

	@Override
	public long getLockWaitMS() {
		return lockWaitMS;
	}
}
