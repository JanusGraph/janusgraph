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
	
	public static class Builder {
		private OrderedKeyColumnValueStore dataStore;
		private OrderedKeyColumnValueStore lockStore;
		private LocalLockMediator localLockMediator;
		private byte[] rid;
		private Integer lockRetryCount;
		private Long lockExpireMS;
		private Long lockWaitMS;
		
		public Builder dataStore(OrderedKeyColumnValueStore dataStore) {
			this.dataStore = dataStore;
			return this;
		}
		
		public Builder lockStore(OrderedKeyColumnValueStore lockStore) {
			this.lockStore = lockStore;
			return this;
		}
		
		public Builder localLockMediator(LocalLockMediator localLockMediator) {
			this.localLockMediator = localLockMediator;
			return this;
		}
		
		public Builder rid(byte[] rid) {
			this.rid = rid;
			return this;
		}
		
		public Builder lockRetryCount(int lockRetryCount) {
			this.lockRetryCount = lockRetryCount;
			return this;
		}
		
		public Builder lockExpireMS(long lockExpireMS) {
			this.lockExpireMS = lockExpireMS;
			return this;
		}
		
		public Builder lockWaitMS(long lockWaitMS) {
			this.lockWaitMS = lockWaitMS;
			return this;
		}
		
		public SimpleLockConfig build() {
			if (null == dataStore)
				throw new NullPointerException("dataStore");
			
			if (null == lockStore)
				throw new NullPointerException("lockStore");
			
			if (null == localLockMediator)
				throw new NullPointerException("localLockMediator");
			
			if (null == rid)
				throw new NullPointerException("rid");
			
			if (null == lockRetryCount)
				throw new NullPointerException("lockRetryCount");
			
			if (null == lockExpireMS)
				throw new NullPointerException("lockExpireMS");
			
			if (null == lockWaitMS)
				throw new NullPointerException("lockWaitMS");
			
			return new SimpleLockConfig(dataStore, lockStore,
					localLockMediator, rid, lockRetryCount, lockWaitMS,
					lockExpireMS);
		}
		
	}
}
