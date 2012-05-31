package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;

/**
 * This interface is used by transactions to call
 * implementation-specific methods on OrderedKeyColumnValueStore objects.
 * 
 * This interface should not be used by code besides Titan core.
 */
public interface LockConfig {

	public OrderedKeyColumnValueStore getDataStore();
	
	public OrderedKeyColumnValueStore getLockStore();
	
	public LocalLockMediator getLocalLockMediator();
	
	public byte[] getRid();
	
	public int getLockRetryCount();
	
	public long getLockExpireMS();
	
	public long getLockWaitMS();
}
