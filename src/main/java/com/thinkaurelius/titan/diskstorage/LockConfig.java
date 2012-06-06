package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;

/**
 * This interface is used by transactions to call implementation-specific
 * methods on OrderedKeyColumnValueStore objects.
 * 
 * This interface should not be used by code besides Titan core.
 * 
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public interface LockConfig {

	/**
	 * Retrieves the store containing Titan graph data.
	 * 
	 * @return graph data store
	 */
	public OrderedKeyColumnValueStore getDataStore();

	/**
	 * Retrieves the store containing lock claims (no graph data).
	 * 
	 * @return lock claim store
	 */
	public OrderedKeyColumnValueStore getLockStore();

	/**
	 * Retrieves the local lock mediator through which transactions using this
	 * LockConfig must arbitrate lock contention before writing claims to
	 * {@link #getlockstore()}.
	 * 
	 * @return contention mediator for local transactions
	 */
	public LocalLockMediator getLocalLockMediator();

	/**
	 * An arbitrary sequence of bytes uniquely identifying this JVM/process for
	 * locking purposes.
	 * 
	 * @return the "lock requester id"
	 */
	public byte[] getRid();

	/**
	 * The number of times to try writing a lock claim to a distributed
	 * key-value store before giving up. This is LR in the draft locking
	 * protocol spec.
	 * 
	 * @return lock claim write retry count
	 */
	public int getLockRetryCount();

	/**
	 * The number of milliseconds which must pass between a lock claim's
	 * timestamp and {@link java.lang.System#currentTimeMillis()} before said
	 * lock claim is considered expired and invalid. This is XT in the draft
	 * locking protocol spec.
	 * 
	 * @return lock expiration time in milliseconds
	 */
	public long getLockExpireMS();

	/**
	 * the number of milliseconds transactions must wait after writing lock
	 * claims to a distributed key-value store before reading back potentially
	 * contending claims. This is WT in the draft locking protocol spec.
	 * 
	 * @return lock wait time in milliseconds
	 */
	public long getLockWaitMS();
}
