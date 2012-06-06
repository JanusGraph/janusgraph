package com.thinkaurelius.titan.diskstorage.locking;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.TransactionHandle;

/**
 * This class resolves lock contention between two transactions on the same JVM.
 * 
 * This is not just an optimization to reduce network traffic. Locks written by
 * Titan to a distributed key-value store contain an identifier, the "Rid",
 * which is unique only to the process level. The Rid can't tell which
 * transaction in a process holds any given lock. This class prevents two
 * transactions in a single process from concurrently writing the same lock to a
 * distributed key-value store.
 * 
 * @author Dan LaRocque <dalaro@hopcount.org>
 */

public class LocalLockMediator {

	
	private static final Logger log = LoggerFactory.getLogger(LocalLockMediator.class);

	// Locking namespace
	private final String name;
	
	// Lock map: TODO: Figure out good parameters constructor
	private final ConcurrentHashMap<KeyColumn, TransactionHandle> locks =
			new ConcurrentHashMap<KeyColumn, TransactionHandle>();
	
	public LocalLockMediator(String name) {
		this.name = name;
		
		assert null != this.name;
	}
	
	public boolean lock(KeyColumn kc, TransactionHandle requestor) {
		assert null != kc;
		assert null != requestor;
		
		TransactionHandle holder = locks.putIfAbsent(kc, requestor);
		
		boolean r = null == holder || holder.equals(requestor);
		
		if (r) {
			log.trace("Local lock succeeded: {} in namespace {} by txn {}",
					new Object[]{kc, name, requestor});
		} else {
			log.trace("Local lock failed: {} in namespace {} by txn {} (already owned by {})",
					new Object[]{kc, name, requestor, holder});
		}
		
		return r;
	}
	
	/*
	 * The requestor argument isn't strictly necessary.  The
	 * requestor argument's only use in this method is to let
	 * us assert that the lock being released is indeed held
	 * by the transaction attempting to unlock.
	 */
	public void unlock(KeyColumn kc, LockingTransaction requestor) {
		
		assert locks.containsKey(kc);
		assert locks.get(kc).equals(requestor);
		
		locks.remove(kc);
		
		log.trace("Local unlock succeeded: {} in namespace {} by {}",
				new Object[]{kc, name, requestor});
	}
	
	public String toString() {
		return "LocalLockMediator [" + name + ",  ~" + locks.size() + " current locks]";
	}

}
