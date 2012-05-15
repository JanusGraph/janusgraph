package com.thinkaurelius.titan.diskstorage.locking;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction;

public class LocalLockMediator {
	
	/*
	 * locking-namespace -> mediator.
	 * 
	 * For Cassandra, "locking-namespace" is a column family name.
	 */
	private static final ConcurrentHashMap<String, LocalLockMediator> mediators =
			new ConcurrentHashMap<String, LocalLockMediator>();
	
	private static final Logger log = LoggerFactory.getLogger(LocalLockMediator.class);

	// Locking namespace
	private final String name;
	
	// Lock map
	private final ConcurrentHashMap<KeyColumn, TransactionHandle> locks =
			new ConcurrentHashMap<KeyColumn, TransactionHandle>();
	
	private LocalLockMediator(String name) {
		this.name = name;
		
		assert null != this.name;
	}
	
	public static LocalLockMediator get(String name) {
		LocalLockMediator m = mediators.get(name);
		
		if (null == m) {
			m = new LocalLockMediator(name);
			LocalLockMediator old = mediators.putIfAbsent(name, m);
			if (null != old)
				m = old;
			else 
				log.debug("Local lock mediator instantiated for namespace {}", name);
		}
		
		return m;
	}
	
	public boolean lock(KeyColumn kc, TransactionHandle requestor) {
		assert null != kc;
		assert null != requestor;
		
		TransactionHandle holder = locks.putIfAbsent(kc, requestor);
		
		boolean r = null == holder || holder.equals(requestor);
		
		if (r) {
			log.debug("Local lock succeeded: {} in namespace {} by txn {}",
					new Object[]{kc, name, requestor});
		} else {
			log.debug("Local lock failed: {} in namespace {} by txn {} (already owned by {})",
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
	public void unlock(KeyColumn kc, CassandraTransaction requestor) {
		
		assert locks.containsKey(kc);
		assert locks.get(kc).equals(requestor);
		
		locks.remove(kc);
		
		log.debug("Local unlock succeeded: {} in namespace {} by {}",
				new Object[]{kc, name, requestor});
	}
	
	public String toString() {
		return "LocalLockMediator [" + name + ",  ~" + locks.size() + " current locks]";
	}
}
