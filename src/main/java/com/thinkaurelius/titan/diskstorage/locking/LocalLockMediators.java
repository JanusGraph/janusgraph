package com.thinkaurelius.titan.diskstorage.locking;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum LocalLockMediators implements LocalLockMediatorProvider {
	INSTANCE;
	
	private static final Logger log =
			LoggerFactory.getLogger(LocalLockMediators.class);

	/*
	 * locking-namespace -> mediator.
	 * 
	 * For Cassandra, "locking-namespace" is a column family name.
	 */
	private final ConcurrentHashMap<String, LocalLockMediator> mediators =
			new ConcurrentHashMap<String, LocalLockMediator>();

	public LocalLockMediator get(String namespace) {
		LocalLockMediator m = mediators.get(namespace);
		
		if (null == m) {
			m = new LocalLockMediator(namespace);
			LocalLockMediator old = mediators.putIfAbsent(namespace, m);
			if (null != old)
				m = old;
			else 
				log.debug("Local lock mediator instantiated for namespace {}", namespace);
		}
		
		return m;
	}
	
	/**
	 * Only use this in testing.
	 */
	public void clear() {
		mediators.clear();
	}
}
