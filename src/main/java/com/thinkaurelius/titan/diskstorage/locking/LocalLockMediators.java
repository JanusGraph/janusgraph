package com.thinkaurelius.titan.diskstorage.locking;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A singleton maintaining a globally unique map of locking-namespaces to {@see
 * LocalLockMediator} instances.
 * 
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public enum LocalLockMediators implements LocalLockMediatorProvider {
	INSTANCE;

	private static final Logger log = LoggerFactory
			.getLogger(LocalLockMediators.class);

	/*
	 * locking-namespace -> mediator.
	 * 
	 * For Cassandra, "locking-namespace" is a column family name.
	 */
	private final ConcurrentHashMap<String, LocalLockMediator> mediators = new ConcurrentHashMap<String, LocalLockMediator>();

	/**
	 * Returns the local lock mediator in charge of the supplied namespace.
	 * 
	 * This method is thread-safe. Exactly one instance of
	 * {@code LocalLockMediator} is returned for any given {@code namespace}.
	 * 
	 */
	public LocalLockMediator get(String namespace) {
		LocalLockMediator m = mediators.get(namespace);

		if (null == m) {
			m = new LocalLockMediator(namespace);
			LocalLockMediator old = mediators.putIfAbsent(namespace, m);
			if (null != old)
				m = old;
			else
				log.debug("Local lock mediator instantiated for namespace {}",
						namespace);
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
