package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A singleton maintaining a globally unique map of {@link LocalLockMediator}
 * instances.
 * 
 * @see LocalLockMediatorProvider
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public enum LocalLockMediators implements LocalLockMediatorProvider {
    INSTANCE;

    private static final Logger log = LoggerFactory
            .getLogger(LocalLockMediators.class);

    /**
     * Maps a namespace to the mediator responsible for the namespace.
     * <p>
     * Implementation note: for Cassandra, namespace is usually a column
     * family name.
     */
    private final ConcurrentHashMap<String, LocalLockMediator> mediators = new ConcurrentHashMap<String, LocalLockMediator>();

    @Override
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
     * <p>
     * This deletes the global map of namespaces to mediators. Calling this in
     * production would result in undetected locking failures and data
     * corruption.
     */
    public void clear() {
        mediators.clear();
    }
}
