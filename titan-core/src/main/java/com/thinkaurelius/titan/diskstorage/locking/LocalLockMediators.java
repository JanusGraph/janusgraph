package com.thinkaurelius.titan.diskstorage.locking;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map.Entry;
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
    private final ConcurrentHashMap<String, LocalLockMediator<?>> mediators = new ConcurrentHashMap<String, LocalLockMediator<?>>();

    @Override
    public <T> LocalLockMediator<T> get(String namespace) {
        
        Preconditions.checkNotNull(namespace);
        
        @SuppressWarnings("unchecked")
        LocalLockMediator<T> m = (LocalLockMediator<T>)mediators.get(namespace);

        if (null == m) {
            m = new LocalLockMediator<T>(namespace);
            @SuppressWarnings("unchecked")
            LocalLockMediator<T> old = (LocalLockMediator<T>)mediators.putIfAbsent(namespace, m);
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
    
    /**
     * Only use this in testing.
     * <p>
     * This deletes all entries in the global map of namespaces to mediators
     * whose namespace key equals the argument.
     * 
     * @param prefix 
     */
    public void clear(String namespace) {
        Iterator<Entry<String, LocalLockMediator<?>>> iter = mediators.entrySet().iterator();
        
        while (iter.hasNext()) {
            Entry<String, LocalLockMediator<?>> e = iter.next();
            
            if (e.getKey().equals(namespace)) {
                iter.remove();
            }
        }
    }
}
