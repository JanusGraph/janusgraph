package com.thinkaurelius.titan.diskstorage.locking;

import com.google.common.collect.MapMaker;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * A store for {@code LockStatus} objects. Thread-safe so long as the method
 * calls with any given {@code StoreTransaction} are serial. Put another way,
 * thread-safety is only broken by concurrently calling this class's methods
 * with the same {@code StoreTransaction} instance in the arguments to each
 * concurrent call.
 * 
 * @see AbstractLocker
 * @param <S>
 *            The {@link LockStatus} type.
 */
public class LockerState<S> {

    /**
     * Locks taken in the LocalLockMediator and written to the store (but not
     * necessarily checked)
     */
    private final ConcurrentMap<StoreTransaction, Map<KeyColumn, S>> locks;

    public LockerState() {
        // TODO this wild guess at the concurrency level should not be hardcoded
        this(new MapMaker().concurrencyLevel(8).weakKeys()
                .<StoreTransaction, Map<KeyColumn, S>> makeMap());
    }

    public LockerState(ConcurrentMap<StoreTransaction, Map<KeyColumn, S>> locks) {
        this.locks = locks;
    }

    public boolean has(StoreTransaction tx, KeyColumn kc) {
        return getLocksForTx(tx).containsKey(kc);
    }

    public void take(StoreTransaction tx, KeyColumn kc, S ls) {
        getLocksForTx(tx).put(kc, ls);
    }

    public void release(StoreTransaction tx, KeyColumn kc) {
        getLocksForTx(tx).remove(kc);
    }

    public Map<KeyColumn, S> getLocksForTx(StoreTransaction tx) {
        Map<KeyColumn, S> m = locks.get(tx);

        if (null == m) {
            m = new HashMap<KeyColumn, S>();
            final Map<KeyColumn, S> x = locks.putIfAbsent(tx, m);
            if (null != x) {
                m = x;
            }
        }

        return m;
    }
}
