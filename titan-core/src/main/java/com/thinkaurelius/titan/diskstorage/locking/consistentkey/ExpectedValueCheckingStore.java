package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVSProxy;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.Locker;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;

/**
 * A {@link KeyColumnValueStore} wrapper intended for nontransactional stores
 * that forwards all <b>but</b> these two methods to an encapsulated store
 * instance:
 * <p/>
 * <ul>
 * <li>{@link #acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)}</li>
 * <li>{@link #mutate(StaticBuffer, List, List, StoreTransaction)}</li>
 * </ul>
 * <p/>
 * This wrapper adds some logic to both of the overridden methods before calling
 * the encapsulated store's version.
 * <p/>
 * This class, along with its collaborator class
 * {@link ExpectedValueCheckingTransaction}, track all {@code expectedValue}
 * arguments passed to {@code acquireLock} for each {@code StoreTransaction}.
 * When the transaction first {@code mutate(...)}s, the these classes cooperate
 * to check that all previously provided expected values match actual values,
 * throwing an exception and preventing mutation if a mismatch is detected.
 * <p/>
 * This relies on a {@code Locker} instance supplied during construction for
 * locking.
 */
public class ExpectedValueCheckingStore extends KCVSProxy {

    private static final Logger log = LoggerFactory.getLogger(ExpectedValueCheckingStore.class);

    final Locker locker;

    public ExpectedValueCheckingStore(KeyColumnValueStore store, Locker locker) {
        super(store);
        this.locker = locker;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation supports locking when {@code lockStore} is non-null.
     */
    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        ExpectedValueCheckingTransaction etx = (ExpectedValueCheckingTransaction)txh;
        boolean hasAtLeastOneLock = etx.prepareForMutations();
        if (hasAtLeastOneLock) {
            // Force all mutations on this transaction to use strong consistency
            store.mutate(key, additions, deletions, getConsistentTx(txh));
        } else {
            store.mutate(key, additions, deletions, unwrapTx(txh));
        }
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation supports locking when {@code lockStore} is non-null.
     * <p/>
     * Consider the following scenario. This method is called twice with
     * identical key, column, and txh arguments, but with different
     * expectedValue arguments in each call. In testing, it seems titan's
     * graphdb requires that implementations discard the second expectedValue
     * and, when checking expectedValues vs actual values just prior to mutate,
     * only the initial expectedValue argument should be considered.
     */
    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        if (locker != null) {
            ExpectedValueCheckingTransaction tx = (ExpectedValueCheckingTransaction) txh;
            if (tx.isMutationStarted())
                throw new PermanentLockingException("Attempted to obtain a lock after mutations had been persisted");
            KeyColumn lockID = new KeyColumn(key, column);
            log.debug("Attempting to acquireLock on {} ev={}", lockID, expectedValue);
            locker.writeLock(lockID, tx.getConsistentTx());
            tx.storeExpectedValue(this, lockID, expectedValue);
        } else {
            store.acquireLock(key, column, expectedValue, unwrapTx(txh));
        }
    }

    Locker getLocker() {
        return locker;
    }

    void deleteLocks(ExpectedValueCheckingTransaction tx) throws BackendException {
        locker.deleteLocks(tx.getConsistentTx());
    }

    KeyColumnValueStore getBackingStore() {
        return store;
    }

    protected StoreTransaction unwrapTx(StoreTransaction t) {
        assert null != t;
        assert t instanceof ExpectedValueCheckingTransaction;
        return ((ExpectedValueCheckingTransaction) t).getInconsistentTx();
    }

    private static StoreTransaction getConsistentTx(StoreTransaction t) {
        assert null != t;
        assert t instanceof ExpectedValueCheckingTransaction;
        return ((ExpectedValueCheckingTransaction) t).getConsistentTx();
    }
}
