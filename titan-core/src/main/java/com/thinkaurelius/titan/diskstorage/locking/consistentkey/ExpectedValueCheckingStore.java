package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.locking.Locker;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
public class ExpectedValueCheckingStore implements KeyColumnValueStore {

    /**
     * Configuration setting key for the local lock mediator prefix
     */
    public static final String LOCAL_LOCK_MEDIATOR_PREFIX_KEY = "local-lock-mediator-prefix";

    private static final Logger log = LoggerFactory.getLogger(ExpectedValueCheckingStore.class);

    /**
     * Titan data store.
     */
    final KeyColumnValueStore dataStore;

    final Locker locker;

    public ExpectedValueCheckingStore(KeyColumnValueStore dataStore, Locker locker) {
        Preconditions.checkNotNull(dataStore);
        this.dataStore = dataStore;
        this.locker = locker;
    }

    public KeyColumnValueStore getDataStore() {
        return dataStore;
    }

    private StoreTransaction getBaseTx(StoreTransaction txh) {
        Preconditions.checkNotNull(txh);
        Preconditions.checkArgument(txh instanceof ExpectedValueCheckingTransaction);
        return ((ExpectedValueCheckingTransaction) txh).getBaseTransaction();
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        return dataStore.containsKey(key, getBaseTx(txh));
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        return dataStore.getSlice(query, getBaseTx(txh));
    }

    @Override
    public List<List<Entry>> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
        return dataStore.getSlice(keys, query, getBaseTx(txh));
    }

    /**
     * {@inheritDoc}
     * <p/>
     * <p/>
     * <p/>
     * This implementation supports locking when {@code lockStore} is non-null.
     */
    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws StorageException {
        if (locker != null) {
            ExpectedValueCheckingTransaction tx = (ExpectedValueCheckingTransaction) txh;
            if (!tx.isMutationStarted()) {
                tx.mutationStarted();
                locker.checkLocks(tx.getConsistentTransaction());
                tx.checkExpectedValues();
            }
        }
        dataStore.mutate(key, additions, deletions, getBaseTx(txh));
    }

    /**
     * {@inheritDoc}
     * <p/>
     * <p/>
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
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        if (locker != null) {
            ExpectedValueCheckingTransaction tx = (ExpectedValueCheckingTransaction) txh;
            if (tx.isMutationStarted())
                throw new PermanentLockingException("Attempted to obtain a lock after mutations had been persisted");
            KeyColumn lockID = new KeyColumn(key, column);
            log.debug("Attempting to acquireLock on {} ev={}", lockID, expectedValue);
            locker.writeLock(lockID, tx.getConsistentTransaction());
            tx.storeExpectedValue(this, lockID, expectedValue);
        } else {
            dataStore.acquireLock(key, column, expectedValue, getBaseTx(txh));
        }
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws StorageException {
        return dataStore.getKeys(query, getBaseTx(txh));
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws StorageException {
        return dataStore.getKeys(query, getBaseTx(txh));
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        return dataStore.getLocalKeyPartition();
    }

    @Override
    public String getName() {
        return dataStore.getName();
    }

    @Override
    public void close() throws StorageException {
        dataStore.close();
        // TODO close locker?
    }

    void deleteLocks(ExpectedValueCheckingTransaction tx) throws StorageException {
        locker.deleteLocks(tx.getConsistentTransaction());
    }
}
