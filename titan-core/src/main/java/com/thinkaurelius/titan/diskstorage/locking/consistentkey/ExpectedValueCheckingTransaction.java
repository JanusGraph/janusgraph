package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * A {@link StoreTransaction} that supports locking via
 * {@link LocalLockMediator} and writing and reading lock records in a
 * {@link ExpectedValueCheckingStore}.
 * <p/>
 * <p/>
 * <b>This class is not safe for concurrent use by multiple threads.
 * Multithreaded access must be prevented or externally synchronized.</b>
 */
public class ExpectedValueCheckingTransaction implements StoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(ExpectedValueCheckingTransaction.class);

    /**
     * This variable starts false.  It remains false during the
     * locking stage of a transaction.  It is set to true at the
     * beginning of the first mutate/mutateMany call in a transaction
     * (before performing any writes to the backing store).
     */
    private boolean isMutationStarted;

    /**
     * Transaction on the store holding information. This is only used for
     * locking-related metadata. No client data are read or written through this
     * transaction.
     */
    private final StoreTransaction lockTx;

    /**
     * Transaction for reading and writing client data.
     */
    private final StoreTransaction dataTx;
    private final Duration maxReadTime;

    private final Map<ExpectedValueCheckingStore, Map<KeyColumn, StaticBuffer>> expectedValuesByStore =
            new HashMap<ExpectedValueCheckingStore, Map<KeyColumn, StaticBuffer>>();

    public ExpectedValueCheckingTransaction(StoreTransaction dataTx, StoreTransaction lockTx, Duration maxReadTime) {
        //Preconditions.checkArgument(consistentTx.getConfiguration().getConsistency() == ConsistencyLevel.KEY_CONSISTENT);
        this.dataTx = dataTx;
        this.lockTx = lockTx;
        this.maxReadTime = maxReadTime;
    }

    StoreTransaction getDataTransaction() {
        return dataTx;
    }

    StoreTransaction getLockTransaction() {
        return lockTx;
    }

    private void lockedOn(ExpectedValueCheckingStore store) {
        Map<KeyColumn, StaticBuffer> m = expectedValuesByStore.get(store);

        if (null == m) {
            m = new HashMap<KeyColumn, StaticBuffer>();
            expectedValuesByStore.put(store, m);
        }
    }

    void storeExpectedValue(ExpectedValueCheckingStore store, KeyColumn lockID, StaticBuffer value) {
        Preconditions.checkNotNull(store);
        Preconditions.checkNotNull(lockID);

        lockedOn(store);
        Map<KeyColumn, StaticBuffer> m = expectedValuesByStore.get(store);
        assert null != m;
        if (m.containsKey(lockID)) {
            log.debug("Multiple expected values for {}: keeping initial value {} and discarding later value {}",
                    new Object[]{lockID, m.get(lockID), value});
        } else {
            m.put(lockID, value);
            log.debug("Store expected value for {}: {}", lockID, value);
        }
    }

    void checkExpectedValues() throws StorageException {
        for (final ExpectedValueCheckingStore store : expectedValuesByStore.keySet()) {
            final Map<KeyColumn, StaticBuffer> m = expectedValuesByStore.get(store);
            for (final KeyColumn kc : m.keySet()) {
                checkSingleExpectedValue(kc, m.get(kc), store);
            }
        }
    }

    private void checkSingleExpectedValue(final KeyColumn kc,
                                          final StaticBuffer ev, final ExpectedValueCheckingStore store)
            throws StorageException {
        BackendOperation.executeDirect(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                checkSingleExpectedValueUnsafe(kc, ev, store);
                return true;
            }
            @Override
            public String toString() {
                return "ExpectedValueChecking";
            }
        },maxReadTime);
    }

    private void checkSingleExpectedValueUnsafe(final KeyColumn kc,
                                                final StaticBuffer ev, final ExpectedValueCheckingStore store) throws StorageException {
        KeySliceQuery ksq = new KeySliceQuery(kc.getKey(), kc.getColumn(), BufferUtil.nextBiggerBuffer(kc.getColumn()));
        List<Entry> actualEntries = store.getSlice(ksq, this); // TODO make this consistent/QUORUM?

        if (null == actualEntries)
            actualEntries = ImmutableList.<Entry>of();

        Iterable<StaticBuffer> avList = Iterables.transform(actualEntries, new Function<Entry, StaticBuffer>() {
            @Override
            public StaticBuffer apply(Entry e) {
                assert e.getColumnAs(StaticBuffer.STATIC_FACTORY).equals(kc.getColumn());
                return e.getValueAs(StaticBuffer.STATIC_FACTORY);
            }
        });

        final Iterable<StaticBuffer> evList;

        if (null == ev) {
            evList = ImmutableList.<StaticBuffer>of();
        } else {
            evList = ImmutableList.<StaticBuffer>of(ev);
        }

        if (!Iterables.elementsEqual(evList, avList)) {
            throw new PermanentLockingException(
                    "Expected value mismatch for " + kc + ": expected="
                            + evList + " vs actual=" + avList + " (store=" + store.getName() + ")");
        }
    }

    private void deleteAllLocks() throws StorageException {
        for (ExpectedValueCheckingStore s : expectedValuesByStore.keySet()) {
            s.deleteLocks(this);
        }
    }

    @Override
    public void rollback() throws StorageException {
        deleteAllLocks();
        dataTx.rollback();
        lockTx.rollback();
    }

    @Override
    public void commit() throws StorageException {
        dataTx.commit();
        deleteAllLocks();
        lockTx.commit();
    }


    /**
     * Tells whether this transaction has been used in a
     * {@link ExpectedValueCheckingStore#mutate(StaticBuffer, List, List, StoreTransaction)}
     * call. When this returns true, the transaction is no longer allowed in
     * calls to
     * {@link ExpectedValueCheckingStore#acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)}.
     *
     * @return False until
     *         {@link ExpectedValueCheckingStore#mutate(StaticBuffer, List, List, StoreTransaction)}
     *         is called on this transaction instance. Returns true forever
     *         after.
     */
    public boolean isMutationStarted() {
        return isMutationStarted;
    }

    /**
     * Signals the transaction that it has been used in a call to
     * {@link ExpectedValueCheckingStore#mutate(StaticBuffer, List, List, StoreTransaction)}
     * . This transaction can't be used in subsequent calls to
     * {@link ExpectedValueCheckingStore#acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)}
     * .
     * <p/>
     * Calling this method at the appropriate time is handled automatically by
     * {@link ExpectedValueCheckingStore}. Titan users don't need to call this
     * method by hand.
     */
    public void mutationStarted() {
        isMutationStarted = true;
    }

    @Override
    public BaseTransactionConfig getConfiguration() {
        return dataTx.getConfiguration();
    }
}
