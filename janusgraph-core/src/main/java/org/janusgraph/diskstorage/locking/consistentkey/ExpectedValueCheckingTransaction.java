// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.locking.consistentkey;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.locking.LocalLockMediator;
import org.janusgraph.diskstorage.locking.Locker;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.KeyColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * A {@link StoreTransaction} that supports locking via
 * {@link LocalLockMediator} and writing and reading lock records in a
 * {@link ExpectedValueCheckingStore}.
 * <p>
 * <p>
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
     * Transaction for reading and writing locking-related metadata. Also used
     * for reading expected values provided as arguments to
     * {@link KeyColumnValueStore#acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)}
     */
    private final StoreTransaction strongConsistentTx;

    /**
     * Transaction for reading and writing client data. No guarantees about
     * consistency strength.
     */
    private final StoreTransaction inconsistentTx;
    private final Duration maxReadTime;

    private final Map<ExpectedValueCheckingStore, Map<KeyColumn, StaticBuffer>> expectedValuesByStore = new HashMap<>();

    public ExpectedValueCheckingTransaction(StoreTransaction inconsistentTx, StoreTransaction strongConsistentTx, Duration maxReadTime) {
        this.inconsistentTx = inconsistentTx;
        this.strongConsistentTx = strongConsistentTx;
        this.maxReadTime = maxReadTime;
    }

    @Override
    public void rollback() throws BackendException {
        deleteAllLocks();
        inconsistentTx.rollback();
        strongConsistentTx.rollback();
    }

    @Override
    public void commit() throws BackendException {
        inconsistentTx.commit();
        deleteAllLocks();
        strongConsistentTx.commit();
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

    @Override
    public BaseTransactionConfig getConfiguration() {
        return inconsistentTx.getConfiguration();
    }

    public StoreTransaction getInconsistentTx() {
        return inconsistentTx;
    }

    public StoreTransaction getConsistentTx() {
        return strongConsistentTx;
    }

    void storeExpectedValue(ExpectedValueCheckingStore store, KeyColumn lockID, StaticBuffer value) {
        Preconditions.checkNotNull(store);
        Preconditions.checkNotNull(lockID);

        lockedOn(store);
        Map<KeyColumn, StaticBuffer> m = expectedValuesByStore.get(store);
        assert null != m;
        if (m.containsKey(lockID)) {
            log.debug("Multiple expected values for {}: keeping initial value {} and discarding later value {}",
                lockID, m.get(lockID), value);
        } else {
            m.put(lockID, value);
            log.debug("Store expected value for {}: {}", lockID, value);
        }
    }

    /**
     * If {@code !}{@link #isMutationStarted()}, check all locks and expected
     * values, then mark the transaction as started.
     * <p>
     * If {@link #isMutationStarted()}, this does nothing.
     *
     * @throws org.janusgraph.diskstorage.BackendException
     *
     * @return true if this transaction holds at least one lock, false if the
     *         transaction holds no locks
     */
    boolean prepareForMutations() throws BackendException {
        if (!isMutationStarted()) {
            checkAllLocks();
            checkAllExpectedValues();
            mutationStarted();
        }
        return !expectedValuesByStore.isEmpty();
    }

    /**
     * Check all locks attempted by earlier
     * {@link KeyColumnValueStore#acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)}
     * calls using this transaction.
     *
     * @throws org.janusgraph.diskstorage.BackendException
     */
    void checkAllLocks() throws BackendException {
        StoreTransaction lt = getConsistentTx();
        for (ExpectedValueCheckingStore store : expectedValuesByStore.keySet()) {
            Locker locker = store.getLocker();
            // Ignore locks on stores without a locker
            if (null == locker)
                continue;
            locker.checkLocks(lt);
        }
    }

    /**
     * Check that all expected values saved from earlier
     * {@link KeyColumnValueStore#acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)}
     * calls using this transaction.
     *
     * @throws org.janusgraph.diskstorage.BackendException
     */
    void checkAllExpectedValues() throws BackendException {
        for (final ExpectedValueCheckingStore store : expectedValuesByStore.keySet()) {
            final Map<KeyColumn, StaticBuffer> m = expectedValuesByStore.get(store);
            for (final KeyColumn kc : m.keySet()) {
                checkSingleExpectedValue(kc, m.get(kc), store);
            }
        }
    }

    /**
     * Signals the transaction that it has been used in a call to
     * {@link ExpectedValueCheckingStore#mutate(StaticBuffer, List, List, StoreTransaction)}
     * . This transaction can't be used in subsequent calls to
     * {@link ExpectedValueCheckingStore#acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)}
     * .
     * <p>
     * Calling this method at the appropriate time is handled automatically by
     * {@link ExpectedValueCheckingStore}. JanusGraph users don't need to call this
     * method by hand.
     */
    private void mutationStarted() {
        isMutationStarted = true;
    }

    private void lockedOn(ExpectedValueCheckingStore store) {
        final Map<KeyColumn, StaticBuffer> m = expectedValuesByStore.computeIfAbsent(store, k -> new HashMap<>());
    }

    private void checkSingleExpectedValue(final KeyColumn kc,
                                          final StaticBuffer ev, final ExpectedValueCheckingStore store) throws BackendException {
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
                                                final StaticBuffer ev, final ExpectedValueCheckingStore store) throws BackendException {
        final StaticBuffer nextBuf = BufferUtil.nextBiggerBuffer(kc.getColumn());
        KeySliceQuery ksq = new KeySliceQuery(kc.getKey(), kc.getColumn(), nextBuf);
        // Call getSlice on the wrapped store using the quorum+ consistency tx
        Iterable<Entry> actualEntries = store.getBackingStore().getSlice(ksq, strongConsistentTx);

        if (null == actualEntries)
            actualEntries = Collections.emptyList();

        /*
         * Discard any columns which do not exactly match kc.getColumn().
         *
         * For example, it's possible that the slice returned columns which for
         * which kc.getColumn() is a prefix.
         */
        actualEntries = Iterables.filter(actualEntries, input -> {
            if (!input.getColumn().equals(kc.getColumn())) {
                log.debug("Dropping entry {} (only accepting column {})", input, kc.getColumn());
                return false;
            }
            log.debug("Accepting entry {}", input);
            return true;
        });

        // Extract values from remaining Entry instances

        final Iterable<StaticBuffer> actualValues = Iterables.transform(actualEntries, e -> {
            final StaticBuffer actualCol = e.getColumnAs(StaticBuffer.STATIC_FACTORY);
            assert null != actualCol;
            assert null != kc.getColumn();
            assert 0 >= kc.getColumn().compareTo(actualCol);
            assert 0  > actualCol.compareTo(nextBuf);
            return e.getValueAs(StaticBuffer.STATIC_FACTORY);
        });

        final Iterable<StaticBuffer> expectedValues;

        if (null == ev) {
            expectedValues = Collections.emptyList();
        } else {
            expectedValues = Collections.singletonList(ev);
        }

        if (!Iterables.elementsEqual(expectedValues, actualValues)) {
            throw new PermanentLockingException(
                    "Expected value mismatch for " + kc + ": expected="
                            + expectedValues + " vs actual=" + actualValues + " (store=" + store.getName() + ")");
        }
    }

    private void deleteAllLocks() throws BackendException {
        for (ExpectedValueCheckingStore s : expectedValuesByStore.keySet()) {
            s.deleteLocks(this);
        }
    }
}
