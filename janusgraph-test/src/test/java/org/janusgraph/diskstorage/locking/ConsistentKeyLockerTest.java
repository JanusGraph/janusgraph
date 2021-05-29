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

package org.janusgraph.diskstorage.locking;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IMocksControl;
import org.easymock.LogicalOperator;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLockStatus;
import org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLocker;
import org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLockerSerializer;
import org.janusgraph.diskstorage.locking.consistentkey.ExpiredLockException;
import org.janusgraph.diskstorage.locking.consistentkey.LockCleanerService;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.KeyColumn;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.janusgraph.diskstorage.util.time.Timer;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.cmp;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLocker.LOCK_COL_END;
import static org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLocker.LOCK_COL_START;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class ConsistentKeyLockerTest {

    // Arbitrary literals -- the exact values assigned here are not intrinsically important
    private final ConsistentKeyLockerSerializer codec = new ConsistentKeyLockerSerializer();
    private final StaticBuffer defaultDataKey = BufferUtil.getIntBuffer(2);
    private final StaticBuffer defaultDataCol = BufferUtil.getIntBuffer(4);
    private final StaticBuffer defaultLockKey = codec.toLockKey(defaultDataKey, defaultDataCol);
    private final KeyColumn defaultLockID = new KeyColumn(defaultDataKey, defaultDataCol);

    private final StaticBuffer otherDataKey = BufferUtil.getIntBuffer(8);
    private final StaticBuffer otherDataCol = BufferUtil.getIntBuffer(16);
    private final StaticBuffer otherLockKey = codec.toLockKey(otherDataKey, otherDataCol);
    private final KeyColumn otherLockID = new KeyColumn(otherDataKey, otherDataCol);

    private final StaticBuffer defaultLockRid = new StaticArrayBuffer(new byte[]{(byte) 32});
    private final StaticBuffer otherLockRid = new StaticArrayBuffer(new byte[]{(byte) 64});
    private final StaticBuffer defaultLockVal = BufferUtil.getIntBuffer(0); // maybe refactor...

    private TestTrxImpl   defaultTx;
    private Configuration defaultTxCustomOpts;

    private TestTrxImpl   otherTx;
    private Configuration otherTxCustomOpts;

    private final Duration defaultWaitNS = Duration.ofNanos(100 * 1000 * 1000);
    private final Duration defaultExpireNS = Duration.ofNanos(30L * 1000 * 1000 * 1000);

    private IMocksControl ctrl;
    private IMocksControl relaxedCtrl;
    private Instant currentTimeNS;
    private TimestampProvider times;
    private KeyColumnValueStore store;
    private StoreManager manager;
    private LocalLockMediator<StoreTransaction> mediator;
    private LockerState<ConsistentKeyLockStatus> lockState;
    private ConsistentKeyLocker locker;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setupMocks() throws BackendException, NoSuchMethodException, SecurityException {
        currentTimeNS = Instant.EPOCH;

        /*
         * relaxedControl doesn't care about the order in which its mocks'
         * methods are called. This is useful for mocks of immutable objects.
         */
        relaxedCtrl = EasyMock.createControl();

        manager = relaxedCtrl.createMock(StoreManager.class);

        BaseTransactionConfig defaultTxCfg = relaxedCtrl.createMock(BaseTransactionConfig.class);
        defaultTx = new TestTrxImpl(defaultTxCfg);
        defaultTxCustomOpts = relaxedCtrl.createMock(Configuration.class);
        expect(defaultTxCfg.getGroupName()).andReturn("default").anyTimes();
        expect(defaultTxCfg.getCustomOptions()).andReturn(defaultTxCustomOpts).anyTimes();
        final Comparator<BaseTransactionConfig> defaultTxCfgChecker
            = (actual, ignored) -> actual.getCustomOptions() == defaultTxCustomOpts ? 0 : -1;
        expect(manager.beginTransaction(cmp(null, defaultTxCfgChecker, LogicalOperator.EQUAL)))
                .andAnswer(new IAnswer<StoreTransaction>() {
                    @Override
                    public StoreTransaction answer() throws Throwable {
                        return defaultTx.open();
                    }
                }).anyTimes();

        BaseTransactionConfig otherTxCfg = relaxedCtrl.createMock(BaseTransactionConfig.class);
        otherTx = new TestTrxImpl(otherTxCfg);
        otherTxCustomOpts = relaxedCtrl.createMock(Configuration.class);
        expect(otherTxCfg.getGroupName()).andReturn("other").anyTimes();
        expect(otherTxCfg.getCustomOptions()).andReturn(otherTxCustomOpts).anyTimes();
        final Comparator<BaseTransactionConfig> otherTxCfgChecker
            = (actual, ignored) -> actual.getCustomOptions() == otherTxCustomOpts ? 0 : -1;
        expect(manager.beginTransaction(cmp(null, otherTxCfgChecker, LogicalOperator.EQUAL)))
                .andAnswer(new IAnswer<StoreTransaction>() {
                    @Override
                    public StoreTransaction answer() throws Throwable {
                        return otherTx.open();
                    }
                }).anyTimes();


        /*
         * ctrl requires that the complete, order-sensitive sequence of actual
         * method invocations on its mocks exactly match the expected sequence
         * hard-coded into each test method. Either an unexpected actual
         * invocation or expected invocation that fails to actually occur will
         * cause a test failure.
         */
        ctrl = EasyMock.createStrictControl();
        Method timeInNativeUnit = FakeTimestampProvider.class.getMethod("getTime");

        Method sleepPast = FakeTimestampProvider.class.getMethod("sleepPast", Instant.class);

        times = EasyMock.createMockBuilder(FakeTimestampProvider.class).addMockedMethod(timeInNativeUnit)
                .addMockedMethod(sleepPast).createMock(ctrl);
        store = ctrl.createMock(KeyColumnValueStore.class);
        mediator = ctrl.createMock(LocalLockMediator.class);
        lockState = ctrl.createMock(LockerState.class);
        ctrl.replay();
        locker = getDefaultBuilder().build();
        ctrl.verify();
        ctrl.reset();

        expect(defaultTxCfg.getTimestampProvider()).andReturn(times).anyTimes();
        expect(otherTxCfg.getTimestampProvider()).andReturn(times).anyTimes();

        relaxedCtrl.replay();
    }

    @AfterEach
    public void verifyMocks() {
        ctrl.verify();
        relaxedCtrl.verify();

        assertFalse(defaultTx.isOpen(), "Transaction leak found: openCount=" + defaultTx.getOpenCount() + ", commitCount=" + defaultTx.getCommitCount() + ", rollbackCount=" + defaultTx.getRollbackCount());
        assertFalse(otherTx.isOpen(), "Transaction leak found: openCount=" + otherTx.getOpenCount() + ", commitCount=" + otherTx.getCommitCount() + ", rollbackCount=" + otherTx.getRollbackCount());
    }

    /**
     * Test a single lock using stub objects. Doesn't test unlock ("leaks" the
     * lock, but since it's backed by stubs, it doesn't matter).
     *
     * @throws org.janusgraph.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testWriteLockInSimplestCase() throws BackendException {

        // Check to see whether the lock was already written before anything else
        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        // Now lock it locally to block other threads in the process
        recordSuccessfulLocalLock();
        // Write a lock claim column to the store
        LockInfo li = recordSuccessfulLockWrite(1, ChronoUnit.NANOS, null);
        // Update the expiration timestamp of the local (thread-level) lock
        recordSuccessfulLocalLock(li.tsNS);
        // Store the taken lock's key, column, and timestamp in the lockState map
        lockState.take(eq(defaultTx), eq(defaultLockID), eq(li.stat));

        ctrl.replay();

        locker.writeLock(defaultLockID, defaultTx); // SUT
    }

    /**
     * Test locker when first attempt to write to the store takes too long (but
     * succeeds). Expected behavior is to call mutate on the store, adding a
     * column with a new timestamp and deleting the column with the old
     * (too-slow-to-write) timestamp.
     *
     * @throws org.janusgraph.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testWriteLockRetriesAfterOneStoreTimeout() throws BackendException {
        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        recordSuccessfulLocalLock();
        StaticBuffer firstCol = recordSuccessfulLockWrite(5, ChronoUnit.SECONDS, null).col; // too slow
        LockInfo secondLI = recordSuccessfulLockWrite(1, ChronoUnit.NANOS, firstCol); // plenty fast
        recordSuccessfulLocalLock(secondLI.tsNS);
        lockState.take(eq(defaultTx), eq(defaultLockID), eq(secondLI.stat));
        ctrl.replay();

        locker.writeLock(defaultLockID, defaultTx); // SUT
    }

    /**
     * Test locker when all three attempts to write a lock succeed but take
     * longer than the wait limit. We expect the locker to delete all three
     * columns that it wrote and locally unlock the KeyColumn, then emit an
     * exception.
     *
     * @throws org.janusgraph.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testWriteLockThrowsExceptionAfterMaxStoreTimeouts() throws BackendException {
        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        recordSuccessfulLocalLock();
        StaticBuffer firstCol = recordSuccessfulLockWrite(5, ChronoUnit.SECONDS, null).col;
        StaticBuffer secondCol = recordSuccessfulLockWrite(5, ChronoUnit.SECONDS, firstCol).col;
        StaticBuffer thirdCol = recordSuccessfulLockWrite(5, ChronoUnit.SECONDS, secondCol).col;
        recordSuccessfulLockDelete(thirdCol);
        recordSuccessfulLocalUnlock();
        ctrl.replay();

        BackendException expected = null;
        try {
            locker.writeLock(defaultLockID, defaultTx); // SUT
        } catch (TemporaryBackendException e) {
            expected = e;
        }
        assertNotNull(expected);
    }

    /**
     * Test that the first {@link org.janusgraph.diskstorage.PermanentBackendException} thrown by the
     * locker's store causes it to attempt to delete outstanding lock writes and
     * then emit the exception without retrying.
     *
     * @throws org.janusgraph.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testWriteLockDiesOnPermanentStorageException() throws BackendException {
        PermanentBackendException errOnFire = new PermanentBackendException("Storage cluster is on fire");

        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        recordSuccessfulLocalLock();
        StaticBuffer lockCol = recordExceptionLockWrite(errOnFire);
        recordSuccessfulLockDelete(lockCol);
        recordSuccessfulLocalUnlock();
        ctrl.replay();

        BackendException expected = null;
        try {
            locker.writeLock(defaultLockID, defaultTx); // SUT
        } catch (PermanentLockingException e) {
            expected = e;
        }
        assertNotNull(expected);
        assertEquals(errOnFire, expected.getCause());
    }

    /**
     * Test the locker retries a lock write after the initial store mutation
     * fails with a {@link org.janusgraph.diskstorage.TemporaryBackendException}. The retry should both
     * attempt to write the and delete the failed mutation column.
     *
     * @throws org.janusgraph.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testWriteLockRetriesOnTemporaryStorageException() throws BackendException {
        TemporaryBackendException tse = new TemporaryBackendException("Storage cluster is waking up");

        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        recordSuccessfulLocalLock();
        StaticBuffer firstCol = recordExceptionLockWrite(tse);
        LockInfo secondLI = recordSuccessfulLockWrite(1, ChronoUnit.NANOS, firstCol);
        recordSuccessfulLocalLock(secondLI.tsNS);
        lockState.take(eq(defaultTx), eq(defaultLockID), eq(secondLI.stat));
        ctrl.replay();

        locker.writeLock(defaultLockID, defaultTx); // SUT
    }

    /**
     * Test that a failure to lock locally results in a {@link TemporaryLockingException}
     *
     * @throws org.janusgraph.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testWriteLockFailsOnLocalContention() throws BackendException {

        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        recordFailedLocalLock();
        ctrl.replay();

        PermanentLockingException le = null;
        try {
            locker.writeLock(defaultLockID, defaultTx); // SUT
        } catch (PermanentLockingException e) {
            le = e;
        }
        assertNotNull(le);
    }

    /**
     * Claim a lock without errors using {@code defaultTx}, the check that
     * {@code otherTx} can't claim it, instead throwing a
     * TemporaryLockingException
     *
     * @throws org.janusgraph.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testWriteLockDetectsMultiTxContention() throws BackendException {
        // defaultTx

        // Check to see whether the lock was already written before anything else
        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        // Now lock it locally to block other threads in the process
        recordSuccessfulLocalLock();
        // Write a lock claim column to the store
        LockInfo li = recordSuccessfulLockWrite(1, ChronoUnit.NANOS, null);
        // Update the expiration timestamp of the local (thread-level) lock
        recordSuccessfulLocalLock(li.tsNS);
        // Store the taken lock's key, column, and timestamp in the lockState map
        lockState.take(eq(defaultTx), eq(defaultLockID), eq(li.stat));

        // otherTx
        // Check to see whether the lock was already written before anything else
        expect(lockState.has(otherTx, defaultLockID)).andReturn(false);
        // Now try to take the lock but fail because defaultTX has it
        recordFailedLocalLock(otherTx);
        ctrl.replay();

        locker.writeLock(defaultLockID, defaultTx); // SUT

        PermanentLockingException le = null;
        try {
            locker.writeLock(defaultLockID, otherTx); // SUT
        } catch (PermanentLockingException e) {
            le = e;
        }
        assertNotNull(le);
    }

    /**
     * Test that multiple calls to
     * {@link ConsistentKeyLocker#writeLock(KeyColumn, StoreTransaction)} with
     * the same arguments have no effect after the first call (until
     * {@link ConsistentKeyLocker#deleteLocks(StoreTransaction)} is called).
     *
     * @throws org.janusgraph.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testWriteLockIdempotence() throws BackendException {
        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        recordSuccessfulLocalLock();
        LockInfo li = recordSuccessfulLockWrite(1, ChronoUnit.NANOS, null);
        recordSuccessfulLocalLock(li.tsNS);
        lockState.take(eq(defaultTx), eq(defaultLockID), eq(li.stat));
        ctrl.replay();

        locker.writeLock(defaultLockID, defaultTx);

        ctrl.verify();
        ctrl.reset();
        expect(lockState.has(defaultTx, defaultLockID)).andReturn(true);
        ctrl.replay();

        locker.writeLock(defaultLockID, defaultTx);
    }

    /**
     * Test a single checking a single lock under optimal conditions (no
     * timeouts, no errors)
     *
     * @throws org.janusgraph.diskstorage.BackendException     shouldn't happen
     * @throws InterruptedException shouldn't happen
     */
    @Test
    public void testCheckLocksInSimplestCase() throws BackendException, InterruptedException {
        // Fake a pre-existing lock
        final ConsistentKeyLockStatus ls = makeStatusNow();
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, ls));
        currentTimeNS = currentTimeNS.plusSeconds(10);
        // Checker should compare the fake lock's timestamp to the current time
        expectSleepAfterWritingLock(ls);
        // Expect a store getSlice() and return the fake lock's column and value
        recordLockGetSliceAndReturnSingleEntry(
                StaticArrayEntry.of(
                        codec.toLockCol(ls.getWriteTimestamp(), defaultLockRid, times),
                        defaultLockVal));
        ctrl.replay();

        locker.checkLocks(defaultTx);
    }

    private void expectSleepAfterWritingLock(ConsistentKeyLockStatus ls) throws InterruptedException {
        expect(times.sleepPast(ls.getWriteTimestamp().plus(defaultWaitNS))).andReturn(currentTimeNS);
    }

    /**
     * A transaction that writes a lock, waits past expiration, and attempts
     * to check locks should receive an {@code ExpiredLockException} during
     * the check stage.
     *
     * @throws org.janusgraph.diskstorage.BackendException     shouldn't happen
     * @throws InterruptedException
     */
    @Test
    public void testCheckOwnExpiredLockThrowsException() throws BackendException, InterruptedException {
        // Fake a pre-existing lock that's long since expired
        final ConsistentKeyLockStatus expired = makeStatusNow();
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, expired));
        // pretend a huge multiple of the expiration time has passed
        currentTimeNS = currentTimeNS.plus(100, ChronoUnit.DAYS);

        // Checker should compare the fake lock's timestamp to the current time
        expectSleepAfterWritingLock(expired);

        // Checker must slice the store; we return the single expired lock column
        recordLockGetSliceAndReturnSingleEntry(
                StaticArrayEntry.of(
                        codec.toLockCol(expired.getWriteTimestamp(), defaultLockRid, times),
                        defaultLockVal));

        ctrl.replay();
        ExpiredLockException ele = null;
        try {
            locker.checkLocks(defaultTx);
        } catch (ExpiredLockException e) {
            ele = e;
        }
        assertNotNull(ele);
    }

    /**
     * A transaction that detects expired locks from other transactions, or from
     * its own transaction but with a different timestamp than the one currently
     * stored in memory by the transaction (presumably from an earlier attempt),
     * should be ignored.
     *
     * @throws org.janusgraph.diskstorage.BackendException     shouldn't happen
     * @throws InterruptedException
     */
    @Test
    public void testCheckLocksIgnoresOtherExpiredLocks() throws BackendException, InterruptedException {
        // Fake a pre-existing lock from a different tx that's long since expired
        final ConsistentKeyLockStatus otherExpired = makeStatusNow();

        // Fake a pre-existing lock from our tx
        final ConsistentKeyLockStatus ownExpired = makeStatusNow();

        // pretend a huge multiple of the expiration time has passed
        currentTimeNS = currentTimeNS.plus(100, ChronoUnit.DAYS);

        // Create a still-valid lock belonging to the default tx
        final ConsistentKeyLockStatus recent = makeStatusNow();
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, recent));
        currentTimeNS = currentTimeNS.plusMillis(1);

        expectSleepAfterWritingLock(recent);

        // Checker must slice the store; return both of the expired claims and the one active claim
        recordLockGetSlice(StaticArrayEntryList.of(
                StaticArrayEntry.of(
                        codec.toLockCol(otherExpired.getWriteTimestamp(), otherLockRid, times),
                        defaultLockVal),
                StaticArrayEntry.of(
                        codec.toLockCol(ownExpired.getWriteTimestamp(), defaultLockRid, times),
                        defaultLockVal),
                StaticArrayEntry.of(
                        codec.toLockCol(recent.getWriteTimestamp(), defaultLockRid, times),
                        defaultLockVal)
        ));

        ctrl.replay();

        locker.checkLocks(defaultTx);
    }

    /**
     * Each written lock should be checked at most once. Test this by faking a
     * single previously written lock using mocks and stubs and then calling
     * checkLocks() twice. The second call should have no effect.
     *
     * @throws InterruptedException shouldn't happen
     * @throws org.janusgraph.diskstorage.BackendException     shouldn't happen
     */
    @Test
    public void testCheckLocksIdempotence() throws InterruptedException, BackendException {
        // Fake a pre-existing valid lock
        final ConsistentKeyLockStatus ls = makeStatusNow();
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, ls));
        currentTimeNS = currentTimeNS.plusSeconds(10);

        expectSleepAfterWritingLock(ls);

        final StaticBuffer lc = codec.toLockCol(ls.getWriteTimestamp(), defaultLockRid, times);
        recordLockGetSliceAndReturnSingleEntry(StaticArrayEntry.of(lc, defaultLockVal));
        ctrl.replay();

        locker.checkLocks(defaultTx);

        ctrl.verify();
        ctrl.reset();
        // Return the faked lock in a map of size 1
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, ls));
        ctrl.replay();
        // At this point, checkLocks() should see that the single lock in the
        // map returned above has already been checked and return immediately

        locker.checkLocks(defaultTx);
    }

    /**
     * If the checker reads its own lock column preceded by a lock column from
     * another rid with an earlier timestamp and the timestamps on both columns
     * are unexpired, then the checker must throw a TemporaryLockingException.
     *
     * @throws InterruptedException shouldn't happen
     * @throws org.janusgraph.diskstorage.BackendException
     *         shouldn't happen (we expect a TemporaryLockingException but we catch and swallow it)
     */
    @Test
    public void testCheckLocksFailsWithSeniorClaimsByOthers() throws InterruptedException, BackendException {
        // Make a pre-existing valid lock by some other tx (written by another process)
        StaticBuffer otherSeniorLockCol = codec.toLockCol(currentTimeNS, otherLockRid, times);
        currentTimeNS = currentTimeNS.plusNanos(1);
        // Expect checker to fetch locks for defaultTx; return just our own lock (not the other guy's)
        StaticBuffer ownJuniorLockCol = codec.toLockCol(currentTimeNS, defaultLockRid, times);
        ConsistentKeyLockStatus ownJuniorLS = makeStatusNow();
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, ownJuniorLS));

        currentTimeNS = currentTimeNS.plusSeconds(10);

        // Return defaultTx's lock in a map when requested
        expectSleepAfterWritingLock(ownJuniorLS);

        // When the checker slices the store, return the senior lock col by a
        // foreign tx and the junior lock col by defaultTx (in that order)
        recordLockGetSlice(StaticArrayEntryList.of(
                StaticArrayEntry.of(otherSeniorLockCol, defaultLockVal),
                StaticArrayEntry.of(ownJuniorLockCol, defaultLockVal)));

        ctrl.replay();

        TemporaryLockingException tle = null;
        try {
            locker.checkLocks(defaultTx);
        } catch (TemporaryLockingException e) {
            tle = e;
        }
        assertNotNull(tle);
    }

    /**
     * When the checker retrieves its own lock column followed by a lock column
     * with a later timestamp (both with unexpired timestamps), it should
     * consider the lock successfully checked.
     *
     * @throws InterruptedException shouldn't happen
     * @throws org.janusgraph.diskstorage.BackendException     shouldn't happen
     */
    @Test
    public void testCheckLocksSucceedsWithJuniorClaimsByOthers() throws InterruptedException, BackendException {
        // Expect checker to fetch locks for defaultTx; return just our own lock (not the other guy's)
        StaticBuffer ownSeniorLockCol = codec.toLockCol(currentTimeNS, defaultLockRid, times);
        ConsistentKeyLockStatus ownSeniorLS = makeStatusNow();
        currentTimeNS = currentTimeNS.plusNanos(1);
        // Make junior lock
        StaticBuffer otherJuniorLockCol = codec.toLockCol(currentTimeNS, otherLockRid, times);
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, ownSeniorLS));

        currentTimeNS = currentTimeNS.plusSeconds(10);

        // Return defaultTx's lock in a map when requested
        expectSleepAfterWritingLock(ownSeniorLS);

        // When the checker slices the store, return the senior lock col by a
        // foreign tx and the junior lock col by defaultTx (in that order)
        recordLockGetSlice(StaticArrayEntryList.of(
                StaticArrayEntry.of(ownSeniorLockCol, defaultLockVal),
                StaticArrayEntry.of(otherJuniorLockCol, defaultLockVal)));

        ctrl.replay();

        locker.checkLocks(defaultTx);
    }

    /**
     * If the checker retrieves a timestamp-ordered list of columns, where the
     * list starts with an unbroken series of columns with the checker's rid but
     * differing timestamps, then consider the lock successfully checked if the
     * checker's expected timestamp occurs anywhere in that series of columns.
     * <p/>
     * This relaxation of the normal checking rules only triggers when either
     * writeLock(...) issued mutate calls that appeared to fail client-side but
     * which actually succeeded (e.g. hinted handoff or timeout)
     *
     * @throws InterruptedException shouldn't happen
     * @throws org.janusgraph.diskstorage.BackendException     shouldn't happen
     */
    @Test
    public void testCheckLocksSucceedsWithSeniorAndJuniorClaimsBySelf() throws InterruptedException, BackendException {
        // Setup three lock columns differing only in timestamp
        StaticBuffer myFirstLockCol = codec.toLockCol(currentTimeNS, defaultLockRid, times);
        currentTimeNS = currentTimeNS.plusNanos(1);
        StaticBuffer mySecondLockCol = codec.toLockCol(currentTimeNS, defaultLockRid, times);
        ConsistentKeyLockStatus mySecondLS = makeStatusNow();
        currentTimeNS = currentTimeNS.plusNanos(1);
        StaticBuffer myThirdLockCol = codec.toLockCol(currentTimeNS, defaultLockRid, times);
        currentTimeNS = currentTimeNS.plusNanos(1);

        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, mySecondLS));

        // Return defaultTx's second lock in a map when requested
        currentTimeNS = currentTimeNS.plusSeconds(10);
        expectSleepAfterWritingLock(mySecondLS);

        // When the checker slices the store, return the senior lock col by a
        // foreign tx and the junior lock col by defaultTx (in that order)
        recordLockGetSlice(StaticArrayEntryList.of(
                StaticArrayEntry.of(myFirstLockCol, defaultLockVal),
                StaticArrayEntry.of(mySecondLockCol, defaultLockVal),
                StaticArrayEntry.of(myThirdLockCol, defaultLockVal)));

        ctrl.replay();

        locker.checkLocks(defaultTx);
    }

    /**
     * The checker should retry getSlice() in the face of a
     * TemporaryStorageException so long as the number of exceptional
     * getSlice()s is fewer than the lock retry count. The retry count applies
     * on a per-lock basis.
     *
     * @throws org.janusgraph.diskstorage.BackendException     shouldn't happen
     * @throws InterruptedException shouldn't happen
     */
    @Test
    public void testCheckLocksRetriesAfterSingleTemporaryStorageException() throws BackendException, InterruptedException {
        // Setup one lock column
        StaticBuffer lockCol = codec.toLockCol(currentTimeNS, defaultLockRid, times);
        ConsistentKeyLockStatus lockStatus = makeStatusNow();
        currentTimeNS = currentTimeNS.plusNanos(1);

        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, lockStatus));

        expectSleepAfterWritingLock(lockStatus);

        // First getSlice will fail
        TemporaryBackendException tse = new TemporaryBackendException("Storage cluster will be right back");
        recordExceptionalLockGetSlice(tse);

        // Second getSlice will succeed
        recordLockGetSliceAndReturnSingleEntry(StaticArrayEntry.of(lockCol, defaultLockVal));

        ctrl.replay();

        locker.checkLocks(defaultTx);

        // TODO run again with two locks instead of one and show that the retry count applies on a per-lock basis
    }

    /**
     * The checker will throw a TemporaryStorageException if getSlice() throws
     * fails with a TemporaryStorageException as many times as there are
     * configured lock retries.
     *
     * @throws InterruptedException shouldn't happen
     * @throws org.janusgraph.diskstorage.BackendException     shouldn't happen
     */
    @Test
    public void testCheckLocksThrowsExceptionAfterMaxTemporaryStorageExceptions() throws InterruptedException, BackendException {
        // Setup a LockStatus for defaultLockID
        ConsistentKeyLockStatus lockStatus = makeStatusNow();
        currentTimeNS = currentTimeNS.plusNanos(1);

        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, lockStatus));

        expectSleepAfterWritingLock(lockStatus);

        // Three successive getSlice calls, each throwing a distinct TSE
        recordExceptionalLockGetSlice(new TemporaryBackendException("Storage cluster is having me-time"));
        recordExceptionalLockGetSlice(new TemporaryBackendException("Storage cluster is in a dissociative fugue state"));
        recordExceptionalLockGetSlice(new TemporaryBackendException("Storage cluster has gone to Prague to find itself"));

        ctrl.replay();

        TemporaryBackendException tse = null;
        try {
            locker.checkLocks(defaultTx);
        } catch (TemporaryBackendException e) {
            tse = e;
        }
        assertNotNull(tse);
    }

    /**
     * A single PermanentStorageException on getSlice() for a single lock is
     * sufficient to make the method return immediately (regardless of whether
     * other locks are waiting to be checked).
     *
     * @throws InterruptedException shouldn't happen
     * @throws org.janusgraph.diskstorage.BackendException     shouldn't happen
     */
    @Test
    public void testCheckLocksDiesOnPermanentStorageException() throws InterruptedException, BackendException {
        // Setup a LockStatus for defaultLockID
        ConsistentKeyLockStatus lockStatus = makeStatusNow();
        currentTimeNS = currentTimeNS.plusNanos(1);

        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, lockStatus));

        expectSleepAfterWritingLock(lockStatus);

        // First and only getSlice call throws a PSE
        recordExceptionalLockGetSlice(
                new PermanentBackendException("Connection to storage cluster failed: peer is an IPv6 toaster"));

        ctrl.replay();

        PermanentBackendException pse = null;
        try {
            locker.checkLocks(defaultTx);
        } catch (PermanentBackendException e) {
            pse = e;
        }
        assertNotNull(pse);
    }

    /**
     * The lock checker should do nothing when passed a transaction for which it
     * holds no locks.
     *
     * @throws org.janusgraph.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testCheckLocksDoesNothingForUnrecognizedTransaction() throws BackendException {
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of());
        ctrl.replay();
        locker.checkLocks(defaultTx);
    }

    /**
     * Delete a single lock without any timeouts, errors, etc.
     *
     * @throws org.janusgraph.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testDeleteLocksInSimplestCase() throws BackendException {
        // Setup a LockStatus for defaultLockID
        final ConsistentKeyLockStatus lockStatus = makeStatusNow();
        currentTimeNS = currentTimeNS.plusNanos(1);

        @SuppressWarnings("serial")
        Map<KeyColumn, ConsistentKeyLockStatus> expectedMap = new HashMap<KeyColumn, ConsistentKeyLockStatus>() {{
            put(defaultLockID, lockStatus);
        }};
        expect(lockState.getLocksForTx(defaultTx)).andReturn(expectedMap);

        List<StaticBuffer> deletions
                = ImmutableList.of(codec.toLockCol(lockStatus.getWriteTimestamp(), defaultLockRid, times));
        expect(times.getTime()).andReturn(currentTimeNS);
        store.mutate(eq(defaultLockKey), eq(ImmutableList.of()), eq(deletions), eq(defaultTx));
        expect(mediator.unlock(defaultLockID, defaultTx)).andReturn(true);

        ctrl.replay();

        locker.deleteLocks(defaultTx);
    }

    /**
     * Delete two locks without any timeouts, errors, etc.
     *
     * @throws org.janusgraph.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testDeleteLocksOnTwoLocks() throws BackendException {
        ConsistentKeyLockStatus defaultLS = makeStatusNow();
        currentTimeNS = currentTimeNS.plusNanos(1);
        ConsistentKeyLockStatus otherLS = makeStatusNow();
        currentTimeNS = currentTimeNS.plusNanos(1);

        // Expect a call for defaultTx's locks and return two
        Map<KeyColumn, ConsistentKeyLockStatus> expectedMap = Maps.newLinkedHashMap();
        expectedMap.put(defaultLockID, defaultLS);
        expectedMap.put(otherLockID, otherLS);
        expect(lockState.getLocksForTx(defaultTx)).andReturn(expectedMap);

        expectLockDeleteSuccessfully(defaultLockID, defaultLockKey, defaultLS);

        expectLockDeleteSuccessfully(otherLockID, otherLockKey, otherLS);

        ctrl.replay();

        locker.deleteLocks(defaultTx);
    }

    private void expectLockDeleteSuccessfully(KeyColumn lockID, StaticBuffer lockKey,
                                              ConsistentKeyLockStatus lockStatus) throws BackendException {
        expectDeleteLock(lockID, lockKey, lockStatus);
    }

    private void expectDeleteLock(KeyColumn lockID, StaticBuffer lockKey, ConsistentKeyLockStatus lockStatus,
                                  BackendException... backendFailures) throws BackendException {
        List<StaticBuffer> deletions = ImmutableList.of(codec.toLockCol(lockStatus.getWriteTimestamp(), defaultLockRid, times));
        expect(times.getTime()).andReturn(currentTimeNS);
        store.mutate(eq(lockKey), eq(ImmutableList.of()), eq(deletions), eq(defaultTx));
        int backendExceptionsThrown = 0;
        for (BackendException e : backendFailures) {
            expectLastCall().andThrow(e);
            if (e instanceof PermanentBackendException) {
                break;
            }
            backendExceptionsThrown++;
            int maxTemporaryStorageExceptions = 3;
            if (backendExceptionsThrown < maxTemporaryStorageExceptions) {
                expect(times.getTime()).andReturn(currentTimeNS);
                store.mutate(eq(lockKey), eq(ImmutableList.of()), eq(deletions), eq(defaultTx));
            }
        }
        expect(mediator.unlock(lockID, defaultTx)).andReturn(true);
    }

    /**
     * Lock deletion should retry if the first store mutation throws a temporary
     * exception.
     *
     * @throws org.janusgraph.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testDeleteLocksRetriesOnTemporaryStorageException() throws BackendException {
        ConsistentKeyLockStatus defaultLS = makeStatusNow();
        currentTimeNS = currentTimeNS.plusNanos(1);
        expect(lockState.getLocksForTx(defaultTx))
                .andReturn(Maps.newLinkedHashMap(ImmutableMap.of(defaultLockID, defaultLS)));
        expectDeleteLock(defaultLockID, defaultLockKey, defaultLS,
                new TemporaryBackendException("Storage cluster is backlogged"));
        ctrl.replay();

        locker.deleteLocks(defaultTx);
    }

    /**
     * If lock deletion exceeds the temporary exception retry count when trying
     * to delete a lock, it should move onto the next lock rather than returning
     * and potentially leaving the remaining locks alone (not deleted).
     *
     * @throws org.janusgraph.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testDeleteLocksSkipsToNextLockAfterMaxTemporaryStorageExceptions() throws BackendException {
        ConsistentKeyLockStatus defaultLS = makeStatusNow();
        currentTimeNS = currentTimeNS.plusNanos(1);
        expect(lockState.getLocksForTx(defaultTx))
                .andReturn(Maps.newLinkedHashMap(ImmutableMap.of(defaultLockID, defaultLS)));

        expectDeleteLock(defaultLockID, defaultLockKey, defaultLS,
                new TemporaryBackendException("Storage cluster is busy"),
                new TemporaryBackendException("Storage cluster is busier"),
                new TemporaryBackendException("Storage cluster has reached peak business"));

        ctrl.replay();

        locker.deleteLocks(defaultTx);
    }

    /**
     * Same as
     * {@link #testDeleteLocksSkipsToNextLockAfterMaxTemporaryStorageExceptions()}
     * , except instead of exceeding the temporary exception retry count on a
     * lock, that lock throws a single permanent exception.
     *
     * @throws org.janusgraph.diskstorage.BackendException should not happen
     */
    @Test
    public void testDeleteLocksSkipsToNextLockOnPermanentStorageException() throws BackendException {
        ConsistentKeyLockStatus defaultLS = makeStatusNow();
        currentTimeNS = currentTimeNS.plusNanos(1);
        expect(lockState.getLocksForTx(defaultTx))
                .andReturn(Maps.newLinkedHashMap(ImmutableMap.of(defaultLockID, defaultLS)));

        expectDeleteLock(defaultLockID, defaultLockKey, defaultLS,
                new PermanentBackendException("Storage cluster has been destroyed by a tornado"));

        ctrl.replay();

        locker.deleteLocks(defaultTx);
    }

    /**
     * Deletion should remove previously written locks regardless of whether
     * they were ever checked; this method fakes and verifies deletion on a
     * single unchecked lock
     *
     * @throws org.janusgraph.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testDeleteLocksDeletesUncheckedLocks() throws BackendException {
        ConsistentKeyLockStatus defaultLS = makeStatusNow();
        assertFalse(defaultLS.isChecked());
        currentTimeNS = currentTimeNS.plusNanos(1);

        // Expect a call for defaultTx's locks and the checked one
        expect(lockState.getLocksForTx(defaultTx))
                .andReturn(Maps.newLinkedHashMap(ImmutableMap.of(defaultLockID, defaultLS)));

        expectLockDeleteSuccessfully(defaultLockID, defaultLockKey, defaultLS);

        ctrl.replay();

        locker.deleteLocks(defaultTx);
    }

    /**
     * When delete is called multiple times with no intervening write or check
     * calls, all calls after the first should have no effect.
     *
     * @throws org.janusgraph.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testDeleteLocksIdempotence() throws BackendException {
        // Setup a LockStatus for defaultLockID
        ConsistentKeyLockStatus lockStatus = makeStatusNow();
        currentTimeNS = currentTimeNS.plusNanos(1);

        expect(lockState.getLocksForTx(defaultTx))
                .andReturn(Maps.newLinkedHashMap(ImmutableMap.of(defaultLockID, lockStatus)));

        expectLockDeleteSuccessfully(defaultLockID, defaultLockKey, lockStatus);

        ctrl.replay();

        locker.deleteLocks(defaultTx);

        ctrl.verify();
        ctrl.reset();
        expect(lockState.getLocksForTx(defaultTx)).andReturn(Maps.newLinkedHashMap(ImmutableMap.of()));
        ctrl.replay();
        locker.deleteLocks(defaultTx);
    }

    /**
     * Delete should do nothing when passed a transaction for which it holds no
     * locks.
     *
     * @throws org.janusgraph.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testDeleteLocksDoesNothingForUnrecognizedTransaction() throws BackendException {
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of());
        ctrl.replay();
        locker.deleteLocks(defaultTx);
    }

    /**
     * Checking locks when the expired lock cleaner is enabled should trigger
     * one call to the LockCleanerService.
     *
     * @throws org.janusgraph.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testCleanExpiredLock() throws BackendException, InterruptedException {
        LockCleanerService mockCleaner = ctrl.createMock(LockCleanerService.class);
        ctrl.replay();
        Locker altLocker = getDefaultBuilder().customCleaner(mockCleaner).build();
        ctrl.verify();
        ctrl.reset();

        final ConsistentKeyLockStatus expired = makeStatusNow();
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, expired));
        // pretend a huge multiple of the expiration time has passed
        currentTimeNS = currentTimeNS.plus(100, ChronoUnit.DAYS);

        // Checker should compare the fake lock's timestamp to the current time
        expect(times.sleepPast(expired.getWriteTimestamp().plus(defaultWaitNS))).andReturn(currentTimeNS);

        // Checker must slice the store; we return the single expired lock column
        recordLockGetSliceAndReturnSingleEntry(
                StaticArrayEntry.of(
                        codec.toLockCol(expired.getWriteTimestamp(), defaultLockRid, times),
                        defaultLockVal));

        // Checker must attempt to cleanup expired lock
        mockCleaner.clean(eq(defaultLockID), eq(currentTimeNS.minus(defaultExpireNS)), eq(defaultTx));
        expectLastCall().once();

        ctrl.replay();
        TemporaryLockingException ple = null;
        try {
            altLocker.checkLocks(defaultTx);
        } catch (TemporaryLockingException e) {
            ple = e;
        }
        assertNotNull(ple);
    }

    /*
     * Helpers
     */

    public static class LockInfo {

        private final Instant tsNS;
        private final ConsistentKeyLockStatus stat;
        private final StaticBuffer col;

        public LockInfo(Instant tsNS, ConsistentKeyLockStatus stat,
                        StaticBuffer col) {
            this.tsNS = tsNS;
            this.stat = stat;
            this.col = col;
        }
    }

    private ConsistentKeyLockStatus makeStatus(Instant currentNS) {
        return new ConsistentKeyLockStatus(
                currentTimeNS,
                Instant.EPOCH.plus(defaultExpireNS));
    }

    private ConsistentKeyLockStatus makeStatusNow() {
        return makeStatus(currentTimeNS);
    }

    private LockInfo recordSuccessfulLockWrite(long duration, TemporalUnit tu, StaticBuffer del) throws BackendException {
        return recordSuccessfulLockWrite(defaultTx, duration, tu, del);
    }

    private LockInfo recordSuccessfulLockWrite(StoreTransaction tx, long duration, TemporalUnit tu,
                                               StaticBuffer del) throws BackendException {
        currentTimeNS = currentTimeNS.plusNanos(1);
        expect(times.getTime()).andReturn(currentTimeNS);

        final Instant lockNS = currentTimeNS;

        StaticBuffer lockCol = codec.toLockCol(lockNS, defaultLockRid, times);
        Entry add = StaticArrayEntry.of(lockCol, defaultLockVal);

        StaticBuffer k = eq(defaultLockKey);
        final List<Entry> adds = eq(Collections.singletonList(add));
        final List<StaticBuffer> deletions;
        if (null != del) {
            deletions = eq(Collections.singletonList(del));
        } else {
            deletions = eq(ImmutableList.of());
        }

        store.mutate(k, adds, deletions, eq(tx));
        expectLastCall().once();

        currentTimeNS = currentTimeNS.plus(duration, tu);


        expect(times.getTime()).andReturn(currentTimeNS);

        ConsistentKeyLockStatus status = new ConsistentKeyLockStatus(
                lockNS,
                lockNS.plus(defaultExpireNS));

        return new LockInfo(lockNS, status, lockCol);
    }

    private StaticBuffer recordExceptionLockWrite(Throwable t) throws BackendException {
        currentTimeNS = currentTimeNS.plusNanos(1);
        expect(times.getTime()).andReturn(currentTimeNS);
        StaticBuffer lockCol = codec.toLockCol(currentTimeNS, defaultLockRid, times);

        Entry add = StaticArrayEntry.of(lockCol, defaultLockVal);

        StaticBuffer k = eq(defaultLockKey);
        final List<Entry> adds = eq(Collections.singletonList(add));
        final List<StaticBuffer> deletions;
        deletions = eq(ImmutableList.of());
        store.mutate(k, adds, deletions, eq(defaultTx));
        expectLastCall().andThrow(t);

        currentTimeNS = currentTimeNS.plus(1, ChronoUnit.NANOS);
        expect(times.getTime()).andReturn(currentTimeNS);

        return lockCol;
    }

    private void recordSuccessfulLockDelete(StaticBuffer del) throws BackendException {
        currentTimeNS = currentTimeNS.plusNanos(1);
        expect(times.getTime()).andReturn(currentTimeNS);
        store.mutate(eq(defaultLockKey), eq(ImmutableList.of()), eq(Collections.singletonList(del)), eq(defaultTx));

        currentTimeNS = currentTimeNS.plus(1, ChronoUnit.NANOS);
        expect(times.getTime()).andReturn(currentTimeNS);
    }

    private void recordSuccessfulLocalLock() {
        recordSuccessfulLocalLock(defaultTx);
    }

    private void recordSuccessfulLocalLock(StoreTransaction tx) {
        currentTimeNS = currentTimeNS.plusNanos(1);
        expect(times.getTime()).andReturn(currentTimeNS);
        expect(mediator.lock(defaultLockID, tx, currentTimeNS.plus(defaultExpireNS))).andReturn(true);
    }

    private void recordSuccessfulLocalLock(Instant ts) {
        recordSuccessfulLocalLock(defaultTx, ts);
    }

    private void recordSuccessfulLocalLock(StoreTransaction tx, Instant ts) {
        expect(mediator.lock(defaultLockID, tx, ts.plus(defaultExpireNS))).andReturn(true);
    }

    private void recordFailedLocalLock() {
        recordFailedLocalLock(defaultTx);
    }

    private void recordFailedLocalLock(StoreTransaction tx) {
        currentTimeNS = currentTimeNS.plusNanos(1);
        expect(times.getTime()).andReturn(currentTimeNS);
        expect(mediator.lock(defaultLockID, tx, currentTimeNS.plus(defaultExpireNS))).andReturn(false);
    }

    private void recordSuccessfulLocalUnlock() {
        expect(mediator.unlock(defaultLockID, defaultTx)).andReturn(true);
    }

    private void recordLockGetSlice(EntryList returnedEntries) throws BackendException {
        final KeySliceQuery ksq = new KeySliceQuery(defaultLockKey, LOCK_COL_START, LOCK_COL_END);
        expect(store.getSlice(eq(ksq), eq(defaultTx))).andReturn(returnedEntries);
    }

    private void recordExceptionalLockGetSlice(Throwable t) throws BackendException {
        final KeySliceQuery ksq = new KeySliceQuery(defaultLockKey, LOCK_COL_START, LOCK_COL_END);
        expect(store.getSlice(eq(ksq), eq(defaultTx))).andThrow(t);
    }

    private void recordLockGetSliceAndReturnSingleEntry(Entry returnSingleEntry) throws BackendException {
        recordLockGetSlice(StaticArrayEntryList.of(returnSingleEntry));
    }

    private ConsistentKeyLocker.Builder getDefaultBuilder() {
        return new ConsistentKeyLocker.Builder(store, manager)
            .times(times)
            .mediator(mediator)
            .internalState(lockState)
            .lockExpire(defaultExpireNS)
            .lockWait(defaultWaitNS)
            .rid(defaultLockRid);
    }

    /*
     * This class supports partial mocking of TimestampProvider.
     *
     * It's impossible to mock Timestamps.NANO because that is an enum;
     * EasyMock will fail to do it at runtime with cryptic
     * "incompatible return value type" exceptions.
     */
    private static class FakeTimestampProvider implements TimestampProvider {

        @Override
        public Instant getTime() {
            throw new IllegalStateException();
        }

        @Override
        public Instant getTime(long sinceEpoch) {
            return Instant.ofEpochSecond(0, sinceEpoch);
        }


        @Override
        public ChronoUnit getUnit() {
            return ChronoUnit.NANOS;
        }

        @Override
        public Instant sleepPast(Instant futureTime) {
            throw new IllegalStateException();
        }

        @Override
        public void sleepFor(Duration duration) {
            throw new IllegalStateException();
        }

        @Override
        public Timer getTimer() {
            return new Timer(this);
        }

        @Override
        public long getTime(Instant timestamp) {
            return timestamp.getEpochSecond() * 1000000000L + timestamp.getNano();
        }
    }

    private static class TestTrxImpl implements StoreTransaction {
        private final BaseTransactionConfig trxConfig;
        private       int                   openCount     = 0;
        private       int                   commitCount   = 0;
        private       int                   rollbackCount = 0;

        public TestTrxImpl(BaseTransactionConfig trxConfig) {
            this.trxConfig = trxConfig;
        }

        public boolean isOpen() {
            return openCount > (commitCount + rollbackCount);
        }

        public int getOpenCount() { return openCount; }

        public int getCommitCount() { return commitCount; }

        public int getRollbackCount() { return rollbackCount; }

        public StoreTransaction open() {
            openCount++;

            return this;
        }

        @Override
        public BaseTransactionConfig getConfiguration() {
            return trxConfig;
        }

        @Override
        public void commit() throws BackendException {
            commitCount++;
        }

        @Override
        public void rollback() throws BackendException {
            rollbackCount++;
        }
    }
}
