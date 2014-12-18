package com.thinkaurelius.titan.diskstorage.locking;

import static com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLocker.LOCK_COL_END;
import static com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLocker.LOCK_COL_START;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.*;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.diskstorage.util.time.StandardTimepoint;
import com.thinkaurelius.titan.diskstorage.util.time.Timepoint;
import com.thinkaurelius.titan.diskstorage.util.time.Timer;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.util.*;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;

import org.easymock.LogicalOperator;

import java.util.Comparator;

import static org.easymock.EasyMock.*;


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

    private StoreTransaction defaultTx;
    private BaseTransactionConfig defaultTxCfg;
    private Configuration defaultTxCustomOpts;

    private StoreTransaction otherTx;
    private BaseTransactionConfig otherTxCfg;
    private Configuration otherTxCustomOpts;

    private final long defaultWaitNS = 100 * 1000 * 1000;
    private final long defaultExpireNS = 30L * 1000 * 1000 * 1000;

    private final int maxTemporaryStorageExceptions = 3;

    private IMocksControl ctrl;
    private IMocksControl relaxedCtrl;
    private long currentTimeNS;
    private TimestampProvider times;
    private KeyColumnValueStore store;
    private StoreManager manager;
    private LocalLockMediator<StoreTransaction> mediator;
    private LockerState<ConsistentKeyLockStatus> lockState;
    private ConsistentKeyLocker locker;

    @SuppressWarnings("unchecked")
    @Before
    public void setupMocks() throws BackendException, NoSuchMethodException, SecurityException {
        currentTimeNS = 0;

        /*
         * relaxedControl doesn't care about the order in which its mocks'
         * methods are called. This is useful for mocks of immutable objects.
         */
        relaxedCtrl = EasyMock.createControl();

        manager = relaxedCtrl.createMock(StoreManager.class);

        defaultTx = relaxedCtrl.createMock(StoreTransaction.class);
        defaultTxCfg = relaxedCtrl.createMock(BaseTransactionConfig.class);
        defaultTxCustomOpts = relaxedCtrl.createMock(Configuration.class);
        expect(defaultTx.getConfiguration()).andReturn(defaultTxCfg).anyTimes();
        expect(defaultTxCfg.getGroupName()).andReturn("default").anyTimes();
        expect(defaultTxCfg.getCustomOptions()).andReturn(defaultTxCustomOpts).anyTimes();
        Comparator<BaseTransactionConfig> defaultTxCfgChecker = new Comparator<BaseTransactionConfig>() {
            @Override
            public int compare(BaseTransactionConfig actual, BaseTransactionConfig ignored) {
                return actual.getCustomOptions() == defaultTxCustomOpts ? 0 : -1;
            }
        };
        expect(manager.beginTransaction(cmp(null, defaultTxCfgChecker, LogicalOperator.EQUAL))).andReturn(defaultTx).anyTimes();

        otherTx = relaxedCtrl.createMock(StoreTransaction.class);
        otherTxCfg = relaxedCtrl.createMock(BaseTransactionConfig.class);
        otherTxCustomOpts = relaxedCtrl.createMock(Configuration.class);
        expect(otherTx.getConfiguration()).andReturn(otherTxCfg).anyTimes();
        expect(otherTxCfg.getGroupName()).andReturn("other").anyTimes();
        expect(otherTxCfg.getCustomOptions()).andReturn(otherTxCustomOpts).anyTimes();
        Comparator<BaseTransactionConfig> otherTxCfgChecker = new Comparator<BaseTransactionConfig>() {
            @Override
            public int compare(BaseTransactionConfig actual, BaseTransactionConfig ignored) {
                return actual.getCustomOptions() == otherTxCustomOpts ? 0 : -1;
            }
        };
        expect(manager.beginTransaction(cmp(null, otherTxCfgChecker, LogicalOperator.EQUAL))).andReturn(otherTx).anyTimes();


        /*
         * ctrl requires that the complete, order-sensitive sequence of actual
         * method invocations on its mocks exactly match the expected sequence
         * hard-coded into each test method. Either an unexpected actual
         * invocation or expected invocation that fails to actually occur will
         * cause a test failure.
         */
        ctrl = EasyMock.createStrictControl();
        Method timeInNativeUnit = FakeTimestampProvider.class.getMethod("getTime");
        Method timeInSpecifiedUnit = FakeTimestampProvider.class.getMethod("getTime", long.class, TimeUnit.class);
        Method sleepPast = FakeTimestampProvider.class.getMethod("sleepPast", Timepoint.class);

        times = ctrl.createMock(FakeTimestampProvider.class, timeInNativeUnit, timeInSpecifiedUnit, sleepPast);
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

    @After
    public void verifyMocks() {
        ctrl.verify();
        relaxedCtrl.verify();
    }

    /**
     * Test a single lock using stub objects. Doesn't test unlock ("leaks" the
     * lock, but since it's backed by stubs, it doesn't matter).
     *
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testWriteLockInSimplestCase() throws BackendException {

        // Check to see whether the lock was already written before anything else
        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        // Now lock it locally to block other threads in the process
        recordSuccessfulLocalLock();
        // Write a lock claim column to the store
        LockInfo li = recordSuccessfulLockWrite(1, TimeUnit.NANOSECONDS, null);
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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testWriteLockRetriesAfterOneStoreTimeout() throws BackendException {
        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        recordSuccessfulLocalLock();
        StaticBuffer firstCol = recordSuccessfulLockWrite(5, TimeUnit.SECONDS, null).col; // too slow
        LockInfo secondLI = recordSuccessfulLockWrite(1, TimeUnit.NANOSECONDS, firstCol); // plenty fast
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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testWriteLockThrowsExceptionAfterMaxStoreTimeouts() throws BackendException {
        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        recordSuccessfulLocalLock();
        StaticBuffer firstCol = recordSuccessfulLockWrite(5, TimeUnit.SECONDS, null).col;
        StaticBuffer secondCol = recordSuccessfulLockWrite(5, TimeUnit.SECONDS, firstCol).col;
        StaticBuffer thirdCol = recordSuccessfulLockWrite(5, TimeUnit.SECONDS, secondCol).col;
        recordSuccessfulLockDelete(1, TimeUnit.NANOSECONDS, thirdCol);
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
     * Test that the first {@link com.thinkaurelius.titan.diskstorage.PermanentBackendException} thrown by the
     * locker's store causes it to attempt to delete outstanding lock writes and
     * then emit the exception without retrying.
     *
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testWriteLockDiesOnPermanentStorageException() throws BackendException {
        PermanentBackendException errOnFire = new PermanentBackendException("Storage cluster is on fire");

        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        recordSuccessfulLocalLock();
        StaticBuffer lockCol = recordExceptionLockWrite(1, TimeUnit.NANOSECONDS, null, errOnFire);
        recordSuccessfulLockDelete(1, TimeUnit.NANOSECONDS, lockCol);
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
     * fails with a {@link com.thinkaurelius.titan.diskstorage.TemporaryBackendException}. The retry should both
     * attempt to write the and delete the failed mutation column.
     *
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testWriteLockRetriesOnTemporaryStorageException() throws BackendException {
        TemporaryBackendException tse = new TemporaryBackendException("Storage cluster is waking up");

        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        recordSuccessfulLocalLock();
        StaticBuffer firstCol = recordExceptionLockWrite(1, TimeUnit.NANOSECONDS, null, tse);
        LockInfo secondLI = recordSuccessfulLockWrite(1, TimeUnit.NANOSECONDS, firstCol);
        recordSuccessfulLocalLock(secondLI.tsNS);
        lockState.take(eq(defaultTx), eq(defaultLockID), eq(secondLI.stat));
        ctrl.replay();

        locker.writeLock(defaultLockID, defaultTx); // SUT
    }

    /**
     * Test that a failure to lock locally results in a {@link TemporaryLockingException}
     *
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shouldn't happen
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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testWriteLockDetectsMultiTxContention() throws BackendException {
        // defaultTx

        // Check to see whether the lock was already written before anything else
        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        // Now lock it locally to block other threads in the process
        recordSuccessfulLocalLock();
        // Write a lock claim column to the store
        LockInfo li = recordSuccessfulLockWrite(1, TimeUnit.NANOSECONDS, null);
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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testWriteLockIdempotence() throws BackendException {
        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        recordSuccessfulLocalLock();
        LockInfo li = recordSuccessfulLockWrite(1, TimeUnit.NANOSECONDS, null);
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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException     shouldn't happen
     * @throws InterruptedException shouldn't happen
     */
    @Test
    public void testCheckLocksInSimplestCase() throws BackendException, InterruptedException {
        // Fake a pre-existing lock
        final ConsistentKeyLockStatus ls = makeStatusNow();
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, ls));
        currentTimeNS += TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
        // Checker should compare the fake lock's timestamp to the current time
        expectSleepAfterWritingLock(ls);
        // Expect a store getSlice() and return the fake lock's column and value
        recordLockGetSliceAndReturnSingleEntry(
                StaticArrayEntry.of(
                        codec.toLockCol(ls.getWriteTimestamp(TimeUnit.NANOSECONDS), defaultLockRid),
                        defaultLockVal));
        ctrl.replay();

        locker.checkLocks(defaultTx);
    }

    private void expectSleepAfterWritingLock(ConsistentKeyLockStatus ls) throws InterruptedException {
        expect(times.sleepPast(new StandardTimepoint(ls.getWriteTimestamp(TimeUnit.NANOSECONDS) + defaultWaitNS, times))).andReturn(new StandardTimepoint(currentTimeNS, times));
    }

    /**
     * A transaction that writes a lock, waits past expiration, and attempts
     * to check locks should receive an {@code ExpiredLockException} during
     * the check stage.
     *
     * @throws com.thinkaurelius.titan.diskstorage.BackendException     shouldn't happen
     * @throws InterruptedException
     */
    @Test
    public void testCheckOwnExpiredLockThrowsException() throws BackendException, InterruptedException {
        // Fake a pre-existing lock that's long since expired
        final ConsistentKeyLockStatus expired = makeStatusNow();
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, expired));
        currentTimeNS += TimeUnit.NANOSECONDS.convert(100, TimeUnit.DAYS); // pretend a huge multiple of the expiration time has passed

        // Checker should compare the fake lock's timestamp to the current time
        expectSleepAfterWritingLock(expired);

        // Checker must slice the store; we return the single expired lock column
        recordLockGetSliceAndReturnSingleEntry(
                StaticArrayEntry.of(
                        codec.toLockCol(expired.getWriteTimestamp(TimeUnit.NANOSECONDS), defaultLockRid),
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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException     shouldn't happen
     * @throws InterruptedException
     */
    @Test
    public void testCheckLocksIgnoresOtherExpiredLocks() throws BackendException, InterruptedException {
        // Fake a pre-existing lock from a different tx that's long since expired
        final ConsistentKeyLockStatus otherExpired = makeStatusNow();

        // Fake a pre-existing lock from our tx
        final ConsistentKeyLockStatus ownExpired = makeStatusNow();

        currentTimeNS += TimeUnit.NANOSECONDS.convert(100, TimeUnit.DAYS); // pretend a huge multiple of the expiration time has passed

        // Create a still-valid lock belonging to the default tx
        final ConsistentKeyLockStatus recent = makeStatusNow();
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, recent));
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1L, TimeUnit.MILLISECONDS);

        expectSleepAfterWritingLock(recent);

        // Checker must slice the store; return both of the expired claims and the one active claim
        recordLockGetSlice(StaticArrayEntryList.of(
                StaticArrayEntry.of(
                        codec.toLockCol(otherExpired.getWriteTimestamp(TimeUnit.NANOSECONDS), otherLockRid),
                        defaultLockVal),
                StaticArrayEntry.of(
                        codec.toLockCol(ownExpired.getWriteTimestamp(TimeUnit.NANOSECONDS), defaultLockRid),
                        defaultLockVal),
                StaticArrayEntry.of(
                        codec.toLockCol(recent.getWriteTimestamp(TimeUnit.NANOSECONDS), defaultLockRid),
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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException     shouldn't happen
     */
    @Test
    public void testCheckLocksIdempotence() throws InterruptedException, BackendException {
        // Fake a pre-existing valid lock
        final ConsistentKeyLockStatus ls = makeStatusNow();
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, ls));
        currentTimeNS += TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);

        expectSleepAfterWritingLock(ls);

        final StaticBuffer lc = codec.toLockCol(ls.getWriteTimestamp(TimeUnit.NANOSECONDS), defaultLockRid);
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
     * If the checker reads its own lock column preceeded by a lock column from
     * another rid with an earlier timestamp and the timestamps on both columns
     * are unexpired, then the checker must throw a TemporaryLockingException.
     *
     * @throws InterruptedException shouldn't happen
     * @throws com.thinkaurelius.titan.diskstorage.BackendException     shouldn't happen (we expect a TemporaryLockingException but
     *                              we catch and swallow it)
     */
    @Test
    public void testCheckLocksFailsWithSeniorClaimsByOthers() throws InterruptedException, BackendException {
        // Make a pre-existing valid lock by some other tx (written by another process)
        StaticBuffer otherSeniorLockCol = codec.toLockCol(currentTimeNS, otherLockRid);
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);
        // Expect checker to fetch locks for defaultTx; return just our own lock (not the other guy's)
        StaticBuffer ownJuniorLockCol = codec.toLockCol(currentTimeNS, defaultLockRid);
        ConsistentKeyLockStatus ownJuniorLS = makeStatusNow();
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, ownJuniorLS));

        currentTimeNS += TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);

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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException     shouldn't happen
     */
    @Test
    public void testCheckLocksSucceedsWithJuniorClaimsByOthers() throws InterruptedException, BackendException {
        // Expect checker to fetch locks for defaultTx; return just our own lock (not the other guy's)
        StaticBuffer ownSeniorLockCol = codec.toLockCol(currentTimeNS, defaultLockRid);
        ConsistentKeyLockStatus ownSeniorLS = makeStatusNow();
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);
        // Make junior lock
        StaticBuffer otherJuniorLockCol = codec.toLockCol(currentTimeNS, otherLockRid);
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, ownSeniorLS));

        currentTimeNS += TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);

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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException     shouldn't happen
     */
    @Test
    public void testCheckLocksSucceedsWithSeniorAndJuniorClaimsBySelf() throws InterruptedException, BackendException {
        // Setup three lock columns differing only in timestamp
        StaticBuffer myFirstLockCol = codec.toLockCol(currentTimeNS, defaultLockRid);
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);
        StaticBuffer mySecondLockCol = codec.toLockCol(currentTimeNS, defaultLockRid);
        ConsistentKeyLockStatus mySecondLS = makeStatusNow();
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);
        StaticBuffer myThirdLockCol = codec.toLockCol(currentTimeNS, defaultLockRid);
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);

        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, mySecondLS));

        // Return defaultTx's second lock in a map when requested
        currentTimeNS += TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException     shouldn't happen
     * @throws InterruptedException shouldn't happen
     */
    @Test
    public void testCheckLocksRetriesAfterSingleTemporaryStorageException() throws BackendException, InterruptedException {
        // Setup one lock column
        StaticBuffer lockCol = codec.toLockCol(currentTimeNS, defaultLockRid);
        ConsistentKeyLockStatus lockStatus = makeStatusNow();
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);

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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException     shouldn't happen
     */
    @Test
    public void testCheckLocksThrowsExceptionAfterMaxTemporaryStorageExceptions() throws InterruptedException, BackendException {
        // Setup a LockStatus for defaultLockID
        ConsistentKeyLockStatus lockStatus = makeStatusNow();
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);

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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException     shouldn't happen
     */
    @Test
    public void testCheckLocksDiesOnPermanentStorageException() throws InterruptedException, BackendException {
        // Setup a LockStatus for defaultLockID
        ConsistentKeyLockStatus lockStatus = makeStatusNow();
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);

        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, lockStatus));

        expectSleepAfterWritingLock(lockStatus);

        // First and only getSlice call throws a PSE
        recordExceptionalLockGetSlice(new PermanentBackendException("Connection to storage cluster failed: peer is an IPv6 toaster"));

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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testCheckLocksDoesNothingForUnrecognizedTransaction() throws BackendException {
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.<KeyColumn, ConsistentKeyLockStatus>of());
        ctrl.replay();
        locker.checkLocks(defaultTx);
    }

    /**
     * Delete a single lock without any timeouts, errors, etc.
     *
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testDeleteLocksInSimplestCase() throws BackendException {
        // Setup a LockStatus for defaultLockID
        final ConsistentKeyLockStatus lockStatus = makeStatusNow();
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);

        @SuppressWarnings("serial")
        Map<KeyColumn, ConsistentKeyLockStatus> expectedMap = new HashMap<KeyColumn, ConsistentKeyLockStatus>() {{
            put(defaultLockID, lockStatus);
        }};
        expect(lockState.getLocksForTx(defaultTx)).andReturn(expectedMap);

        List<StaticBuffer> dels = ImmutableList.of(codec.toLockCol(lockStatus.getWriteTimestamp(TimeUnit.NANOSECONDS), defaultLockRid));
        expect(times.getTime()).andReturn(new StandardTimepoint(currentTimeNS, times));
        store.mutate(eq(defaultLockKey), eq(ImmutableList.<Entry>of()), eq(dels), eq(defaultTx));
        expect(mediator.unlock(defaultLockID, defaultTx)).andReturn(true);

        ctrl.replay();

        locker.deleteLocks(defaultTx);
    }

    /**
     * Delete two locks without any timeouts, errors, etc.
     *
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testDeleteLocksOnTwoLocks() throws BackendException {
        ConsistentKeyLockStatus defaultLS = makeStatusNow();
        currentTimeNS++;
        ConsistentKeyLockStatus otherLS = makeStatusNow();
        currentTimeNS++;

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

    private void expectLockDeleteSuccessfully(KeyColumn lockID, StaticBuffer lockKey, ConsistentKeyLockStatus lockStatus) throws BackendException {
        expectDeleteLock(lockID, lockKey, lockStatus);
    }

    private void expectDeleteLock(KeyColumn lockID, StaticBuffer lockKey, ConsistentKeyLockStatus lockStatus, BackendException... backendFailures) throws BackendException {
        List<StaticBuffer> dels = ImmutableList.of(codec.toLockCol(lockStatus.getWriteTimestamp(TimeUnit.NANOSECONDS), defaultLockRid));
        expect(times.getTime()).andReturn(new StandardTimepoint(currentTimeNS, times));
        store.mutate(eq(lockKey), eq(ImmutableList.<Entry>of()), eq(dels), eq(defaultTx));
        int backendExceptionsThrown = 0;
        for (BackendException e : backendFailures) {
            expectLastCall().andThrow(e);
            if (e instanceof PermanentBackendException) {
                break;
            }
            backendExceptionsThrown++;
            if (backendExceptionsThrown < maxTemporaryStorageExceptions) {
                expect(times.getTime()).andReturn(new StandardTimepoint(currentTimeNS, times));
                store.mutate(eq(lockKey), eq(ImmutableList.<Entry>of()), eq(dels), eq(defaultTx));
            }
        }
        expect(mediator.unlock(lockID, defaultTx)).andReturn(true);
    }

    /**
     * Lock deletion should retry if the first store mutation throws a temporary
     * exception.
     *
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testDeleteLocksRetriesOnTemporaryStorageException() throws BackendException {
        ConsistentKeyLockStatus defaultLS = makeStatusNow();
        currentTimeNS++;
        expect(lockState.getLocksForTx(defaultTx)).andReturn(Maps.newLinkedHashMap(ImmutableMap.of(defaultLockID, defaultLS)));
        expectDeleteLock(defaultLockID, defaultLockKey, defaultLS, new TemporaryBackendException("Storage cluster is backlogged"));
        ctrl.replay();

        locker.deleteLocks(defaultTx);
    }

    /**
     * If lock deletion exceeds the temporary exception retry count when trying
     * to delete a lock, it should move onto the next lock rather than returning
     * and potentially leaving the remaining locks undeleted.
     *
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testDeleteLocksSkipsToNextLockAfterMaxTemporaryStorageExceptions() throws BackendException {
        ConsistentKeyLockStatus defaultLS = makeStatusNow();
        currentTimeNS++;
        expect(lockState.getLocksForTx(defaultTx)).andReturn(Maps.newLinkedHashMap(ImmutableMap.of(defaultLockID, defaultLS)));

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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shoudn't happen
     */
    @Test
    public void testDeleteLocksSkipsToNextLockOnPermanentStorageException() throws BackendException {
        ConsistentKeyLockStatus defaultLS = makeStatusNow();
        currentTimeNS++;
        expect(lockState.getLocksForTx(defaultTx)).andReturn(Maps.newLinkedHashMap(ImmutableMap.of(defaultLockID, defaultLS)));

        expectDeleteLock(defaultLockID, defaultLockKey, defaultLS, new PermanentBackendException("Storage cluster has been destroyed by a tornado"));

        ctrl.replay();

        locker.deleteLocks(defaultTx);
    }

    /**
     * Deletion should remove previously written locks regardless of whether
     * they were ever checked; this method fakes and verifies deletion on a
     * single unchecked lock
     *
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testDeleteLocksDeletesUncheckedLocks() throws BackendException {
        ConsistentKeyLockStatus defaultLS = makeStatusNow();
        assertFalse(defaultLS.isChecked());
        currentTimeNS++;

        // Expect a call for defaultTx's locks and the checked one
        expect(lockState.getLocksForTx(defaultTx)).andReturn(Maps.newLinkedHashMap(ImmutableMap.of(defaultLockID, defaultLS)));

        expectLockDeleteSuccessfully(defaultLockID, defaultLockKey, defaultLS);

        ctrl.replay();

        locker.deleteLocks(defaultTx);
    }

    /**
     * When delete is called multiple times with no intervening write or check
     * calls, all calls after the first should have no effect.
     *
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testDeleteLocksIdempotence() throws BackendException {
        // Setup a LockStatus for defaultLockID
        ConsistentKeyLockStatus lockStatus = makeStatusNow();
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);

        expect(lockState.getLocksForTx(defaultTx)).andReturn(Maps.newLinkedHashMap(ImmutableMap.of(defaultLockID, lockStatus)));

        expectLockDeleteSuccessfully(defaultLockID, defaultLockKey, lockStatus);

        ctrl.replay();

        locker.deleteLocks(defaultTx);

        ctrl.verify();
        ctrl.reset();
        expect(lockState.getLocksForTx(defaultTx)).andReturn(Maps.newLinkedHashMap(ImmutableMap.<KeyColumn, ConsistentKeyLockStatus>of()));
        ctrl.replay();
        locker.deleteLocks(defaultTx);
    }

    /**
     * Delete should do nothing when passed a transaction for which it holds no
     * locks.
     *
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shouldn't happen
     */
    @Test
    public void testDeleteLocksDoesNothingForUnrecognizedTransaction() throws BackendException {
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.<KeyColumn, ConsistentKeyLockStatus>of());
        ctrl.replay();
        locker.deleteLocks(defaultTx);
    }

    /**
     * Checking locks when the expired lock cleaner is enabled should trigger
     * one call to the LockCleanerService.
     *
     * @throws com.thinkaurelius.titan.diskstorage.BackendException shouldn't happen
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
        currentTimeNS += TimeUnit.NANOSECONDS.convert(100, TimeUnit.DAYS); // pretend a huge multiple of the expiration time has passed

        // Checker should compare the fake lock's timestamp to the current time
        expect(times.sleepPast(new StandardTimepoint(expired.getWriteTimestamp(TimeUnit.NANOSECONDS) + defaultWaitNS, times))).andReturn(new StandardTimepoint(currentTimeNS, times));

        // Checker must slice the store; we return the single expired lock column
        recordLockGetSliceAndReturnSingleEntry(
                StaticArrayEntry.of(
                        codec.toLockCol(expired.getWriteTimestamp(TimeUnit.NANOSECONDS), defaultLockRid),
                        defaultLockVal));

        // Checker must attempt to cleanup expired lock
        mockCleaner.clean(eq(defaultLockID), eq(currentTimeNS - defaultExpireNS), eq(defaultTx));
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

        private final long tsNS;
        private final ConsistentKeyLockStatus stat;
        private final StaticBuffer col;

        public LockInfo(long tsNS, ConsistentKeyLockStatus stat,
                        StaticBuffer col) {
            this.tsNS = tsNS;
            this.stat = stat;
            this.col = col;
        }
    }

    private ConsistentKeyLockStatus makeStatus(long currentNS) {
        return new ConsistentKeyLockStatus(
                new StandardTimepoint(currentTimeNS, times),
                new StandardTimepoint(defaultExpireNS, times));
    }

    private ConsistentKeyLockStatus makeStatusNow() {
        return makeStatus(currentTimeNS);
    }

    private LockInfo recordSuccessfulLockWrite(long duration, TimeUnit tu, StaticBuffer del) throws BackendException {
        return recordSuccessfulLockWrite(defaultTx, duration, tu, del);
    }

    private LockInfo recordSuccessfulLockWrite(StoreTransaction tx, long duration, TimeUnit tu, StaticBuffer del) throws BackendException {
        expect(times.getTime()).andReturn(new StandardTimepoint(++currentTimeNS, times));

        final long lockNS = currentTimeNS;

        StaticBuffer lockCol = codec.toLockCol(lockNS, defaultLockRid);
        Entry add = StaticArrayEntry.of(lockCol, defaultLockVal);

        StaticBuffer k = eq(defaultLockKey);
        final List<Entry> adds = eq(Arrays.<Entry>asList(add));
        final List<StaticBuffer> dels;
        if (null != del) {
            dels = eq(Arrays.<StaticBuffer>asList(del));
        } else {
            dels = eq(ImmutableList.<StaticBuffer>of());
        }

        store.mutate(k, adds, dels, eq(tx));
        expectLastCall().once();

        currentTimeNS += TimeUnit.NANOSECONDS.convert(duration, tu);

        expect(times.getTime()).andReturn(new StandardTimepoint(currentTimeNS, times));

        ConsistentKeyLockStatus status = new ConsistentKeyLockStatus(
                new StandardTimepoint(lockNS, times),
                new StandardTimepoint(lockNS + defaultExpireNS, times));

        return new LockInfo(lockNS, status, lockCol);
    }

    private StaticBuffer recordExceptionLockWrite(long duration, TimeUnit tu, StaticBuffer del, Throwable t) throws BackendException {
        expect(times.getTime()).andReturn(new StandardTimepoint(++currentTimeNS, times));

        StaticBuffer lockCol = codec.toLockCol(currentTimeNS, defaultLockRid);
        Entry add = StaticArrayEntry.of(lockCol, defaultLockVal);

        StaticBuffer k = eq(defaultLockKey);
        final List<Entry> adds = eq(Arrays.<Entry>asList(add));
        final List<StaticBuffer> dels;
        if (null != del) {
            dels = eq(Arrays.<StaticBuffer>asList(del));
        } else {
            dels = eq(ImmutableList.<StaticBuffer>of());
        }
        store.mutate(k, adds, dels, eq(defaultTx));
        expectLastCall().andThrow(t);

        currentTimeNS += TimeUnit.NANOSECONDS.convert(duration, tu);
        expect(times.getTime()).andReturn(new StandardTimepoint(currentTimeNS, times));

        return lockCol;
    }

    private void recordSuccessfulLockDelete(long duration, TimeUnit tu, StaticBuffer del) throws BackendException {
        expect(times.getTime()).andReturn(new StandardTimepoint(++currentTimeNS, times));
        store.mutate(eq(defaultLockKey), eq(ImmutableList.<Entry>of()), eq(Arrays.asList(del)), eq(defaultTx));

        currentTimeNS += TimeUnit.NANOSECONDS.convert(duration, tu);
        expect(times.getTime()).andReturn(new StandardTimepoint(currentTimeNS, times));
    }

    private void recordSuccessfulLocalLock() {
        recordSuccessfulLocalLock(defaultTx);
    }

    private void recordSuccessfulLocalLock(StoreTransaction tx) {
        expect(times.getTime()).andReturn(new StandardTimepoint(++currentTimeNS, times));
        expect(mediator.lock(defaultLockID, tx, new StandardTimepoint(currentTimeNS + defaultExpireNS, times))).andReturn(true);
    }

    private void recordSuccessfulLocalLock(long ts) {
        recordSuccessfulLocalLock(defaultTx, ts);
    }

    private void recordSuccessfulLocalLock(StoreTransaction tx, long ts) {
        expect(mediator.lock(defaultLockID, tx, new StandardTimepoint(ts + defaultExpireNS, times))).andReturn(true);
    }

    private void recordFailedLocalLock() {
        recordFailedLocalLock(defaultTx);
    }

    private void recordFailedLocalLock(StoreTransaction tx) {
        expect(times.getTime()).andReturn(new StandardTimepoint(++currentTimeNS, times));
        expect(mediator.lock(defaultLockID, tx, new StandardTimepoint(currentTimeNS + defaultExpireNS, times))).andReturn(false);
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
            .lockExpire(new StandardDuration(defaultExpireNS, TimeUnit.NANOSECONDS))
            .lockWait(new StandardDuration(defaultWaitNS, TimeUnit.NANOSECONDS))
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
        public Timepoint getTime() {
            throw new IllegalStateException();
        }

        @Override
        public Timepoint getTime(long sinceEpoch, TimeUnit unit) {
            throw new IllegalStateException();
        }

        @Override
        public TimeUnit getUnit() {
            return TimeUnit.NANOSECONDS;
        }

        @Override
        public Timepoint sleepPast(Timepoint futureTime)
                throws InterruptedException {
            throw new IllegalStateException();
        }

        @Override
        public void sleepFor(Duration duration) throws InterruptedException {
            throw new IllegalStateException();
        }

        @Override
        public Timer getTimer() {
            return new Timer(this);
        }
    }
}
