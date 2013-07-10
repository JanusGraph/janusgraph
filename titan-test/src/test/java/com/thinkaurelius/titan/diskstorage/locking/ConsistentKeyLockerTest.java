package com.thinkaurelius.titan.diskstorage.locking;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLocker;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLocker.LockState;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLocker.LockStatus;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockerConfiguration;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockerSerializer;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;


public class ConsistentKeyLockerTest {
    
    // Arbitrary literals -- the exact values assigned here are not intrinsically important
    private final ConsistentKeyLockerSerializer codec = new ConsistentKeyLockerSerializer();
    private final StaticBuffer defaultDataKey = ByteBufferUtil.getIntBuffer(2);
    private final StaticBuffer defaultDataCol = ByteBufferUtil.getIntBuffer(4);
    private final StaticBuffer defaultLockKey = codec.toLockKey(defaultDataKey, defaultDataCol);
    private final KeyColumn defaultLockID = new KeyColumn(defaultDataKey, defaultDataCol);
    
    private final StaticBuffer otherDataKey = ByteBufferUtil.getIntBuffer(8);
    private final StaticBuffer otherDataCol = ByteBufferUtil.getIntBuffer(16);
    private final StaticBuffer otherLockKey = codec.toLockKey(otherDataKey, otherDataCol);
    private final KeyColumn otherLockID = new KeyColumn(otherDataKey, otherDataCol);
    
    private final StaticBuffer defaultLockRid = new StaticArrayBuffer(new byte[] { (byte)32 });
    private final StaticBuffer otherLockRid = new StaticArrayBuffer(new byte[] { (byte)64 });
    private final StaticBuffer defaultLockVal = ByteBufferUtil.getIntBuffer(0); // maybe refactor...
    
    private StoreTransaction defaultTx;
    private StoreTransaction otherTx;
    
    private final long defaultWaitNS =          100 * 1000 * 1000;
    private final long defaultExpireNS = 30L * 1000 * 1000 * 1000;
    
    private IMocksControl ctrl;
    private long currentTimeNS;
    private TimestampProvider times;
    private KeyColumnValueStore store;
    private LocalLockMediator<StoreTransaction> mediator;
    private ConsistentKeyLockerConfiguration conf;
    private LockState lockState;
    private ConsistentKeyLocker locker;
    
    @SuppressWarnings("unchecked")
    @Before
    public void setupMocks() {
        currentTimeNS = 0;
        ctrl = createStrictControl();
        defaultTx = ctrl.createMock(StoreTransaction.class);
        otherTx = ctrl.createMock(StoreTransaction.class);
        times = ctrl.createMock(TimestampProvider.class);
        store = ctrl.createMock(KeyColumnValueStore.class);
        mediator = ctrl.createMock(LocalLockMediator.class);
        lockState = ctrl.createMock(LockState.class);
        conf = new ConsistentKeyLockerConfiguration.Builder(
                store).times(times).mediator(mediator)
                .lockExpireNS(defaultExpireNS, TimeUnit.NANOSECONDS)
                .lockWaitNS(defaultWaitNS, TimeUnit.NANOSECONDS)
                .rid(defaultLockRid).build();
        locker = new ConsistentKeyLocker(conf, lockState);
    }
    
    @After
    public void verifyMocks() {
        ctrl.verify();
    }
    
    /**
     * Test a single lock using stub objects. Doesn't test unlock ("leaks" the
     * lock, but since it's backed by stubs, it doesn't matter).
     * 
     * @throws StorageException shouldn't happen
     */
    @Test
    public void testWriteLockInSimplestCase() throws StorageException {

        // Check to see whether the lock was already written before anything else
        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        // Now lock it locally to block other threads in the process
        recordSuccessfulLocalLock();
        // Write a lock claim column to the store
        StaticBuffer lockCol = recordSuccessfulLockWrite(1, TimeUnit.NANOSECONDS, null);
        long lockColTimestampNS = codec.fromLockColumn(lockCol).getTimestamp();
        // Update the expiration timestamp of the local (thread-level) lock
        recordSuccessfulLocalLock(lockColTimestampNS);
        // Store the taken lock's key, column, and timestamp in the lockState map
        LockStatus expectedLS = new LockStatus(lockColTimestampNS, TimeUnit.NANOSECONDS);
        lockState.take(eq(defaultTx), eq(defaultLockID), eq(expectedLS));
        ctrl.replay();
        
        locker.writeLock(defaultLockID, defaultTx); // SUT
    }
    
    /**
     * Test locker when first attempt to write to the store takes too long (but
     * succeeds). Expected behavior is to call mutate on the store, adding a
     * column with a new timestamp and deleting the column with the old
     * (too-slow-to-write) timestamp.
     * 
     * @throws StorageException shouldn't happen
     */
    @Test
    public void testWriteLockRetriesAfterOneStoreTimeout() throws StorageException {
        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        recordSuccessfulLocalLock();
        StaticBuffer firstCol  = recordSuccessfulLockWrite(5, TimeUnit.SECONDS, null); // too slow
        StaticBuffer secondCol = recordSuccessfulLockWrite(1, TimeUnit.NANOSECONDS, firstCol); // plenty fast
        final long secondTimestampNS = codec.fromLockColumn(secondCol).getTimestamp();
        recordSuccessfulLocalLock(secondTimestampNS);
        LockStatus expectedLS = new LockStatus(secondTimestampNS, TimeUnit.NANOSECONDS);
        lockState.take(eq(defaultTx), eq(defaultLockID), eq(expectedLS));
        ctrl.replay();
        
        locker.writeLock(defaultLockID, defaultTx); // SUT
    }

    /**
     * Test locker when all three attempts to write a lock succeed but take
     * longer than the wait limit. We expect the locker to delete all three
     * columns that it wrote and locally unlock the KeyColumn, then emit an
     * exception.
     * 
     * @throws StorageException shouldn't happen
     */
    @Test
    public void testWriteLockThrowsExceptionAfterMaxStoreTimeouts() throws StorageException {
        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        recordSuccessfulLocalLock();
        StaticBuffer firstCol =  recordSuccessfulLockWrite(5, TimeUnit.SECONDS, null);
        StaticBuffer secondCol = recordSuccessfulLockWrite(5, TimeUnit.SECONDS, firstCol);
        StaticBuffer thirdCol =  recordSuccessfulLockWrite(5, TimeUnit.SECONDS, secondCol);
        recordSuccessfulLockDelete(1, TimeUnit.NANOSECONDS, thirdCol);
        recordSuccessfulLocalUnlock();
        ctrl.replay();

        StorageException expected = null;
        try {
            locker.writeLock(defaultLockID, defaultTx); // SUT
        } catch (TemporaryStorageException e) {
            expected = e;
        }
        assertNotNull(expected);
    }
    
    /**
     * Test that the first {@link PermanentStorageException} thrown by the
     * locker's store causes it to attempt to delete outstanding lock writes and
     * then emit the exception without retrying.
     * 
     * @throws StorageException shouldn't happen
     */
    @Test
    public void testWriteLockDiesOnPermanentStorageException() throws StorageException {
        PermanentStorageException errOnFire = new PermanentStorageException("Storage cluster is on fire");

        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        recordSuccessfulLocalLock();
        StaticBuffer lockCol = recordExceptionLockWrite(1, TimeUnit.NANOSECONDS, null, errOnFire);
        recordSuccessfulLockDelete(1, TimeUnit.NANOSECONDS, lockCol);
        recordSuccessfulLocalUnlock();
        ctrl.replay();
        
        StorageException expected = null;
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
     * fails with a {@link TemporaryStorageException}. The retry should both
     * attempt to write the and delete the failed mutation column.
     * 
     * @throws StorageException shouldn't happen
     */
    @Test
    public void testWriteLockRetriesOnTemporaryStorageException() throws StorageException {
        TemporaryStorageException tse = new TemporaryStorageException("Storage cluster is waking up");

        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        recordSuccessfulLocalLock();
        StaticBuffer firstCol  = recordExceptionLockWrite(1, TimeUnit.NANOSECONDS, null, tse);
        StaticBuffer secondCol = recordSuccessfulLockWrite(1, TimeUnit.NANOSECONDS, firstCol);
        final long secondTimestampNS = codec.fromLockColumn(secondCol).getTimestamp();
        recordSuccessfulLocalLock(secondTimestampNS);
        LockStatus expectedLS = new LockStatus(secondTimestampNS, TimeUnit.NANOSECONDS);
        lockState.take(eq(defaultTx), eq(defaultLockID), eq(expectedLS));
        ctrl.replay();
        
        locker.writeLock(defaultLockID, defaultTx); // SUT
    }
    
    /**
     * Test that a failure to lock locally results in a {@link TemporaryLockingException}
     * 
     * @throws StorageException shouldn't happen
     */
    @Test
    public void testWriteLockFailsOnLocalContention() throws StorageException {
        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        recordFailedLocalLock();
        ctrl.replay();
        
        TemporaryLockingException tle = null;
        try {
            locker.writeLock(defaultLockID, defaultTx); // SUT
        } catch (TemporaryLockingException e) {
            tle = e;
        }
        assertNotNull(tle);
    }
    
    /**
     * Test that multiple calls to
     * {@link ConsistentKeyLocker#writeLock(KeyColumn, StoreTransaction)} with
     * the same arguments have no effect after the first call (until
     * {@link ConsistentKeyLocker#deleteLocks(StoreTransaction)} is called).
     * 
     * @throws StorageException
     *             shouldn't happen
     */
    @Test
    public void testWriteLockIdempotence() throws StorageException {
        expect(lockState.has(defaultTx, defaultLockID)).andReturn(false);
        recordSuccessfulLocalLock();
        StaticBuffer firstCol = recordSuccessfulLockWrite(1, TimeUnit.NANOSECONDS, null);
        final long timestampNS = codec.fromLockColumn(firstCol).getTimestamp();
        recordSuccessfulLocalLock(timestampNS);
        LockStatus expectedLS = new LockStatus(timestampNS, TimeUnit.NANOSECONDS);
        lockState.take(eq(defaultTx), eq(defaultLockID), eq(expectedLS));
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
     * @throws StorageException
     *             shouldn't happen
     * @throws InterruptedException
     *             shouldn't happen
     */
    @Test
    public void testCheckLocksInSimplestCase() throws StorageException, InterruptedException {
        // Fake a pre-existing lock
        final LockStatus ls = new LockStatus(currentTimeNS, TimeUnit.NANOSECONDS);
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, ls));
        currentTimeNS += TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
        // Checker should compare the fake lock's timestamp to the current time
        expect(times.sleepUntil(ls.getWrittenTimestamp(TimeUnit.NANOSECONDS) + conf.getLockWait(TimeUnit.NANOSECONDS))).andReturn(currentTimeNS);
        // Expect a store getSlice() and return the fake lock's column and value
        recordLockGetSliceAndReturnSingleEntry(
                new StaticBufferEntry(
                        codec.toLockCol(ls.getWrittenTimestamp(TimeUnit.NANOSECONDS), defaultLockRid),
                        defaultLockVal));
        ctrl.replay();
        
        locker.checkLocks(defaultTx);
    }
    
    /**
     * Lock checking should treat columns with timestamps older than the
     * expiration period as though they were never read from the store (aside
     * from logging them). This tests the checker with a single expired column.
     * 
     * @throws StorageException
     *             shouldn't happen
     * @throws InterruptedException 
     */
    @Test
    public void testCheckLocksIgnoresSingleExpiredLock() throws StorageException, InterruptedException {
        // Fake a pre-existing lock that's long since expired
        final LockStatus expired = new LockStatus(currentTimeNS, TimeUnit.NANOSECONDS);
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, expired));
        currentTimeNS += TimeUnit.NANOSECONDS.convert(100, TimeUnit.DAYS); // fake expiration here
        
        // Checker should compare the fake lock's timestamp to the current time
        expect(times.sleepUntil(expired.getWrittenTimestamp(TimeUnit.NANOSECONDS) + conf.getLockWait(TimeUnit.NANOSECONDS))).andReturn(currentTimeNS);

        // Checker must slice the store; we return the single expired lock column
        recordLockGetSliceAndReturnSingleEntry(
                new StaticBufferEntry(
                        codec.toLockCol(expired.getWrittenTimestamp(TimeUnit.NANOSECONDS), defaultLockRid),
                        defaultLockVal));

        ctrl.replay();
        PermanentLockingException ple = null;
        try {
            locker.checkLocks(defaultTx);
        } catch (PermanentLockingException e) {
            ple = e;
        }
        assertNotNull(ple);
    }
    
    /**
     * Each written lock should be checked at most once. Test this by faking a
     * single previously written lock using mocks and stubs and then calling
     * checkLocks() twice. The second call should have no effect.
     * 
     * @throws InterruptedException
     *             shouldn't happen
     * @throws StorageException
     *             shouldn't happen
     */
    @Test
    public void testCheckLocksIdempotence() throws InterruptedException, StorageException {
        // Fake a pre-existing valid lock
        final LockStatus ls = new LockStatus(currentTimeNS, TimeUnit.NANOSECONDS);
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, ls));
        currentTimeNS += TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
        
        expect(times.sleepUntil(ls.getWrittenTimestamp(TimeUnit.NANOSECONDS) + conf.getLockWait(TimeUnit.NANOSECONDS))).andReturn(currentTimeNS);
        final StaticBuffer lc = codec.toLockCol(ls.getWrittenTimestamp(TimeUnit.NANOSECONDS), defaultLockRid);
        recordLockGetSliceAndReturnSingleEntry(new StaticBufferEntry(lc, defaultLockVal));
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
     * @throws InterruptedException
     *             shouldn't happen
     * @throws StorageException
     *             shouldn't happen (we expect a TemporaryLockingException but
     *             we catch and swallow it)
     */
    @Test
    public void testCheckLocksFailsWithSeniorClaimsByOthers() throws InterruptedException, StorageException {        
        // Make a pre-existing valid lock by some other tx (written by another process)
        StaticBuffer otherSeniorLockCol = codec.toLockCol(currentTimeNS, otherLockRid);
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);
        // Expect checker to fetch locks for defaultTx; return just our own lock (not the other guy's)
        StaticBuffer ownJuniorLockCol = codec.toLockCol(currentTimeNS, defaultLockRid);
        LockStatus ownJuniorLS = new LockStatus(currentTimeNS, TimeUnit.NANOSECONDS);
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, ownJuniorLS));
        
        currentTimeNS += TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
        
        // Return defaultTx's lock in a map when requested
        expect(times.sleepUntil(ownJuniorLS.getWrittenTimestamp(TimeUnit.NANOSECONDS) + conf.getLockWait(TimeUnit.NANOSECONDS))).andReturn(currentTimeNS);

        // When the checker slices the store, return the senior lock col by a
        // foreign tx and the junior lock col by defaultTx (in that order)
        recordLockGetSlice(ImmutableList.<Entry>of(
                new StaticBufferEntry(otherSeniorLockCol, defaultLockVal),
                new StaticBufferEntry(ownJuniorLockCol,   defaultLockVal)));

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
     * @throws InterruptedException
     *             shouldn't happen
     * @throws StorageException
     *             shouldn't happen
     */
    @Test
    public void testCheckLocksSucceedsWithJuniorClaimsByOthers() throws InterruptedException, StorageException {
        // Expect checker to fetch locks for defaultTx; return just our own lock (not the other guy's)
        StaticBuffer ownSeniorLockCol = codec.toLockCol(currentTimeNS, defaultLockRid);
        LockStatus ownSeniorLS = new LockStatus(currentTimeNS, TimeUnit.NANOSECONDS);
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);
        // Make junior lock
        StaticBuffer otherJuniorLockCol = codec.toLockCol(currentTimeNS, otherLockRid);
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, ownSeniorLS));
        
        currentTimeNS += TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
        
        // Return defaultTx's lock in a map when requested
        expect(times.sleepUntil(ownSeniorLS.getWrittenTimestamp(TimeUnit.NANOSECONDS) + conf.getLockWait(TimeUnit.NANOSECONDS))).andReturn(currentTimeNS);

        // When the checker slices the store, return the senior lock col by a
        // foreign tx and the junior lock col by defaultTx (in that order)
        recordLockGetSlice(ImmutableList.<Entry>of(
                new StaticBufferEntry(ownSeniorLockCol, defaultLockVal),
                new StaticBufferEntry(otherJuniorLockCol,   defaultLockVal)));

        ctrl.replay();
        
        locker.checkLocks(defaultTx);
    }
    
    /**
     * If the checker retrieves a timestamp-ordered list of columns, where the
     * list starts with an unbroken series of columns with the checker's rid but
     * differing timestamps, then consider the lock successfully checked if the
     * checker's expected timestamp occurs anywhere in that series of columns.
     * <p>
     * This relaxation of the normal checking rules only triggers when either
     * writeLock(...) issued mutate calls that appeared to fail client-side but
     * which actually succeeded (e.g. hinted handoff or timeout)
     * 
     * @throws InterruptedException
     *             shouldn't happen
     * @throws StorageException
     *             shouldn't happen
     */
    @Test
    public void testCheckLocksSucceedsWithSeniorAndJuniorClaimsBySelf() throws InterruptedException, StorageException {
        // Setup three lock columns differing only in timestamp
        StaticBuffer myFirstLockCol = codec.toLockCol(currentTimeNS, defaultLockRid);
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);
        StaticBuffer mySecondLockCol = codec.toLockCol(currentTimeNS, defaultLockRid);
        LockStatus mySecondLS = new LockStatus(currentTimeNS, TimeUnit.NANOSECONDS);
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);
        StaticBuffer myThirdLockCol = codec.toLockCol(currentTimeNS, defaultLockRid);
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);
        
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, mySecondLS));
        
        // Return defaultTx's second lock in a map when requested
        currentTimeNS += TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
        expect(times.sleepUntil(mySecondLS.getWrittenTimestamp(TimeUnit.NANOSECONDS) + conf.getLockWait(TimeUnit.NANOSECONDS))).andReturn(currentTimeNS);

        // When the checker slices the store, return the senior lock col by a
        // foreign tx and the junior lock col by defaultTx (in that order)
        recordLockGetSlice(ImmutableList.<Entry>of(
                new StaticBufferEntry(myFirstLockCol,  defaultLockVal),
                new StaticBufferEntry(mySecondLockCol, defaultLockVal),
                new StaticBufferEntry(myThirdLockCol,  defaultLockVal)));

        ctrl.replay();
        
        locker.checkLocks(defaultTx);
    }
    
    /**
     * The checker should retry getSlice() in the face of a
     * TemporaryStorageException so long as the number of exceptional
     * getSlice()s is fewer than the lock retry count. The retry count applies
     * on a per-lock basis.
     * 
     * @throws StorageException
     *             shouldn't happen
     * @throws InterruptedException
     *             shouldn't happen
     */
    @Test
    public void testCheckLocksRetriesAfterSingleTemporaryStorageException() throws StorageException, InterruptedException {
        // Setup one lock column
        StaticBuffer lockCol = codec.toLockCol(currentTimeNS, defaultLockRid);
        LockStatus lockStatus = new LockStatus(currentTimeNS, TimeUnit.NANOSECONDS);
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);
        
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, lockStatus));
        
        expect(times.sleepUntil(lockStatus.getWrittenTimestamp(TimeUnit.NANOSECONDS) + conf.getLockWait(TimeUnit.NANOSECONDS))).andReturn(currentTimeNS);

        // First getSlice will fail
        TemporaryStorageException tse = new TemporaryStorageException("Storage cluster will be right back");
        recordExceptionalLockGetSlice(tse);
        
        // Second getSlice will succeed
        recordLockGetSliceAndReturnSingleEntry(new StaticBufferEntry(lockCol, defaultLockVal));
        
        ctrl.replay();
        
        locker.checkLocks(defaultTx);
        
        // TODO run again with two locks instead of one and show that the retry count applies on a per-lock basis
    }
    
    /**
     * The checker will throw a TemporaryStorageException if getSlice() throws
     * fails with a TemporaryStorageException as many times as there are
     * configured lock retries.
     * 
     * @throws InterruptedException
     *             shouldn't happen
     * @throws StorageException
     *             shouldn't happen
     */
    @Test
    public void testCheckLocksThrowsExceptionAfterMaxTemporaryStorageExceptions() throws InterruptedException, StorageException {
        // Setup a LockStatus for defaultLockID
        LockStatus lockStatus = new LockStatus(currentTimeNS, TimeUnit.NANOSECONDS);
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);
        
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, lockStatus));
        
        expect(times.sleepUntil(lockStatus.getWrittenTimestamp(TimeUnit.NANOSECONDS) + conf.getLockWait(TimeUnit.NANOSECONDS))).andReturn(currentTimeNS);

        // Three successive getSlice calls, each throwing a distinct TSE
        recordExceptionalLockGetSlice(new TemporaryStorageException("Storage cluster is having me-time"));
        recordExceptionalLockGetSlice(new TemporaryStorageException("Storage cluster is in a dissociative fugue state"));
        recordExceptionalLockGetSlice(new TemporaryStorageException("Storage cluster has gone to Prague to find itself"));
        
        ctrl.replay();
        
        TemporaryStorageException tse = null;
        try {
            locker.checkLocks(defaultTx);
        } catch (TemporaryStorageException e) {
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
     * @throws StorageException shouldn't happen
     */
    @Test
    public void testCheckLocksDiesOnPermanentStorageException() throws InterruptedException, StorageException {
        // Setup a LockStatus for defaultLockID
        LockStatus lockStatus = new LockStatus(currentTimeNS, TimeUnit.NANOSECONDS);
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);
        
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.of(defaultLockID, lockStatus));
        
        expect(times.sleepUntil(lockStatus.getWrittenTimestamp(TimeUnit.NANOSECONDS) + conf.getLockWait(TimeUnit.NANOSECONDS))).andReturn(currentTimeNS);

        // First and only getSlice call throws a PSE
        recordExceptionalLockGetSlice(new PermanentStorageException("Connection to storage cluster failed: peer is an IPv6 toaster"));
        
        ctrl.replay();
        
        PermanentStorageException pse = null;
        try {
            locker.checkLocks(defaultTx);
        } catch (PermanentStorageException e) {
            pse = e;
        }
        assertNotNull(pse);
    }
    
    @Test
    public void testCheckLocksDoesNothingForUnrecognizedTransaction() throws StorageException {
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.<KeyColumn, LockStatus>of());
        ctrl.replay();
        locker.checkLocks(defaultTx);
    }
    
    /**
     * Delete a single lock without any timeouts, errors, etc.
     * @throws StorageException shouldn't happen
     * 
     */
    @Test
    public void testDeleteLocksInSimplestCase() throws StorageException {
        // Setup a LockStatus for defaultLockID
        final LockStatus lockStatus = new LockStatus(currentTimeNS, TimeUnit.NANOSECONDS);
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);

        @SuppressWarnings("serial")
        Map<KeyColumn, LockStatus> expectedMap = new HashMap<KeyColumn, LockStatus>() {{
            put(defaultLockID, lockStatus);
        }};
        expect(lockState.getLocksForTx(defaultTx)).andReturn(expectedMap);
        
        StaticBuffer del = codec.toLockCol(lockStatus.getWrittenTimestamp(TimeUnit.NANOSECONDS), defaultLockRid);
        store.mutate(eq(defaultLockKey), eq(ImmutableList.<Entry>of()), eq(Arrays.asList(del)), eq(defaultTx));
        expect(mediator.unlock(defaultLockID, defaultTx)).andReturn(true);
        ctrl.replay();
        
        locker.deleteLocks(defaultTx);
    }
    
    @Test
    public void testDeleteLocksOnTwoLocks() throws StorageException {
        LockStatus defaultLS = new LockStatus(currentTimeNS, TimeUnit.NANOSECONDS);
        currentTimeNS++;
        LockStatus otherLS = new LockStatus(currentTimeNS, TimeUnit.NANOSECONDS);
        currentTimeNS++;
        
        // Expect a call for defaultTx's locks and return two
        Map<KeyColumn, LockStatus> expectedMap = Maps.newLinkedHashMap();
        expectedMap.put(defaultLockID, defaultLS);
        expectedMap.put(otherLockID, otherLS);
        expect(lockState.getLocksForTx(defaultTx)).andReturn(expectedMap);
        
        List<StaticBuffer> dels = ImmutableList.of(codec.toLockCol(defaultLS.getWrittenTimestamp(TimeUnit.NANOSECONDS), defaultLockRid));
        store.mutate(eq(defaultLockKey), eq(ImmutableList.<Entry>of()), eq(dels), eq(defaultTx));
        expect(mediator.unlock(defaultLockID, defaultTx)).andReturn(true);
        
        dels = ImmutableList.of(codec.toLockCol(otherLS.getWrittenTimestamp(TimeUnit.NANOSECONDS), defaultLockRid));
        store.mutate(eq(otherLockKey), eq(ImmutableList.<Entry>of()), eq(dels), eq(defaultTx));
        expect(mediator.unlock(otherLockID, defaultTx)).andReturn(true);
        ctrl.replay();
        
        locker.deleteLocks(defaultTx);
    }
    
    @Test
    public void testDeleteLocksRetriesOnTemporaryStorageException() throws StorageException {
        LockStatus defaultLS = new LockStatus(currentTimeNS, TimeUnit.NANOSECONDS);
        currentTimeNS++;
        expect(lockState.getLocksForTx(defaultTx)).andReturn(Maps.newLinkedHashMap(ImmutableMap.of(defaultLockID, defaultLS)));
        
        List<StaticBuffer> dels = ImmutableList.of(codec.toLockCol(defaultLS.getWrittenTimestamp(TimeUnit.NANOSECONDS), defaultLockRid));
        store.mutate(eq(defaultLockKey), eq(ImmutableList.<Entry>of()), eq(dels), eq(defaultTx));
        expectLastCall().andThrow(new TemporaryStorageException("Storage cluster is backlogged"));
        store.mutate(eq(defaultLockKey), eq(ImmutableList.<Entry>of()), eq(dels), eq(defaultTx));
        expect(mediator.unlock(defaultLockID, defaultTx)).andReturn(true);
//        lockState.release(defaultTx, defaultLockID);
        ctrl.replay();
        
        locker.deleteLocks(defaultTx);
    }
    
    @Test
    public void testDeleteLocksSkipsToNextLockAfterMaxTemporaryStorageExceptions() throws StorageException {
        LockStatus defaultLS = new LockStatus(currentTimeNS, TimeUnit.NANOSECONDS);
        currentTimeNS++;
        expect(lockState.getLocksForTx(defaultTx)).andReturn(Maps.newLinkedHashMap(ImmutableMap.of(defaultLockID, defaultLS)));
        
        List<StaticBuffer> dels = ImmutableList.of(codec.toLockCol(defaultLS.getWrittenTimestamp(TimeUnit.NANOSECONDS), defaultLockRid));
        store.mutate(eq(defaultLockKey), eq(ImmutableList.<Entry>of()), eq(dels), eq(defaultTx));
        expectLastCall().andThrow(new TemporaryStorageException("Storage cluster is busy"));
        store.mutate(eq(defaultLockKey), eq(ImmutableList.<Entry>of()), eq(dels), eq(defaultTx));
        expectLastCall().andThrow(new TemporaryStorageException("Storage cluster is busier"));
        store.mutate(eq(defaultLockKey), eq(ImmutableList.<Entry>of()), eq(dels), eq(defaultTx));
        expectLastCall().andThrow(new TemporaryStorageException("Storage cluster has reached peak business"));
        expect(mediator.unlock(defaultLockID, defaultTx)).andReturn(true);
//        lockState.release(defaultTx, defaultLockID);
        ctrl.replay();
        
        locker.deleteLocks(defaultTx);
    }
    
    @Test
    public void testDeleteLocksSkipsToNextLockOnPermanentStorageException() throws StorageException {
        LockStatus defaultLS = new LockStatus(currentTimeNS, TimeUnit.NANOSECONDS);
        currentTimeNS++;
        expect(lockState.getLocksForTx(defaultTx)).andReturn(Maps.newLinkedHashMap(ImmutableMap.of(defaultLockID, defaultLS)));
        
        List<StaticBuffer> dels = ImmutableList.of(codec.toLockCol(defaultLS.getWrittenTimestamp(TimeUnit.NANOSECONDS), defaultLockRid));
        store.mutate(eq(defaultLockKey), eq(ImmutableList.<Entry>of()), eq(dels), eq(defaultTx));
        expectLastCall().andThrow(new PermanentStorageException("Storage cluster has been destroyed by a tornado"));
        expect(mediator.unlock(defaultLockID, defaultTx)).andReturn(true);
//        lockState.release(defaultTx, defaultLockID);
        ctrl.replay();
        
        locker.deleteLocks(defaultTx);
    }
    
    @Test
    public void testDeleteLocksDeletesUncheckedLocks() throws StorageException {
        LockStatus defaultLS = new LockStatus(currentTimeNS, TimeUnit.NANOSECONDS);
        defaultLS.setChecked();
        assertTrue(defaultLS.isChecked());
        currentTimeNS++;
        
        // Expect a call for defaultTx's locks and the checked one
        expect(lockState.getLocksForTx(defaultTx)).andReturn(Maps.newLinkedHashMap(ImmutableMap.of(defaultLockID, defaultLS)));
        
        List<StaticBuffer> dels = ImmutableList.of(codec.toLockCol(defaultLS.getWrittenTimestamp(TimeUnit.NANOSECONDS), defaultLockRid));
        store.mutate(eq(defaultLockKey), eq(ImmutableList.<Entry>of()), eq(dels), eq(defaultTx));
        expect(mediator.unlock(defaultLockID, defaultTx)).andReturn(true);
//        lockState.release(defaultTx, defaultLockID);
        ctrl.replay();
        
        locker.deleteLocks(defaultTx);
    }
    
    @Test
    public void testDeleteLocksIdempotence() throws StorageException {
        // Setup a LockStatus for defaultLockID
        LockStatus lockStatus = new LockStatus(currentTimeNS, TimeUnit.NANOSECONDS);
        currentTimeNS += TimeUnit.NANOSECONDS.convert(1, TimeUnit.NANOSECONDS);
        
        expect(lockState.getLocksForTx(defaultTx)).andReturn(Maps.newLinkedHashMap(ImmutableMap.of(defaultLockID, lockStatus)));
        
        StaticBuffer del = codec.toLockCol(lockStatus.getWrittenTimestamp(TimeUnit.NANOSECONDS), defaultLockRid);
        store.mutate(eq(defaultLockKey), eq(ImmutableList.<Entry>of()), eq(Arrays.asList(del)), eq(defaultTx));
        expect(mediator.unlock(defaultLockID, defaultTx)).andReturn(true);
//        lockState.release(defaultTx, defaultLockID);
        ctrl.replay();
        
        locker.deleteLocks(defaultTx);
        
        ctrl.verify();
        ctrl.reset();
        expect(lockState.getLocksForTx(defaultTx)).andReturn(Maps.newLinkedHashMap(ImmutableMap.<KeyColumn, LockStatus>of()));
        ctrl.replay();
        locker.deleteLocks(defaultTx);
    }
    
    @Test
    public void testDeleteLocksDoesNothingForUnrecognizedTransaction() throws StorageException {
        expect(lockState.getLocksForTx(defaultTx)).andReturn(ImmutableMap.<KeyColumn, LockStatus>of());
        ctrl.replay();
        locker.deleteLocks(defaultTx);
    }
    
    /*
     * Helpers
     */
    
    private StaticBuffer recordSuccessfulLockWrite(long duration, TimeUnit tu, StaticBuffer del) throws StorageException {
        expect(times.getApproxNSSinceEpoch(false)).andReturn(++currentTimeNS);
        
        StaticBuffer lockCol = codec.toLockCol(currentTimeNS, defaultLockRid);
        Entry add = new StaticBufferEntry(lockCol, defaultLockVal);

        StaticBuffer k = eq(defaultLockKey);
        assert null != add;
        final List<Entry> adds = eq(Arrays.<Entry>asList(add));
        assert null != adds;
        final List<StaticBuffer> dels;
        if (null != del) {
            dels = eq(Arrays.<StaticBuffer>asList(del));
        } else {
            dels = eq(ImmutableList.<StaticBuffer>of());
        }
        store.mutate(k, adds, dels, eq(defaultTx));

        currentTimeNS += TimeUnit.NANOSECONDS.convert(duration, tu);
        expect(times.getApproxNSSinceEpoch(false)).andReturn(currentTimeNS);
        
        return lockCol;
    }
    
    private StaticBuffer recordExceptionLockWrite(long duration, TimeUnit tu, StaticBuffer del, Throwable t) throws StorageException {
        expect(times.getApproxNSSinceEpoch(false)).andReturn(++currentTimeNS);
        
        StaticBuffer lockCol = codec.toLockCol(currentTimeNS, defaultLockRid);
        Entry add = new StaticBufferEntry(lockCol, defaultLockVal);

        StaticBuffer k = eq(defaultLockKey);
        assert null != add;
        final List<Entry> adds = eq(Arrays.<Entry>asList(add));
        assert null != adds;
        final List<StaticBuffer> dels;
        if (null != del) {
            dels = eq(Arrays.<StaticBuffer>asList(del));
        } else {
            dels = eq(ImmutableList.<StaticBuffer>of());
        }
        store.mutate(k, adds, dels, eq(defaultTx));
        expectLastCall().andThrow(t);
        
        currentTimeNS += TimeUnit.NANOSECONDS.convert(duration, tu);
        expect(times.getApproxNSSinceEpoch(false)).andReturn(currentTimeNS);
        
        return lockCol;
    }
    
    private void recordSuccessfulLockDelete(long duration, TimeUnit tu, StaticBuffer del) throws StorageException {
        expect(times.getApproxNSSinceEpoch(false)).andReturn(++currentTimeNS);
        store.mutate(eq(defaultLockKey), eq(ImmutableList.<Entry>of()), eq(Arrays.asList(del)), eq(defaultTx));

        currentTimeNS += TimeUnit.NANOSECONDS.convert(duration, tu);
        expect(times.getApproxNSSinceEpoch(false)).andReturn(currentTimeNS);
    }
    
    private void recordSuccessfulLocalLock() {
        expect(times.getApproxNSSinceEpoch(false)).andReturn(++currentTimeNS);
        expect(mediator.lock(defaultLockID, defaultTx, currentTimeNS + defaultExpireNS, TimeUnit.NANOSECONDS)).andReturn(true);
    }
    
    private void recordSuccessfulLocalLock(long ts) {
        expect(mediator.lock(defaultLockID, defaultTx, ts + defaultExpireNS, TimeUnit.NANOSECONDS)).andReturn(true);
    }
    
    private void recordFailedLocalLock() {
        expect(times.getApproxNSSinceEpoch(false)).andReturn(++currentTimeNS);
        expect(mediator.lock(defaultLockID, defaultTx, currentTimeNS + defaultExpireNS, TimeUnit.NANOSECONDS)).andReturn(false);
    }
    
    private void recordSuccessfulLocalUnlock() {
        expect(mediator.unlock(defaultLockID, defaultTx)).andReturn(true);
    }
    
    private void recordLockGetSlice(List<Entry> returnedEntries) throws StorageException {
        final StaticBuffer lower = ByteBufferUtil.zeroBuffer(9);
        final StaticBuffer upper = ByteBufferUtil.oneBuffer(9);
        final KeySliceQuery ksq = new KeySliceQuery(defaultLockKey, lower, upper);
        expect(store.getSlice(eq(ksq), eq(defaultTx))).andReturn(returnedEntries);
    }
    
    private void recordExceptionalLockGetSlice(Throwable t) throws StorageException {
        final StaticBuffer lower = ByteBufferUtil.zeroBuffer(9);
        final StaticBuffer upper = ByteBufferUtil.oneBuffer(9);
        final KeySliceQuery ksq = new KeySliceQuery(defaultLockKey, lower, upper);
        expect(store.getSlice(eq(ksq), eq(defaultTx))).andThrow(t);
    }
    
    private void recordLockGetSliceAndReturnSingleEntry(Entry returnSingleEntry) throws StorageException {
        recordLockGetSlice(ImmutableList.<Entry>of(returnSingleEntry));
    }
 
//    private static class Sleeper<T> implements IAnswer<T> {
//
//        private final T ret;
//        private final long sleepMS;
//        
//        public Sleeper(T ret, long sleep, TimeUnit tu) {
//            this.ret = ret;
//            this.sleepMS = TimeUnit.MILLISECONDS.convert(sleep, tu);
//        }
//
//        @Override
//        public T answer() throws Throwable {
//            
//            long start = System.currentTimeMillis();
//            long finish = start;
//            long slept = 0L;
//            long remaining = sleepMS;
//            do {
//                Thread.sleep(remaining);
//                finish = System.currentTimeMillis();
//                slept = finish - start;
//                remaining = sleepMS - slept;
//            } while (0 < remaining);
//            
//            return ret;
//        }
//        
//    }
}
