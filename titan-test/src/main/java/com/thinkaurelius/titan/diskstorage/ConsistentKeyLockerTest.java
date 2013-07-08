package com.thinkaurelius.titan.diskstorage;

import static org.easymock.EasyMock.createStrictControl;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockStore;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLocker;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLocker.LockStatus;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockerConfiguration;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockerSerializer;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.TimestampRid;
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
    private final StaticBuffer defaultLockRid = new StaticArrayBuffer(new byte[] { (byte)8 });
    private final StaticBuffer otherLockRid = new StaticArrayBuffer(new byte[] { (byte)16 });
    private final StaticBuffer defaultLockVal = ByteBufferUtil.getIntBuffer(0); // maybe refactor...
    private StoreTransaction defaultTx;
    
    private final long defaultWaitNS =          100 * 1000 * 1000;
    private final long defaultExpireNS = 30L * 1000 * 1000 * 1000;
    
    private IMocksControl ctrl;
    private long currentTimeNS;
    private TimestampProvider times;
    private KeyColumnValueStore store;
    private LocalLockMediator<StoreTransaction> mediator;
    private ConsistentKeyLockerConfiguration conf;
    private ConsistentKeyLocker locker;
    
    @SuppressWarnings("unchecked")
    @Before
    public void setupMocks() {
        currentTimeNS = 0;
        ctrl = createStrictControl();
        defaultTx = ctrl.createMock(StoreTransaction.class);
        times = ctrl.createMock(TimestampProvider.class);
        store = ctrl.createMock(KeyColumnValueStore.class);
        mediator = ctrl.createMock(LocalLockMediator.class);
        conf = new ConsistentKeyLockerConfiguration.Builder(
                store).times(times).mediator(mediator)
                .lockExpireNS(defaultExpireNS, TimeUnit.NANOSECONDS)
                .lockWaitNS(defaultWaitNS, TimeUnit.NANOSECONDS)
                .rid(defaultLockRid).build();
        locker = new ConsistentKeyLocker(conf);
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

        recordSuccessfulLocalLock();
        StaticBuffer lockCol = recordSuccessfulLockWrite(1, TimeUnit.NANOSECONDS, null);
        recordSuccessfulLocalLock(codec.fromLockColumn(lockCol).getTimestamp());
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
        recordSuccessfulLocalLock();
        StaticBuffer firstCol  = recordSuccessfulLockWrite(5, TimeUnit.SECONDS, null); // too slow
        StaticBuffer secondCol = recordSuccessfulLockWrite(1, TimeUnit.NANOSECONDS, firstCol); // plenty fast
        recordSuccessfulLocalLock(codec.fromLockColumn(secondCol).getTimestamp());
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
        TemporaryStorageException tse = new TemporaryStorageException("Storage cluster is super lazy");
        recordSuccessfulLocalLock();
        StaticBuffer firstCol  = recordExceptionLockWrite(1, TimeUnit.NANOSECONDS, null, tse);
        StaticBuffer secondCol = recordSuccessfulLockWrite(1, TimeUnit.NANOSECONDS, firstCol);
        recordSuccessfulLocalLock(codec.fromLockColumn(secondCol).getTimestamp());
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
        recordSuccessfulLocalLock();
        StaticBuffer firstCol = recordSuccessfulLockWrite(1, TimeUnit.NANOSECONDS, null);
        recordSuccessfulLocalLock(codec.fromLockColumn(firstCol).getTimestamp());
        ctrl.replay();
        
        locker.writeLock(defaultLockID, defaultTx);
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
        // Fake a pre-existing lock with mocks and stubs
        final long lockTime = currentTimeNS;
        StaticBuffer existingLockCol = codec.toLockCol(lockTime, defaultLockRid);
        StaticBuffer existingLockVal = defaultLockVal;
        Map<KeyColumn, LockStatus> lockMap =
                ImmutableMap.of(defaultLockID, new LockStatus(lockTime, TimeUnit.NANOSECONDS));
        currentTimeNS += TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
        
        @SuppressWarnings("unchecked")
        ConcurrentMap<StoreTransaction, Map<KeyColumn, LockStatus>> locks = ctrl.createMock(ConcurrentMap.class);
        
        // Return the faked lock in a map of size 1
        expect(locks.get(defaultTx)).andReturn(lockMap);
        // Checker should compare the fake lock's timestamp to the current time
//        expect(times.getApproxNSSinceEpoch(false)).andReturn(currentTimeNS);
        expect(times.sleepUntil(lockTime + conf.getLockWait(TimeUnit.NANOSECONDS))).andReturn(currentTimeNS);

        // Checker must fetch all columns on the appropriate key
        StaticBuffer lower = ByteBufferUtil.zeroBuffer(9);
        StaticBuffer upper = ByteBufferUtil.oneBuffer(9);
        KeySliceQuery ksq = new KeySliceQuery(defaultLockKey, lower, upper);
        expect(store.getSlice(eq(ksq), eq(defaultTx)))
            .andReturn(ImmutableList.<Entry>of(new StaticBufferEntry(existingLockCol, existingLockVal)));

        ctrl.replay();
        
        locker = new ConsistentKeyLocker(conf, locks);
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
        // Fake a pre-existing lock with mocks and stubs
        final long lockTime = currentTimeNS;
        StaticBuffer existingLockCol = codec.toLockCol(lockTime, defaultLockRid);
        StaticBuffer existingLockVal = defaultLockVal;
        Map<KeyColumn, LockStatus> lockMap =
                ImmutableMap.of(defaultLockID, new LockStatus(lockTime, TimeUnit.NANOSECONDS));
        currentTimeNS += TimeUnit.NANOSECONDS.convert(100, TimeUnit.DAYS);
        
        @SuppressWarnings("unchecked")
        ConcurrentMap<StoreTransaction, Map<KeyColumn, LockStatus>> locks = ctrl.createMock(ConcurrentMap.class);
        
        // Return the faked lock in a map of size 1
        expect(locks.get(defaultTx)).andReturn(lockMap);
        // Checker should compare the fake lock's timestamp to the current time
//        expect(times.getApproxNSSinceEpoch(false)).andReturn(currentTimeNS);
        expect(times.sleepUntil(lockTime + conf.getLockWait(TimeUnit.NANOSECONDS))).andReturn(currentTimeNS);

        // Checker must fetch all columns on the appropriate key
        StaticBuffer lower = ByteBufferUtil.zeroBuffer(9);
        StaticBuffer upper = ByteBufferUtil.oneBuffer(9);
        KeySliceQuery ksq = new KeySliceQuery(defaultLockKey, lower, upper);
        expect(store.getSlice(eq(ksq), eq(defaultTx)))
            .andReturn(ImmutableList.<Entry>of(new StaticBufferEntry(existingLockCol, existingLockVal)));

        ctrl.replay();
        locker = new ConsistentKeyLocker(conf, locks);
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
     * checkLocks() twice.  The second call should have no effect.
     * @throws InterruptedException 
     * @throws StorageException 
     */
    @Test
    public void testCheckLocksIdempotence() throws InterruptedException, StorageException {
        // Fake a pre-existing lock with mocks and stubs
        final long lockTime = currentTimeNS;
        StaticBuffer existingLockCol = codec.toLockCol(lockTime, defaultLockRid);
        StaticBuffer existingLockVal = defaultLockVal;
        Map<KeyColumn, LockStatus> lockMap =
                ImmutableMap.of(defaultLockID, new LockStatus(lockTime, TimeUnit.NANOSECONDS));
        currentTimeNS += TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
        
        @SuppressWarnings("unchecked")
        ConcurrentMap<StoreTransaction, Map<KeyColumn, LockStatus>> locks = ctrl.createMock(ConcurrentMap.class);
        
        // Return the faked lock in a map of size 1
        expect(locks.get(defaultTx)).andReturn(lockMap);
        // Checker should compare the fake lock's timestamp to the current time
//        expect(times.getApproxNSSinceEpoch(false)).andReturn(currentTimeNS);
        expect(times.sleepUntil(lockTime + conf.getLockWait(TimeUnit.NANOSECONDS))).andReturn(currentTimeNS);

        // Checker must fetch all columns on the appropriate key
        StaticBuffer lower = ByteBufferUtil.zeroBuffer(9);
        StaticBuffer upper = ByteBufferUtil.oneBuffer(9);
        KeySliceQuery ksq = new KeySliceQuery(defaultLockKey, lower, upper);
        expect(store.getSlice(eq(ksq), eq(defaultTx)))
            .andReturn(ImmutableList.<Entry>of(new StaticBufferEntry(existingLockCol, existingLockVal)));
        ctrl.replay();
        locker = new ConsistentKeyLocker(conf, locks);
        locker.checkLocks(defaultTx);
        ctrl.verify();
        
        ctrl.reset();
        // Return the faked lock in a map of size 1
        expect(locks.get(defaultTx)).andReturn(lockMap);
        ctrl.replay();
        // At this point, checkLocks() should see that the single lock in the
        // map returned above has already been checked and return immediately
        
        locker.checkLocks(defaultTx);
    }
    
    @Test
    public void testCheckLocksFailsWithSeniorClaimsByOthers() throws InterruptedException, StorageException {
        // Fake pre-existing locks with mocks and stubs
        final long lockTime = currentTimeNS;
        StaticBuffer seniorLockCol = codec.toLockCol(lockTime-1, otherLockRid);
        StaticBuffer existingLockCol = codec.toLockCol(lockTime, defaultLockRid);
        StaticBuffer existingLockVal = defaultLockVal;
        Map<KeyColumn, LockStatus> lockMap =
                ImmutableMap.of(defaultLockID, new LockStatus(lockTime, TimeUnit.NANOSECONDS));
        currentTimeNS += TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
        
        @SuppressWarnings("unchecked")
        ConcurrentMap<StoreTransaction, Map<KeyColumn, LockStatus>> locks = ctrl.createMock(ConcurrentMap.class);
        
        // Return the faked lock in a map of size 1
        expect(locks.get(defaultTx)).andReturn(lockMap);
        // Checker should compare the fake lock's timestamp to the current time
//        expect(times.getApproxNSSinceEpoch(false)).andReturn(currentTimeNS);
        expect(times.sleepUntil(lockTime + conf.getLockWait(TimeUnit.NANOSECONDS))).andReturn(currentTimeNS);

        // Checker must fetch all columns on the appropriate key
        StaticBuffer lower = ByteBufferUtil.zeroBuffer(9);
        StaticBuffer upper = ByteBufferUtil.oneBuffer(9);
        KeySliceQuery ksq = new KeySliceQuery(defaultLockKey, lower, upper);
        expect(store.getSlice(eq(ksq), eq(defaultTx)))
            .andReturn(ImmutableList.<Entry>of(
                    new StaticBufferEntry(seniorLockCol, defaultLockVal),
                    new StaticBufferEntry(existingLockCol, existingLockVal)));

        ctrl.replay();
        
        locker = new ConsistentKeyLocker(conf, locks);
        
        TemporaryLockingException tle = null;
        try {
            locker.checkLocks(defaultTx);
        } catch (TemporaryLockingException e) {
            tle = e;
        }
        assertNotNull(tle);
    }
    
    @Test
    public void testCheckLocksSucceedsWithJuniorClaimsByOthers() {
        
    }
    
    @Test
    public void testCheckLocksSucceedsWithSeniorClaimsBySelf() {
        
    }
    
    @Test
    public void testCheckLocksFailsWithJuniorClaimsBySelf() {
        
    }
    
    @Test
    public void testCheckLocksRetriesAfterOneStoreTimeout() {
        
    }
    
    @Test
    public void testCheckLocksThrowsExceptionAfterMaxStoreTimeouts() {
        
    }
    
    @Test
    public void testCheckLocksDiesOnPermanentStorageException() {
        
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
            dels = EasyMock.<List<StaticBuffer>>isNull();
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
            dels = EasyMock.<List<StaticBuffer>>isNull();
        }
        store.mutate(k, adds, dels, eq(defaultTx));
        expectLastCall().andThrow(t);
        
        currentTimeNS += TimeUnit.NANOSECONDS.convert(duration, tu);
        expect(times.getApproxNSSinceEpoch(false)).andReturn(currentTimeNS);
        
        return lockCol;
    }
    
    private void recordSuccessfulLockDelete(long duration, TimeUnit tu, StaticBuffer del) throws StorageException {
        expect(times.getApproxNSSinceEpoch(false)).andReturn(++currentTimeNS);
        store.mutate(eq(defaultLockKey), EasyMock.<List<Entry>>isNull(), eq(Arrays.asList(del)), eq(defaultTx));

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
