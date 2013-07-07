package com.thinkaurelius.titan.diskstorage;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockCodec;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLocker;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockerConfiguration;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;


public class ConsistentKeyLockerTest {
    
    // Arbitrary literals -- the exact values assigned here are not intrinsically important
    private final ConsistentKeyLockCodec codec = new ConsistentKeyLockCodec();
    private final StaticBuffer defaultDataKey = ByteBufferUtil.getIntBuffer(2);
    private final StaticBuffer defaultDataCol = ByteBufferUtil.getIntBuffer(4);
    private final StaticBuffer defaultLockKey = codec.toLockKey(defaultDataKey, defaultDataCol);
    private final KeyColumn defaultLockID = new KeyColumn(defaultDataKey, defaultDataCol);
    private final StaticBuffer defaultLockRid = new StaticArrayBuffer(new byte[] { (byte)8 });
    private final long defaultLockTimestamp = 42L;     
    private final StaticBuffer defaultLockCol = codec.toLockCol(defaultLockTimestamp, defaultLockRid);
    private final StaticBuffer defaultLockVal = ByteBufferUtil.getIntBuffer(0); // maybe refactor...
    private StoreTransaction defaultTx;
    
    private final long defaultWaitNS =        100 * 1000 * 1000;
    private final long defaultExpireNS = 10 * 100 * 1000 * 1000;
    
    private IMocksControl ctrl;
    private long currentTimeNS;
    private TimestampProvider times;
    private KeyColumnValueStore store;
    private LocalLockMediator<StoreTransaction> mediator;
    private ConsistentKeyLocker locker;
    
    @SuppressWarnings("unchecked")
    @Before
    public void setupDefaultTx() {
        currentTimeNS = 0;
        ctrl = createStrictControl();
        defaultTx = ctrl.createMock(StoreTransaction.class);
        times = ctrl.createMock(TimestampProvider.class);
        store = ctrl.createMock(KeyColumnValueStore.class);
        mediator = ctrl.createMock(LocalLockMediator.class);
        ConsistentKeyLockerConfiguration conf = new ConsistentKeyLockerConfiguration.Builder(
                store).times(times).mediator(mediator)
                .lockExpireNS(defaultExpireNS, TimeUnit.NANOSECONDS)
                .rid(defaultLockRid).build();
        locker = new ConsistentKeyLocker(conf);
    }
    
    @After
    public void tearDownDefaultTx() {
        ctrl.verify();
    }
    
    /**
     * Test a single lock using stub objects. Doesn't test unlock ("leaks" the
     * lock, but since it's backed by stubs, it doesn't matter).
     * 
     * @throws StorageException shouldn't happen
     */
    @Test
    public void testWriteLockWithoutErrors() throws StorageException {

        recordSuccessfulLocalLock();
        recordSuccessfulLockWrite(1, TimeUnit.NANOSECONDS, null);
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
        StaticBuffer firstCol = recordSuccessfulLockWrite(5, TimeUnit.SECONDS, null); // too slow
        recordSuccessfulLockWrite(1, TimeUnit.NANOSECONDS, firstCol); // plenty fast
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
        recordSuccessfulLockWrite(1, TimeUnit.NANOSECONDS, firstCol);
        ctrl.replay();
        
        locker.writeLock(defaultLockID, defaultTx); // SUT
    }
    
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
