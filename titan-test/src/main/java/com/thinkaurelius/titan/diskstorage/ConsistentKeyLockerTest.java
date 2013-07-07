package com.thinkaurelius.titan.diskstorage;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.easymock.EasyMock;
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
    
    @Before
    public void setupDefaultTx() {
        defaultTx = createMock(StoreTransaction.class);
        replay(defaultTx); // No method calls allowed on txh
    }
    
    @After
    public void tearDownDefaultTx() {
        verify(defaultTx);
    }
    
    /**
     * Test a single lock using stub objects. Doesn't test unlock ("leaks" the
     * lock, but since it's backed by stubs, it doesn't matter).
     * 
     * @throws StorageException shouldn't happen
     */
    @Test
    public void testWriteLockWithoutErrors() throws StorageException {

        // Stub timestamp calls
        TimestampProvider times = createMock(TimestampProvider.class);
        expect(times.getApproxNSSinceEpoch(false))
            .andReturn(defaultLockTimestamp-1)
            .andReturn(defaultLockTimestamp)
            .andReturn(defaultLockTimestamp+1);
        replay(times);

        // Stub store calls
        List<Entry> expectedAdditions = Arrays.<Entry>asList(new StaticBufferEntry(defaultLockCol, defaultLockVal));
        KeyColumnValueStore store = createMock(KeyColumnValueStore.class);
        store.mutate(eq(defaultLockKey), eq(expectedAdditions), EasyMock.<List<StaticBuffer>>isNull(), eq(defaultTx));
        replay(store);
        
        // Stub local lock mediator calls
        @SuppressWarnings("unchecked")
        LocalLockMediator<StoreTransaction> mediator  = createMock(LocalLockMediator.class);
        expect(mediator.lock(defaultLockID, defaultTx, defaultLockTimestamp - 1 + defaultExpireNS, TimeUnit.NANOSECONDS)).andReturn(true);
        replay(mediator);
        
        // Done with stubbing; create locker instance around stubs
        ConsistentKeyLockerConfiguration conf = new ConsistentKeyLockerConfiguration.Builder(store).times(times).mediator(mediator).lockExpireNS(defaultExpireNS, TimeUnit.NANOSECONDS).rid(defaultLockRid).build();
        ConsistentKeyLocker locker = new ConsistentKeyLocker(conf); 
        
        locker.writeLock(defaultLockID, defaultTx); // SUT
        
        // Check for unscripted method calls
        verify(store);
        verify(times);
        verify(mediator);
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
        // Stubbing and setup
        List<Entry> firstAddition = Arrays.<Entry> asList(new StaticBufferEntry(
                        defaultLockCol,
                        defaultLockVal));
        List<Entry> secondAddition = Arrays.<Entry> asList(new StaticBufferEntry(
                        codec.toLockCol(defaultLockTimestamp + 2000000000, defaultLockRid),
                        defaultLockVal));
        
        TimestampProvider times = createMock(TimestampProvider.class);
        expect(times.getApproxNSSinceEpoch(false))
            .andReturn(defaultLockTimestamp-1)           // passed to LocalLockMediator
            .andReturn(defaultLockTimestamp)             // before first attempt on remote lock
            .andReturn(defaultLockTimestamp+1999000000)  // after first attempt
            .andReturn(defaultLockTimestamp+2000000000)  // before second attempt
            .andReturn(defaultLockTimestamp+2001000000); // after second attempt
        replay(times);

        // Store that times out on first mutate, returns immediately on second mutate
        KeyColumnValueStore store = createMock(KeyColumnValueStore.class);
        // Expect initial lock write (this will timeout)
        store.mutate(eq(defaultLockKey), eq(firstAddition), EasyMock.<List<StaticBuffer>>isNull(), eq(defaultTx));
//        expectLastCall().andAnswer(new Sleeper<Void>(null, defaultWaitNS, TimeUnit.NANOSECONDS)); // inject timeout
        // Expect lock write with previous_ts+1 and try to delete old claim too (returns immediately)
        store.mutate(eq(defaultLockKey), eq(secondAddition), eq(Arrays.asList(defaultLockCol)), eq(defaultTx));
        expect(store.getName()).andReturn("blah").times(0, 1);
        replay(store);
        
        // Done with stubbing; create locker instance around stubs
        ConsistentKeyLockerConfiguration conf = new ConsistentKeyLockerConfiguration.Builder(store).times(times).lockWaitNS(defaultWaitNS, TimeUnit.NANOSECONDS).rid(defaultLockRid).build();
        ConsistentKeyLocker locker = new ConsistentKeyLocker(conf); 
        
        locker.writeLock(defaultLockID, defaultTx); // SUT
        
        // check for unscripted method calls
        verify(store);
        verify(times);
        
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
        // Stubbing and setup
        List<Entry> firstAddition = Arrays.<Entry> asList(new StaticBufferEntry(
                defaultLockCol,
                defaultLockVal));
        StaticBuffer secondLockCol = codec.toLockCol(defaultLockTimestamp + 2000000000L, defaultLockRid);
        List<Entry> secondAddition = Arrays.<Entry> asList(new StaticBufferEntry(
                secondLockCol,
                defaultLockVal));
        StaticBuffer thirdLockCol = codec.toLockCol(defaultLockTimestamp + 4000000000L, defaultLockRid);
        List<Entry> thirdAddition = Arrays.<Entry> asList(new StaticBufferEntry(
                thirdLockCol,
                defaultLockVal));
        
        // Stub timestamp calls
        TimestampProvider times = createMock(TimestampProvider.class);
        expect(times.getApproxNSSinceEpoch(false))
            .andReturn(defaultLockTimestamp-1)            // passed to LocalLockMediator
            .andReturn(defaultLockTimestamp)              // before first attempt on remote lock
            .andReturn(defaultLockTimestamp+1999000000L)  // after first attempt
            .andReturn(defaultLockTimestamp+2000000000L)  // before second attempt
            .andReturn(defaultLockTimestamp+3999000000L)  // after second attempt
            .andReturn(defaultLockTimestamp+4000000000L)  // before third attempt
            .andReturn(defaultLockTimestamp+5999000000L)  // after third attempt
            .andReturn(defaultLockTimestamp+6000000000L)  // before final post-failure cleanup deletion
            .andReturn(defaultLockTimestamp+6001000000L); // after final deletion
        replay(times);

        // Stub store calls
        KeyColumnValueStore store = createMock(KeyColumnValueStore.class);
        // First attempt
        store.mutate(eq(defaultLockKey), eq(firstAddition), EasyMock.<List<StaticBuffer>>isNull(), eq(defaultTx));
        // Second attempt
        store.mutate(eq(defaultLockKey), eq(secondAddition), eq(Arrays.asList(defaultLockCol)), eq(defaultTx));
        // Third attempt
        store.mutate(eq(defaultLockKey), eq(thirdAddition), eq(Arrays.asList(secondLockCol)), eq(defaultTx));
        // Final deletion
        store.mutate(eq(defaultLockKey), EasyMock.<List<Entry>>isNull(), eq(Arrays.asList(thirdLockCol)), eq(defaultTx));
        replay(store);
        
        // Stub local lock mediator calls
        @SuppressWarnings("unchecked")
        LocalLockMediator<StoreTransaction> mediator  = createMock(LocalLockMediator.class);
        expect(mediator.lock(defaultLockID, defaultTx, defaultLockTimestamp - 1 + defaultExpireNS, TimeUnit.NANOSECONDS)).andReturn(true);
        expect(mediator.unlock(defaultLockID, defaultTx)).andReturn(true);
        replay(mediator);
        
        // Done with stubbing; create locker instance around stubs
        ConsistentKeyLockerConfiguration conf = new ConsistentKeyLockerConfiguration.Builder(store).times(times).mediator(mediator).lockExpireNS(defaultExpireNS, TimeUnit.NANOSECONDS).rid(defaultLockRid).build();
        ConsistentKeyLocker locker = new ConsistentKeyLocker(conf); 
        
        StorageException expected = null;
        try {
            locker.writeLock(defaultLockID, defaultTx); // SUT
        } catch (TemporaryStorageException e) {
            expected = e;
        }
        assertNotNull(expected);
        
        // Check for unscripted method calls
        verify(store);
        verify(times);
        verify(mediator);
    }
    
    /**
     * Test that the first {@link @PermanentStorageException} thrown by the
     * locker's store causes it to attempt to delete outstanding lock writes and
     * then emit the exception without retrying.
     * 
     * @throws StorageException shouldn't happen
     */
    @Test
    public void testWriteLockDiesOnPermanentStorageException() throws StorageException {
        // Stubbing and setup
        List<Entry> firstAddition = Arrays.<Entry> asList(new StaticBufferEntry(
                defaultLockCol,
                defaultLockVal));
        
        // Stub timestamp calls
        TimestampProvider times = createMock(TimestampProvider.class);
        expect(times.getApproxNSSinceEpoch(false))
            .andReturn(defaultLockTimestamp-1)   // passed to LocalLockMediator
            .andReturn(defaultLockTimestamp)     // before first attempt on remote lock
            .andReturn(defaultLockTimestamp+1L)  // after first (PermanentStorageException-throwing) attempt
            .andReturn(defaultLockTimestamp+2L)  // before cleanup delete attempt
            .andReturn(defaultLockTimestamp+3L); // after cleanup delete attempt
        replay(times);

        // Stub store calls
        KeyColumnValueStore store = createMock(KeyColumnValueStore.class);
        // First attempt (throws PSE)
        store.mutate(eq(defaultLockKey), eq(firstAddition), EasyMock.<List<StaticBuffer>>isNull(), eq(defaultTx));
        PermanentStorageException pse = new PermanentStorageException("Storage cluster is on fire");
        expectLastCall().andThrow(pse);
        // Final deletion
        store.mutate(eq(defaultLockKey), EasyMock.<List<Entry>>isNull(), eq(Arrays.asList(defaultLockCol)), eq(defaultTx));
        replay(store);
        
        // Stub local lock mediator calls
        @SuppressWarnings("unchecked")
        LocalLockMediator<StoreTransaction> mediator  = createMock(LocalLockMediator.class);
        expect(mediator.lock(defaultLockID, defaultTx, defaultLockTimestamp - 1 + defaultExpireNS, TimeUnit.NANOSECONDS)).andReturn(true);
        expect(mediator.unlock(defaultLockID, defaultTx)).andReturn(true);
        replay(mediator);
        
        // Done with stubbing; create locker instance around stubs
        ConsistentKeyLockerConfiguration conf = new ConsistentKeyLockerConfiguration.Builder(store).times(times).mediator(mediator).lockExpireNS(defaultExpireNS, TimeUnit.NANOSECONDS).rid(defaultLockRid).build();
        ConsistentKeyLocker locker = new ConsistentKeyLocker(conf); 
        
        StorageException expected = null;
        try {
            locker.writeLock(defaultLockID, defaultTx); // SUT
        } catch (PermanentLockingException e) {
            expected = e;
        }
        assertNotNull(expected);
        assertEquals(pse, expected.getCause());
        
        // Check for unscripted method calls
        verify(store);
        verify(times);
        verify(mediator);
    }
    
    @Test
    public void testWriteLockRetriesOnTemporaryStorageException() throws StorageException {
        // Stubbing and setup
        List<Entry> firstAddition = Arrays.<Entry> asList(new StaticBufferEntry(
                defaultLockCol,
                defaultLockVal));
        StaticBuffer secondLockCol = codec.toLockCol(defaultLockTimestamp + 2L, defaultLockRid);
        List<Entry> secondAddition = Arrays.<Entry> asList(new StaticBufferEntry(
                secondLockCol,
                defaultLockVal));
        
        // Stub timestamp calls
        TimestampProvider times = createMock(TimestampProvider.class);
        expect(times.getApproxNSSinceEpoch(false))
            .andReturn(defaultLockTimestamp-1)   // passed to LocalLockMediator
            .andReturn(defaultLockTimestamp)     // before first attempt on remote lock
            .andReturn(defaultLockTimestamp+1L)  // after first attempt (which threw a TemporaryStorageException)
            .andReturn(defaultLockTimestamp+2L)  // before the second attempt
            .andReturn(defaultLockTimestamp+3L); // after the second attempt (which succeeded)
        replay(times);

        // Stub store calls
        KeyColumnValueStore store = createMock(KeyColumnValueStore.class);
        // First attempt (throws TSE)
        store.mutate(eq(defaultLockKey), eq(firstAddition), EasyMock.<List<StaticBuffer>>isNull(), eq(defaultTx));
        TemporaryStorageException tse = new TemporaryStorageException("Storage cluster is super lazy");
        expectLastCall().andThrow(tse);
        // Second attempt
        store.mutate(eq(defaultLockKey), eq(secondAddition), eq(Arrays.asList(defaultLockCol)), eq(defaultTx));
        replay(store);
        
        // Stub local lock mediator calls
        @SuppressWarnings("unchecked")
        LocalLockMediator<StoreTransaction> mediator  = createMock(LocalLockMediator.class);
        expect(mediator.lock(defaultLockID, defaultTx, defaultLockTimestamp - 1 + defaultExpireNS, TimeUnit.NANOSECONDS)).andReturn(true);
        replay(mediator);
        
        // Done with stubbing; create locker instance around stubs
        ConsistentKeyLockerConfiguration conf = new ConsistentKeyLockerConfiguration.Builder(store).times(times).mediator(mediator).lockExpireNS(defaultExpireNS, TimeUnit.NANOSECONDS).rid(defaultLockRid).build();
        ConsistentKeyLocker locker = new ConsistentKeyLocker(conf); 
        
        locker.writeLock(defaultLockID, defaultTx); // SUT
        
        // Check for unscripted method calls
        verify(store);
        verify(times);
        verify(mediator);
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
