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

package org.janusgraph.diskstorage;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KCVSUtil;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.locking.LocalLockMediators;
import org.janusgraph.diskstorage.locking.Locker;
import org.janusgraph.diskstorage.locking.LockerProvider;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.janusgraph.diskstorage.locking.TemporaryLockingException;
import org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLocker;
import org.janusgraph.diskstorage.locking.consistentkey.ExpectedValueCheckingStore;
import org.janusgraph.diskstorage.locking.consistentkey.ExpectedValueCheckingStoreManager;
import org.janusgraph.diskstorage.locking.consistentkey.ExpectedValueCheckingTransaction;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.KeyColumn;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore.NO_DELETIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class LockKeyColumnValueStoreTest extends AbstractKCVSTest {

    private static final Logger log =
            LoggerFactory.getLogger(LockKeyColumnValueStoreTest.class);

    public static final int CONCURRENCY = 8;
    public static final int NUM_TX = 2;
    public static final String DB_NAME = "test";
    protected static final long EXPIRE_MS = 5000L;

    /*
     * Don't change these back to static. We can run test classes concurrently
     * now. There are multiple concrete subclasses of this abstract class. If
     * the subclasses run in separate threads and were to concurrently mutate
     * static state on this common superclass, then thread safety fails.
     *
     * Anything final and deeply immutable is of course fair game for static,
     * but these are mutable.
     */
    public KeyColumnValueStoreManager[] manager;
    public StoreTransaction[][] tx;
    public KeyColumnValueStore[] store;

    private StaticBuffer k, c1, c2, v1, v2;

    protected final String concreteClassName;

    public LockKeyColumnValueStoreTest() {
        concreteClassName = getClass().getSimpleName();
    }

    @BeforeEach
    public void setUp() throws Exception {

        StoreManager tmp = null;
        try {
            tmp = openStorageManager(0, GraphDatabaseConfiguration.buildGraphConfiguration());
            tmp.clearStorage();
        } finally {
            tmp.close();
        }

        for (int i = 0; i < CONCURRENCY; i++) {
            LocalLockMediators.INSTANCE.clear(concreteClassName + i);
        }

        open();
        k = KeyValueStoreUtil.getBuffer("testkey");
        c1 = KeyValueStoreUtil.getBuffer("col1");
        c2 = KeyValueStoreUtil.getBuffer("col2");
        v1 = KeyValueStoreUtil.getBuffer("val1");
        v2 = KeyValueStoreUtil.getBuffer("val2");
    }

    public abstract KeyColumnValueStoreManager openStorageManager(int id, Configuration configuration) throws BackendException;

    public void open() throws BackendException {
        manager = new KeyColumnValueStoreManager[CONCURRENCY];
        tx = new StoreTransaction[CONCURRENCY][NUM_TX];
        store = new KeyColumnValueStore[CONCURRENCY];

        for (int i = 0; i < CONCURRENCY; i++) {
            final ModifiableConfiguration sc = GraphDatabaseConfiguration.buildGraphConfiguration();
            sc.set(GraphDatabaseConfiguration.LOCK_LOCAL_MEDIATOR_GROUP,concreteClassName + i);
            sc.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID,"inst"+i);
            sc.set(GraphDatabaseConfiguration.LOCK_RETRY,10);
            sc.set(GraphDatabaseConfiguration.LOCK_EXPIRE, Duration.ofMillis(EXPIRE_MS));

            manager[i] = openStorageManager(i, sc);
            StoreFeatures storeFeatures = manager[i].getFeatures();
            store[i] = manager[i].openDatabase(DB_NAME);
            for (int j = 0; j < NUM_TX; j++) {
                tx[i][j] = manager[i].beginTransaction(getTxConfig());
                log.debug("Began transaction of class {}", tx[i][j].getClass().getCanonicalName());
            }

            if (!storeFeatures.hasLocking()) {
                Preconditions.checkArgument(storeFeatures.isKeyConsistent(),"Store needs to support some form of locking");
                KeyColumnValueStore lockerStore = manager[i].openDatabase(DB_NAME + "_lock_");
                ConsistentKeyLocker c = new ConsistentKeyLocker.Builder(lockerStore, manager[i]).fromConfig(sc).mediatorName(concreteClassName + i).build();
                store[i] = new ExpectedValueCheckingStore(store[i], c);
                for (int j = 0; j < NUM_TX; j++)
                    tx[i][j] = new ExpectedValueCheckingTransaction(tx[i][j], manager[i].beginTransaction(getConsistentTxConfig(manager[i])), GraphDatabaseConfiguration.STORAGE_READ_WAITTIME.getDefaultValue());
            }
        }
    }

    public StoreTransaction newTransaction(KeyColumnValueStoreManager manager) throws BackendException {
        StoreTransaction transaction = manager.beginTransaction(getTxConfig());
        if (!manager.getFeatures().hasLocking() && manager.getFeatures().isKeyConsistent()) {
            transaction = new ExpectedValueCheckingTransaction(transaction, manager.beginTransaction(getConsistentTxConfig(manager)), GraphDatabaseConfiguration.STORAGE_READ_WAITTIME.getDefaultValue());
        }
        return transaction;
    }

    @AfterEach
    public void tearDown() throws Exception {
        close();
    }

    public void close() throws BackendException {
        for (int i = 0; i < CONCURRENCY; i++) {
            store[i].close();

            for (int j = 0; j < NUM_TX; j++) {
                log.debug("Committing tx[{}][{}] = {}", i, j, tx[i][j]);
                if (tx[i][j] != null) tx[i][j].commit();
            }

            manager[i].close();
        }
        LocalLockMediators.INSTANCE.clear();
    }

    @Test
    public void singleLockAndUnlock() throws BackendException {
        store[0].acquireLock(k, c1, null, tx[0][0]);
        store[0].mutate(k, Collections.singletonList(StaticArrayEntry.of(c1, v1)), NO_DELETIONS, tx[0][0]);
        tx[0][0].commit();

        tx[0][0] = newTransaction(manager[0]);
        assertEquals(v1, KCVSUtil.get(store[0], k, c1, tx[0][0]));
    }

    @Test
    public void transactionMayReenterLock() throws BackendException {
        store[0].acquireLock(k, c1, null, tx[0][0]);
        store[0].acquireLock(k, c1, null, tx[0][0]);
        store[0].acquireLock(k, c1, null, tx[0][0]);
        store[0].mutate(k, Collections.singletonList(StaticArrayEntry.of(c1, v1)), NO_DELETIONS, tx[0][0]);
        tx[0][0].commit();

        tx[0][0] = newTransaction(manager[0]);
        assertEquals(v1, KCVSUtil.get(store[0], k, c1, tx[0][0]));
    }

    @Test
    public void expectedValueMismatchCausesMutateFailure() throws BackendException {
        assertThrows(PermanentLockingException.class, () -> {
            store[0].acquireLock(k, c1, v1, tx[0][0]);
            store[0].mutate(k, Collections.singletonList(StaticArrayEntry.of(c1, v1)), NO_DELETIONS, tx[0][0]);
        });
    }

    @Test
    public void testLocalLockContention() throws BackendException {
        store[0].acquireLock(k, c1, null, tx[0][0]);

        try {
            store[0].acquireLock(k, c1, null, tx[0][1]);
            fail("Lock contention exception not thrown");
        } catch (BackendException e) {
            assertTrue(e instanceof PermanentLockingException || e instanceof TemporaryLockingException);
        }

        try {
            store[0].acquireLock(k, c1, null, tx[0][1]);
            fail("Lock contention exception not thrown (2nd try)");
        } catch (BackendException e) {
            assertTrue(e instanceof PermanentLockingException || e instanceof TemporaryLockingException);
        }
    }

    @Test
    public void testRemoteLockContention() throws InterruptedException, BackendException {
        // acquire lock on "host1"
        store[0].acquireLock(k, c1, null, tx[0][0]);

        Thread.sleep(50L);

        try {
            // acquire same lock on "host2"
            store[1].acquireLock(k, c1, null, tx[1][0]);
        } catch (BackendException e) {            /* Lock attempts between hosts with different LocalLockMediators,
             * such as tx[0][0] and tx[1][0] in this example, should
			 * not generate locking failures until one of them tries
			 * to issue a mutate or mutateMany call.  An exception
			 * thrown during the acquireLock call above suggests that
			 * the LocalLockMediators for these two transactions are
			 * not really distinct, which would be a severe and fundamental
			 * bug in this test.
			 */
            fail("Contention between remote transactions detected too soon");
        }

        Thread.sleep(50L);

        try {
            // This must fail since "host1" took the lock first
            store[1].mutate(k, Collections.singletonList(StaticArrayEntry.of(c1, v2)), NO_DELETIONS, tx[1][0]);
            fail("Expected lock contention between remote transactions did not occur");
        } catch (BackendException e) {
            assertTrue(e instanceof PermanentLockingException || e instanceof TemporaryLockingException);
        }

        // This should succeed
        store[0].mutate(k, Collections.singletonList(StaticArrayEntry.of(c1, v1)), NO_DELETIONS, tx[0][0]);

        tx[0][0].commit();
        tx[0][0] = newTransaction(manager[0]);
        assertEquals(v1, KCVSUtil.get(store[0], k, c1, tx[0][0]));
    }

    @Test
    public void singleTransactionWithMultipleLocks() throws BackendException {
        tryWrites(store[0], manager[0], tx[0][0], store[0], tx[0][0]);
        /*
         * tryWrites commits transactions. set committed tx references to null
         * to prevent a second commit attempt in close().
         */
        tx[0][0] = null;
    }

    @Test
    public void twoLocalTransactionsWithIndependentLocks() throws BackendException {
        tryWrites(store[0], manager[0], tx[0][0], store[0], tx[0][1]);
        /*
         * tryWrites commits transactions. set committed tx references to null
         * to prevent a second commit attempt in close().
         */
        tx[0][0] = null;
        tx[0][1] = null;
    }

    @Test
    public void twoTransactionsWithIndependentLocks() throws BackendException {
        tryWrites(store[0], manager[0], tx[0][0], store[1], tx[1][0]);
        /*
         * tryWrites commits transactions. set committed tx references to null
         * to prevent a second commit attempt in close().
         */
        tx[0][0] = null;
        tx[1][0] = null;
    }

    @Test
    public void expiredLocalLockIsIgnored() throws BackendException, InterruptedException {
        tryLocks(store[0], tx[0][0], store[0], tx[0][1], true);
    }

    @Test
    public void expiredRemoteLockIsIgnored() throws BackendException, InterruptedException {
        tryLocks(store[0], tx[0][0], store[1], tx[1][0], false);
    }

    @Test
    public void repeatLockingDoesNotExtendExpiration() throws BackendException, InterruptedException {        /*
		 * This test is intrinsically racy and unreliable. There's no guarantee
		 * that the thread scheduler will put our test thread back on a core in
		 * a timely fashion after our Thread.sleep() argument elapses.
		 * Theoretically, Thread.sleep could also receive spurious wakeups that
		 * alter the timing of the test.
		 */
        long start = System.currentTimeMillis();
        long gracePeriodMS = 50L;
        long loopDurationMS = (EXPIRE_MS - gracePeriodMS);
        long targetMS = start + loopDurationMS;
        int steps = 20;

        // Initial lock acquisition by tx[0][0]
        store[0].acquireLock(k, k, null, tx[0][0]);

        // Repeat lock acquisition until just before expiration
        for (int i = 0; i <= steps; i++) {
            if (targetMS <= System.currentTimeMillis()) {
                break;
            }
            store[0].acquireLock(k, k, null, tx[0][0]);
            Thread.sleep(loopDurationMS / steps);
        }

        // tx[0][0]'s lock is about to expire (or already has)
        Thread.sleep(gracePeriodMS * 2);
        // tx[0][0]'s lock has expired (barring spurious wakeup)

        try {
            // Lock (k,k) with tx[0][1] now that tx[0][0]'s lock has expired
            store[0].acquireLock(k, k, null, tx[0][1]);
            // If acquireLock returns without throwing an Exception, we're OK
        } catch (BackendException e) {
            log.debug("Relocking exception follows", e);
            fail("Relocking following expiration failed");
        }
    }

    @Test
    public void parallelNoncontendedLockStressTest() throws InterruptedException {
        final Executor stressPool = Executors.newFixedThreadPool(CONCURRENCY);
        final CountDownLatch stressComplete = new CountDownLatch(CONCURRENCY);
        final long maxWallTimeAllowedMilliseconds = 90 * 1000L;
        final int lockOperationsPerThread = 100;
        final LockStressor[] ls = new LockStressor[CONCURRENCY];

        for (int i = 0; i < CONCURRENCY; i++) {
            ls[i] = new LockStressor(manager[i], store[i], stressComplete,
                    lockOperationsPerThread, KeyColumnValueStoreUtil.longToByteBuffer(i));
            stressPool.execute(ls[i]);
        }

        assertTrue(stressComplete.await(maxWallTimeAllowedMilliseconds, TimeUnit.MILLISECONDS),
            "Timeout exceeded");
        // All runnables submitted to the executor are done

        for (int i = 0; i < CONCURRENCY; i++) {
            if (0 < ls[i].temporaryFailures) {
                log.warn("Recorded {} temporary failures for thread index {}", ls[i].temporaryFailures, i);
            }
            assertEquals(lockOperationsPerThread, ls[i].succeeded + ls[i].temporaryFailures);
        }
    }

    @Test
    public void testLocksOnMultipleStores() throws Exception {

        //the number of stores must be a multiple of 3
        final int numStores = 6;
        final StaticBuffer key  = BufferUtil.getLongBuffer(1);
        final StaticBuffer col  = BufferUtil.getLongBuffer(2);
        final StaticBuffer val2 = BufferUtil.getLongBuffer(8);

        // Create mocks
        LockerProvider mockLockerProvider = createStrictMock(LockerProvider.class);
        Locker mockLocker = createStrictMock(Locker.class);

        // Create EVCSManager with mockLockerProvider
        ExpectedValueCheckingStoreManager expManager =
                new ExpectedValueCheckingStoreManager(manager[0], "multi_store_lock_mgr",
                        mockLockerProvider, Duration.ofMillis(100L));

        // Begin EVCTransaction
        BaseTransactionConfig txCfg = StandardBaseTransactionConfig.of(times);
        ExpectedValueCheckingTransaction tx = expManager.beginTransaction(txCfg);

        // openDatabase calls getLocker, and we do it numStores times
        expect(mockLockerProvider.getLocker(anyObject(String.class))).andReturn(mockLocker).times(numStores);

        // acquireLock calls writeLock, and we do it 2/3 * numStores times
        mockLocker.writeLock(eq(new KeyColumn(key, col)), eq(tx.getConsistentTx()));
        expectLastCall().times(numStores / 3 * 2);

        // mutateMany calls checkLocks, and we do it 2/3 * numStores times
        mockLocker.checkLocks(tx.getConsistentTx());
        expectLastCall().times(numStores / 3 * 2);

        replay(mockLockerProvider);
        replay(mockLocker);

        /*
         * Acquire a lock on several distinct stores (numStores total distinct
         * stores) and build mutations.
         */
        ImmutableMap.Builder<String, Map<StaticBuffer, KCVMutation>> builder = ImmutableMap.builder();
        for (int i = 0; i < numStores; i++) {
            String storeName = "multi_store_lock_" + i;
            KeyColumnValueStore s = expManager.openDatabase(storeName);

            if (i % 3 < 2)
                s.acquireLock(key, col, null, tx);

            if (i % 3 > 0) {
                builder.put(storeName, ImmutableMap.of(key,
                    new KCVMutation(ImmutableList.of(StaticArrayEntry.of(col, val2)), ImmutableList.of())));
            }
        }

        // Mutate
        expManager.mutateMany(builder.build(), tx);

        // Shutdown
        expManager.close();

        // Check the mocks
        verify(mockLockerProvider);
        verify(mockLocker);
    }

    private void tryWrites(KeyColumnValueStore store1, KeyColumnValueStoreManager keyColumnValueStoreManager,
                           StoreTransaction tx1, KeyColumnValueStore store2,
                           StoreTransaction tx2) throws BackendException {
        assertNull(KCVSUtil.get(store1, k, c1, tx1));
        assertNull(KCVSUtil.get(store2, k, c2, tx2));

        store1.acquireLock(k, c1, null, tx1);
        store2.acquireLock(k, c2, null, tx2);

        store1.mutate(k, Collections.singletonList(StaticArrayEntry.of(c1, v1)), NO_DELETIONS, tx1);
        store2.mutate(k, Collections.singletonList(StaticArrayEntry.of(c2, v2)), NO_DELETIONS, tx2);

        tx1.commit();
        if (tx2 != tx1)
            tx2.commit();

        StoreTransaction transaction = newTransaction(keyColumnValueStoreManager);
        assertEquals(v1, KCVSUtil.get(store1, k, c1, transaction));
        assertEquals(v2, KCVSUtil.get(store2, k, c2, transaction));
        transaction.commit();
    }

    private void tryLocks(KeyColumnValueStore s1,
                          StoreTransaction tx1, KeyColumnValueStore s2,
                          StoreTransaction tx2, boolean detectLocally) throws BackendException, InterruptedException {

        s1.acquireLock(k, k, null, tx1);

        // Require local lock contention, if requested by our caller
        // Remote lock contention is checked by separate cases
        if (detectLocally) {
            try {
                s2.acquireLock(k, k, null, tx2);
                fail("Expected lock contention between transactions did not occur");
            } catch (BackendException e) {
                assertTrue(e instanceof PermanentLockingException || e instanceof TemporaryLockingException);
            }
        }

        // Let the original lock expire
        Thread.sleep(EXPIRE_MS + 100L);

        // This should succeed now that the original lock is expired
        s2.acquireLock(k, k, null, tx2);

        // Mutate to check for remote contention
        s2.mutate(k, Collections.singletonList(StaticArrayEntry.of(c2, v2)), NO_DELETIONS, tx2);

    }

    /**
     * Run lots of acquireLock() and commit() ops on a provided store and txn.
     * <p/>
     * Used by {@link #parallelNoncontendedLockStressTest()}.
     *
     * @author "Dan LaRocque <dalaro@hopcount.org>"
     */
    private class LockStressor implements Runnable {

        private final KeyColumnValueStoreManager manager;
        private final KeyColumnValueStore store;
        private final CountDownLatch doneLatch;
        private final int opCount;
        private final StaticBuffer toLock;

        private int succeeded = 0;
        private int temporaryFailures = 0;

        private LockStressor(KeyColumnValueStoreManager manager,
                             KeyColumnValueStore store, CountDownLatch doneLatch, int opCount, StaticBuffer toLock) {
            this.manager = manager;
            this.store = store;
            this.doneLatch = doneLatch;
            this.opCount = opCount;
            this.toLock = toLock;
        }

        @Override
        public void run() {

            // Catch & log exceptions
            for (int opIndex = 0; opIndex < opCount; opIndex++) {

                StoreTransaction tx = null;
                try {
                    tx = newTransaction(manager);
                    store.acquireLock(toLock, toLock, null, tx);
                    store.mutate(toLock, ImmutableList.of(), Collections.singletonList(toLock), tx);
                    tx.commit();
                    succeeded++;
                } catch (TemporaryLockingException e) {
                    temporaryFailures++;
                } catch (Throwable t) {
                    log.error("Unexpected locking-related exception on iteration " + (opIndex + 1) + "/" + opCount, t);
                }
            }

            /*
             * This latch is the only thing guaranteeing that succeeded's true
             * value is observable by other threads once we're done with run()
             * and the latch's await() method returns.
             */
            doneLatch.countDown();
        }
    }


}
