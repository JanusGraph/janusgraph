package com.thinkaurelius.titan.diskstorage;

import static com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore.NO_DELETIONS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.diskstorage.idmanagement.ConsistentKeyIDManager;
import com.thinkaurelius.titan.diskstorage.idmanagement.TransactionalIDManager;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediators;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLocker;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingStore;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingTransaction;
import com.thinkaurelius.titan.diskstorage.locking.transactional.TransactionalLockStore;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;

public abstract class LockKeyColumnValueStoreTest {

    private static final Logger log =
            LoggerFactory.getLogger(LockKeyColumnValueStoreTest.class);

    public static final int CONCURRENCY = 8;
    public static final int NUM_TX = 2;
    public static final String DB_NAME = "test";
    protected static final long EXPIRE_MS = 5000;

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
    public IDAuthority[] idAuthorities;

    private StaticBuffer k, c1, c2, v1, v2;

    private final String concreteClassName;

    public LockKeyColumnValueStoreTest() {
        concreteClassName = getClass().getSimpleName();
    }

    @Before
    public void setUp() throws Exception {
        openStorageManager(0).clearStorage();

        for (int i = 0; i < CONCURRENCY; i++) {
            LocalLockMediators.INSTANCE.clear(concreteClassName + i);
        }

        open();
        k = KeyValueStoreUtil.getBuffer("key");
        c1 = KeyValueStoreUtil.getBuffer("col1");
        c2 = KeyValueStoreUtil.getBuffer("col2");
        v1 = KeyValueStoreUtil.getBuffer("val1");
        v2 = KeyValueStoreUtil.getBuffer("val2");
    }

    public abstract KeyColumnValueStoreManager openStorageManager(int id) throws StorageException;

    public void open() throws StorageException {
        manager = new KeyColumnValueStoreManager[CONCURRENCY];
        tx = new StoreTransaction[CONCURRENCY][NUM_TX];
        store = new KeyColumnValueStore[CONCURRENCY];
        idAuthorities = new IDAuthority[CONCURRENCY];

        for (int i = 0; i < CONCURRENCY; i++) {
            manager[i] = openStorageManager(i);
            StoreFeatures storeFeatures = manager[i].getFeatures();
            store[i] = manager[i].openDatabase(DB_NAME);
            for (int j = 0; j < NUM_TX; j++) {
                tx[i][j] = manager[i].beginTransaction(new StoreTxConfig());
                log.debug("Began transaction of class {}", tx[i][j].getClass().getCanonicalName());
            }

            Configuration sc = new BaseConfiguration();
            sc.addProperty(ExpectedValueCheckingStore.LOCAL_LOCK_MEDIATOR_PREFIX_KEY, concreteClassName + i);
            sc.addProperty(GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY, (short) i);
            sc.addProperty(GraphDatabaseConfiguration.LOCK_RETRY_COUNT, 10);
            sc.addProperty(GraphDatabaseConfiguration.LOCK_EXPIRE_MS, EXPIRE_MS);
            sc.addProperty(GraphDatabaseConfiguration.IDAUTHORITY_RETRY_COUNT_KEY, 50);
            sc.addProperty(GraphDatabaseConfiguration.IDAUTHORITY_WAIT_MS_KEY, 100);

            if (!storeFeatures.supportsLocking()) {
                if (storeFeatures.supportsTransactions()) {
                    store[i] = new TransactionalLockStore(store[i]);
                } else if (storeFeatures.supportsConsistentKeyOperations()) {

                    KeyColumnValueStore lockerStore = manager[i].openDatabase(DB_NAME + "_lock_");
                    ConsistentKeyLocker c = new ConsistentKeyLocker.Builder(lockerStore).fromCommonsConfig(sc).mediatorName(concreteClassName + i).build();
                    store[i] = new ExpectedValueCheckingStore(store[i], c);
                    for (int j = 0; j < NUM_TX; j++)
                        tx[i][j] = new ExpectedValueCheckingTransaction(tx[i][j], manager[i].beginTransaction(new StoreTxConfig(ConsistencyLevel.KEY_CONSISTENT)), GraphDatabaseConfiguration.READ_ATTEMPTS_DEFAULT);
                } else throw new IllegalArgumentException("Store needs to support some form of locking");
            }

            KeyColumnValueStore idStore = manager[i].openDatabase("ids");
            if (storeFeatures.supportsTransactions())
                idAuthorities[i] = new TransactionalIDManager(idStore, manager[i], sc);
            else if (storeFeatures.supportsConsistentKeyOperations())
                idAuthorities[i] = new ConsistentKeyIDManager(idStore, manager[i], sc);
            else throw new IllegalArgumentException("Cannot open id store");
        }
    }

    public StoreTransaction newTransaction(KeyColumnValueStoreManager manager) throws StorageException {
        StoreTransaction transaction = manager.beginTransaction(new StoreTxConfig());
        if (!manager.getFeatures().supportsLocking() && manager.getFeatures().supportsConsistentKeyOperations()) {
            transaction = new ExpectedValueCheckingTransaction(transaction, manager.beginTransaction(new StoreTxConfig(ConsistencyLevel.KEY_CONSISTENT)), GraphDatabaseConfiguration.READ_ATTEMPTS_DEFAULT);
        }
        return transaction;
    }

    @After
    public void tearDown() throws Exception {
        close();
    }

    public void close() throws StorageException {
        for (int i = 0; i < CONCURRENCY; i++) {
            store[i].close();
            idAuthorities[i].close();

            for (int j = 0; j < NUM_TX; j++) {
                log.debug("Committing tx[{}][{}] = {}", new Object[]{i, j, tx[i][j]});
                if (tx[i][j] != null) tx[i][j].commit();
            }

            manager[i].close();
        }
        LocalLockMediators.INSTANCE.clear();
    }

    @Test
    public void singleLockAndUnlock() throws StorageException {
        store[0].acquireLock(k, c1, null, tx[0][0]);
        store[0].mutate(k, Arrays.<Entry>asList(new StaticBufferEntry(c1, v1)), NO_DELETIONS, tx[0][0]);
        tx[0][0].commit();

        tx[0][0] = newTransaction(manager[0]);
        Assert.assertEquals(v1, KCVSUtil.get(store[0], k, c1, tx[0][0]));
    }

    @Test
    public void transactionMayReenterLock() throws StorageException {
        store[0].acquireLock(k, c1, null, tx[0][0]);
        store[0].acquireLock(k, c1, null, tx[0][0]);
        store[0].acquireLock(k, c1, null, tx[0][0]);
        store[0].mutate(k, Arrays.<Entry>asList(new StaticBufferEntry(c1, v1)), NO_DELETIONS, tx[0][0]);
        tx[0][0].commit();

        tx[0][0] = newTransaction(manager[0]);
        Assert.assertEquals(v1, KCVSUtil.get(store[0], k, c1, tx[0][0]));
    }

    @Test(expected = PermanentLockingException.class)
    public void expectedValueMismatchCausesMutateFailure() throws StorageException {
        store[0].acquireLock(k, c1, v1, tx[0][0]);
        store[0].mutate(k, Arrays.<Entry>asList(new StaticBufferEntry(c1, v1)), NO_DELETIONS, tx[0][0]);
    }

    @Test
    public void testLocalLockContention() throws StorageException {
        store[0].acquireLock(k, c1, null, tx[0][0]);

        try {
            store[0].acquireLock(k, c1, null, tx[0][1]);
            Assert.fail("Lock contention exception not thrown");
        } catch (StorageException e) {
            Assert.assertTrue(e instanceof PermanentLockingException || e instanceof TemporaryLockingException);
        }

        try {
            store[0].acquireLock(k, c1, null, tx[0][1]);
            Assert.fail("Lock contention exception not thrown (2nd try)");
        } catch (StorageException e) {
            Assert.assertTrue(e instanceof PermanentLockingException || e instanceof TemporaryLockingException);
        }
    }

    @Test
    public void testRemoteLockContention() throws InterruptedException, StorageException {
        // acquire lock on "host1"
        store[0].acquireLock(k, c1, null, tx[0][0]);

        Thread.sleep(50L);

        try {
            // acquire same lock on "host2"
            store[1].acquireLock(k, c1, null, tx[1][0]);
        } catch (StorageException e) {            /* Lock attempts between hosts with different LocalLockMediators,
             * such as tx[0][0] and tx[1][0] in this example, should
			 * not generate locking failures until one of them tries
			 * to issue a mutate or mutateMany call.  An exception
			 * thrown during the acquireLock call above suggests that
			 * the LocalLockMediators for these two transactions are
			 * not really distinct, which would be a severe and fundamental
			 * bug in this test.
			 */
            Assert.fail("Contention between remote transactions detected too soon");
        }

        Thread.sleep(50L);

        try {
            // This must fail since "host1" took the lock first
            store[1].mutate(k, Arrays.<Entry>asList(new StaticBufferEntry(c1, v2)), NO_DELETIONS, tx[1][0]);
            Assert.fail("Expected lock contention between remote transactions did not occur");
        } catch (StorageException e) {
            Assert.assertTrue(e instanceof PermanentLockingException || e instanceof TemporaryLockingException);
        }

        // This should succeed
        store[0].mutate(k, Arrays.<Entry>asList(new StaticBufferEntry(c1, v1)), NO_DELETIONS, tx[0][0]);

        tx[0][0].commit();
        tx[0][0] = newTransaction(manager[0]);
        Assert.assertEquals(v1, KCVSUtil.get(store[0], k, c1, tx[0][0]));
    }

    @Test
    public void singleTransactionWithMultipleLocks() throws StorageException {
        tryWrites(store[0], manager[0], tx[0][0], store[0], tx[0][0]);
        /*
         * tryWrites commits transactions. set committed tx references to null
         * to prevent a second commit attempt in close().
         */
        tx[0][0] = null;
    }

    @Test
    public void twoLocalTransactionsWithIndependentLocks() throws StorageException {
        tryWrites(store[0], manager[0], tx[0][0], store[0], tx[0][1]);
        /*
         * tryWrites commits transactions. set committed tx references to null
         * to prevent a second commit attempt in close().
         */
        tx[0][0] = null;
        tx[0][1] = null;
    }

    @Test
    public void twoTransactionsWithIndependentLocks() throws StorageException {
        tryWrites(store[0], manager[0], tx[0][0], store[1], tx[1][0]);
        /*
         * tryWrites commits transactions. set committed tx references to null
         * to prevent a second commit attempt in close().
         */
        tx[0][0] = null;
        tx[1][0] = null;
    }

    @Test
    public void expiredLocalLockIsIgnored() throws StorageException, InterruptedException {
        tryLocks(store[0], tx[0][0], store[0], tx[0][1], true);
    }

    @Test
    public void expiredRemoteLockIsIgnored() throws StorageException, InterruptedException {
        tryLocks(store[0], tx[0][0], store[1], tx[1][0], false);
    }

    @Test
    public void repeatLockingDoesNotExtendExpiration() throws StorageException, InterruptedException {        /*
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

        // Repeat lock acquistion until just before expiration
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
        } catch (StorageException e) {
            log.debug("Relocking exception follows", e);
            Assert.fail("Relocking following expiration failed");
        }
    }

    @Test
    public void parallelNoncontendedLockStressTest() throws StorageException, InterruptedException {
        final Executor stressPool = Executors.newFixedThreadPool(CONCURRENCY);
        final CountDownLatch stressComplete = new CountDownLatch(CONCURRENCY);
        final long maxWalltimeAllowedMS = 90 * 1000L;
        final int lockOperationsPerThread = 100;
        final LockStressor[] ls = new LockStressor[CONCURRENCY];

        for (int i = 0; i < CONCURRENCY; i++) {
            ls[i] = new LockStressor(manager[i], store[i], stressComplete,
                    lockOperationsPerThread, KeyColumnValueStoreUtil.longToByteBuffer(i));
            stressPool.execute(ls[i]);
        }

        Assert.assertTrue("Timeout exceeded",
                stressComplete.await(maxWalltimeAllowedMS, TimeUnit.MILLISECONDS));
        // All runnables submitted to the executor are done

        for (int i = 0; i < CONCURRENCY; i++) {
            if (0 < ls[i].temporaryFailures) {
                log.warn("Recorded {} temporary failures for thread index {}", ls[i].temporaryFailures, i);
            }
            Assert.assertEquals(lockOperationsPerThread, ls[i].succeeded + ls[i].temporaryFailures);
        }
    }

    private void tryWrites(KeyColumnValueStore store1, KeyColumnValueStoreManager checkmgr,
                           StoreTransaction tx1, KeyColumnValueStore store2,
                           StoreTransaction tx2) throws StorageException {
        Assert.assertNull(KCVSUtil.get(store1, k, c1, tx1));
        Assert.assertNull(KCVSUtil.get(store2, k, c2, tx2));

        store1.acquireLock(k, c1, null, tx1);
        store2.acquireLock(k, c2, null, tx2);

        store1.mutate(k, Arrays.<Entry>asList(new StaticBufferEntry(c1, v1)), NO_DELETIONS, tx1);
        store2.mutate(k, Arrays.<Entry>asList(new StaticBufferEntry(c2, v2)), NO_DELETIONS, tx2);

        tx1.commit();
        if (tx2 != tx1)
            tx2.commit();

        StoreTransaction checktx = newTransaction(checkmgr);
        Assert.assertEquals(v1, KCVSUtil.get(store1, k, c1, checktx));
        Assert.assertEquals(v2, KCVSUtil.get(store2, k, c2, checktx));
        checktx.commit();
    }

    private void tryLocks(KeyColumnValueStore s1,
                          StoreTransaction tx1, KeyColumnValueStore s2,
                          StoreTransaction tx2, boolean detectLocally) throws StorageException, InterruptedException {

        s1.acquireLock(k, k, null, tx1);

        // Require local lock contention, if requested by our caller
        // Remote lock contention is checked by separate cases
        if (detectLocally) {
            try {
                s2.acquireLock(k, k, null, tx2);
                Assert.fail("Expected lock contention between transactions did not occur");
            } catch (StorageException e) {
                Assert.assertTrue(e instanceof PermanentLockingException || e instanceof TemporaryLockingException);
            }
        }

        // Let the original lock expire
        Thread.sleep(EXPIRE_MS + 100L);

        // This should succeed now that the original lock is expired
        s2.acquireLock(k, k, null, tx2);

        // Mutate to check for remote contention
        s2.mutate(k, Arrays.<Entry>asList(new StaticBufferEntry(c2, v2)), NO_DELETIONS, tx2);

    }

    @Test
    public void testSimpleIDAcquisition() throws StorageException {
        final int blockSize = 400;
        final IDBlockSizer blockSizer = new IDBlockSizer() {
            @Override
            public long getBlockSize(int partitionID) {
                return blockSize;
            }
        };
        idAuthorities[0].setIDBlockSizer(blockSizer);
        long[] block = idAuthorities[0].getIDBlock(0);
        Assert.assertEquals(1, block[0]);
        Assert.assertEquals(block[1], block[0] + blockSize);
        block = idAuthorities[0].getIDBlock(0);
        Assert.assertEquals(1 + blockSize, block[0]);
        Assert.assertEquals(block[1], block[0] + blockSize);
    }

    @Test
    public void testMultiIDAcquisition() throws Throwable {
        final int numPartitions = 4;
        final int numAcquisitionsPerThreadPartition = 300;
        final int blockSize = 250;
        final IDBlockSizer blockSizer = new IDBlockSizer() {
            @Override
            public long getBlockSize(int partitionID) {
                return blockSize;
            }
        };
        for (int i = 0; i < CONCURRENCY; i++) idAuthorities[i].setIDBlockSizer(blockSizer);
        final List<List<Long>> ids = new ArrayList<List<Long>>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            ids.add(Collections.synchronizedList(new ArrayList<Long>(numAcquisitionsPerThreadPartition * CONCURRENCY)));
        }

        final int maxIterations = numAcquisitionsPerThreadPartition * numPartitions * 2;
        final Collection<Future<?>> futures = new ArrayList<Future<?>>(CONCURRENCY);
        ExecutorService es = Executors.newFixedThreadPool(CONCURRENCY);

        for (int i = 0; i < CONCURRENCY; i++) {
            final IDAuthority idAuthority = idAuthorities[i];
            final IDStressor stressRunnable = new IDStressor(
                    numAcquisitionsPerThreadPartition, numPartitions,
                    maxIterations, blockSize, idAuthority, ids);
            futures.add(es.submit(stressRunnable));
        }

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        }

        for (int i = 0; i < numPartitions; i++) {
            List<Long> list = ids.get(i);
            Assert.assertEquals(numAcquisitionsPerThreadPartition * CONCURRENCY, list.size());
            Collections.sort(list);

            int pos = 0;
            long id = 1;
            while (pos < list.size()) {
                long block = list.get(pos).longValue();
                /*
                 * If the ID allocator never timed out while servicing a
                 * request, then id = block on every iteration. However, if the
                 * ID allocator timed out while trying to claim some blocks,
                 * then block = id + (blockSize * n) where n >= 1. Intuitively,
                 * this allows "dead" blocks lost to timeout exceptions to be
                 * skipped without failing the test.
                 */
                Assert.assertTrue(0 < block);
                Assert.assertTrue(id <= block);
                Assert.assertTrue(0 == (block - id) % blockSize);
                final long skipped = (block - id) / blockSize;
                Assert.assertTrue(0 <= skipped);
                id = block + blockSize;
                pos++;
            }
        }
    }


    @Test
    public void testLocalPartitionAcquisition() throws StorageException {
        for (int c = 0; c < CONCURRENCY; c++) {
            if (manager[c].getFeatures().hasLocalKeyPartition()) {
                try {
                    StaticBuffer[] partition = idAuthorities[c].getLocalIDPartition();
                    Assert.assertEquals(partition[0].length(), partition[1].length());
                    for (int i = 0; i < 2; i++) {
                        Assert.assertTrue(partition[i].length() >= 4);
                    }
                } catch (UnsupportedOperationException e) {
                    Assert.fail();
                }
            }
        }
    }

    /**
     * Run lots of acquireLock() and commit() ops on a provided store and txn.
     * <p/>
     * Used by {@link LockKeyColumnValueStoreTest#parallelLockStressTest()}.
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
                    store.mutate(toLock, ImmutableList.<Entry>of(), Arrays.asList(toLock), tx);
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

    private class IDStressor implements Runnable {

        private final int numRounds;
        private final int numPartitions;
        private final int maxIterations;
        private final int blockSize;
        private final IDAuthority authority;
        private final List<List<Long>> allocatedBlocks;

        private static final long sleepMS = 250L;

        private IDStressor(int numRounds, int numPartitions, int maxIterations,
                           int blockSize, IDAuthority authority, List<List<Long>> ids) {
            this.numRounds = numRounds;
            this.numPartitions = numPartitions;
            this.maxIterations = maxIterations;
            this.blockSize = blockSize;
            this.authority = authority;
            this.allocatedBlocks = ids;
        }

        @Override
        public void run() {
            try {
                runInterruptible();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private void runInterruptible() throws InterruptedException {
            int iterations = 0;
            long lastStart[] = new long[numPartitions];
            for (int i = 0; i < numPartitions; i++)
                lastStart[i] = Long.MIN_VALUE;
            for (int j = 0; j < numRounds; j++) {
                for (int p = 0; p < numPartitions; p++) {
                    if (maxIterations < ++iterations) {
                        throwIterationsExceededException();
                    }

                    Long start = allocate(p);

                    if (null == start) {
                        Thread.sleep(sleepMS);
                        p--;
                    } else {
                        Assert.assertTrue("Previous block start "
                                + lastStart[p] + " exceeds next block start "
                                + start, lastStart[p] <= start);
                        Assert.assertFalse(allocatedBlocks.get(p).contains(start));
                        allocatedBlocks.get(p).add(start);
                        lastStart[p] = start;
                    }
                }
            }
        }

        private Long allocate(int partitionIndex) {

            final long[] block;
            try {
                block = authority.getIDBlock(partitionIndex);
            } catch (StorageException e) {
                log.error("Unexpected exception while getting ID block", e);
                return null;
            }

            /*
             * This is not guaranteed in the consistentkey implementation.
             * Writers of ID block claims in that implementation delete their
             * writes if they take too long. A peek can see this short-lived
             * block claim even though a subsequent getblock does not.
             */
//            Assert.assertTrue(nextId <= block[0]);
            Assert.assertEquals(block[0] + blockSize, block[1]);
            log.trace("Obtained ID block {},{}", block[0], block[1]);

            return block[0];
        }

        private boolean throwIterationsExceededException() {
            throw new RuntimeException(
                    "Exceeded maximum ID allocation iteration count ("
                            + maxIterations + "); too many timeouts?");
        }
    }

}
