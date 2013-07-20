package com.thinkaurelius.titan.diskstorage;

import static com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore.NO_DELETIONS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.diskstorage.idmanagement.ConsistentKeyIDManager;
import com.thinkaurelius.titan.diskstorage.idmanagement.TransactionalIDManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVSUtil;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.LockingException;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockConfiguration;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockStore;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockTransaction;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.LocalLockMediators;
import com.thinkaurelius.titan.diskstorage.locking.transactional.TransactionalLockStore;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public abstract class LockKeyColumnValueStoreTest {
    private static final Logger logger = LoggerFactory.getLogger(LockKeyColumnValueStoreTest.class);

    private static final int CONCURRENCY = 8;
    private static final int NUM_TX = 2;
    protected static final long EXPIRE_MS = 1000;

    private static final KeyColumnValueStoreManager[] MANAGERS = new KeyColumnValueStoreManager[CONCURRENCY];
    private static final KeyColumnValueStore[] STORES = new KeyColumnValueStore[CONCURRENCY];
    private static final IDAuthority[] ID_AUTHORITIES = new IDAuthority[CONCURRENCY];

    private final StoreTransaction[][] transactions = new StoreTransaction[CONCURRENCY][NUM_TX];

    public static final String DB_NAME = "test";

    private StaticBuffer k, c1, c2, v1, v2;

    @Before
    public void setUp() throws Exception {
        for (int i = 0; i < CONCURRENCY; i++) {
            if (MANAGERS[i] == null)
                continue;

            MANAGERS[i].clearStorage();
            reopenIDAuthority(i);
        }

        k  = KeyValueStoreUtil.getBuffer("key");
        c1 = KeyValueStoreUtil.getBuffer("col1");
        c2 = KeyValueStoreUtil.getBuffer("col2");
        v1 = KeyValueStoreUtil.getBuffer("val1");
        v2 = KeyValueStoreUtil.getBuffer("val2");

        for (int i = 0; i < CONCURRENCY; i++) {
            KeyColumnValueStoreManager manager = getManager(i);

            for (int j = 0; j < NUM_TX; j++) {
                transactions[i][j] = manager.beginTransaction(ConsistencyLevel.DEFAULT);
                logger.debug("Began transaction of class {}", transactions[i][j].getClass().getCanonicalName());
            }

            if (manager.getFeatures().supportsConsistentKeyOperations()) {
                for (int j = 0; j  < NUM_TX; j++) {
                    transactions[i][j] = new ConsistentKeyLockTransaction(transactions[i][j],
                                                                          manager.beginTransaction(ConsistencyLevel.KEY_CONSISTENT));
                }
            }
        }
    }

    public abstract KeyColumnValueStoreManager openStorageManager(int id) throws StorageException;

    public StoreTransaction newTransaction(KeyColumnValueStoreManager manager) throws StorageException {
        StoreTransaction transaction = manager.beginTransaction(ConsistencyLevel.DEFAULT);
        if (!manager.getFeatures().supportsLocking() && manager.getFeatures().supportsConsistentKeyOperations()) {
            transaction = new ConsistentKeyLockTransaction(transaction, manager.beginTransaction(ConsistencyLevel.KEY_CONSISTENT));
        }
        return transaction;
    }

    @After
    public void tearDown() throws Exception {
        for (int i = 0; i < CONCURRENCY; i++) {
            for (int j = 0; j < NUM_TX; j++) {
                logger.debug("Committing transactions[{}][{}] = {}", new Object[]{ i, j, transactions[i][j] });
                if (transactions[i][j] != null)
                    transactions[i][j].commit();
            }
        }

        LocalLockMediators.INSTANCE.clear();
    }

    @Test
    public void singleLockAndUnlock() throws StorageException {
        getStore(0).acquireLock(k, c1, null, transactions[0][0]);
        getStore(0).mutate(k, Arrays.<Entry>asList(new StaticBufferEntry(c1, v1)), NO_DELETIONS, transactions[0][0]);
        transactions[0][0].commit();

        transactions[0][0] = newTransaction(getManager(0));
        Assert.assertEquals(v1, KCVSUtil.get(getStore(0), k, c1, transactions[0][0]));
    }

    @Test
    public void transactionMayReenterLock() throws StorageException {
        KeyColumnValueStore store = getStore(0);

        store.acquireLock(k, c1, null, transactions[0][0]);
        store.acquireLock(k, c1, null, transactions[0][0]);
        store.acquireLock(k, c1, null, transactions[0][0]);
        store.mutate(k, Arrays.<Entry>asList(new StaticBufferEntry(c1, v1)), NO_DELETIONS, transactions[0][0]);
        transactions[0][0].commit();

        transactions[0][0] = newTransaction(getManager(0));
        Assert.assertEquals(v1, KCVSUtil.get(store, k, c1, transactions[0][0]));
    }

    @Test(expected = PermanentLockingException.class)
    public void expectedValueMismatchCausesMutateFailure() throws StorageException {
        getStore(0).acquireLock(k, c1, v1, transactions[0][0]);
        getStore(0).mutate(k, Arrays.<Entry>asList(new StaticBufferEntry(c1, v1)), NO_DELETIONS, transactions[0][0]);
    }

    @Test
    public void testLocalLockContention() throws StorageException {
        KeyColumnValueStore store = getStore(0);
        store.acquireLock(k, c1, null, transactions[0][0]);

        try {
            store.acquireLock(k, c1, null, transactions[0][1]);
            Assert.fail("Lock contention exception not thrown");
        } catch (StorageException e) {
            Assert.assertTrue(e instanceof LockingException);
        }

        try {
            store.acquireLock(k, c1, null, transactions[0][1]);
            Assert.fail("Lock contention exception not thrown (2nd try)");
        } catch (StorageException e) {
            Assert.assertTrue(e instanceof LockingException);
        }
    }

    @Test
    public void testRemoteLockContention() throws InterruptedException, StorageException {
        KeyColumnValueStore store0 = getStore(0);
        KeyColumnValueStore store1 = getStore(1);

        // acquire lock on "host1"
        store0.acquireLock(k, c1, null, transactions[0][0]);

        Thread.sleep(50L);

        try {
            // acquire same lock on "host2"
            store1.acquireLock(k, c1, null, transactions[1][0]);
        } catch (StorageException e) {
            // Lock attempts between hosts with different LocalLockMediators,
            // such as tx[0][0] and tx[1][0] in this example, should
            // not generate locking failures until one of them tries
            // to issue a mutate or mutateMany call.  An exception
            // thrown during the acquireLock call above suggests that
            // the LocalLockMediators for these two transactions are
            // not really distinct, which would be a severe and fundamental
            // bug in this test.
            Assert.fail("Contention between remote transactions detected too soon");
        }

        Thread.sleep(50L);

        try {
            // This must fail since "host1" took the lock first
            store1.mutate(k, Arrays.<Entry>asList(new StaticBufferEntry(c1, v2)), NO_DELETIONS, transactions[1][0]);
            Assert.fail("Expected lock contention between remote transactions did not occur");
        } catch (StorageException e) {
            Assert.assertTrue(e instanceof LockingException);
        }

        // This should succeed
        store0.mutate(k, Arrays.<Entry>asList(new StaticBufferEntry(c1, v1)), NO_DELETIONS, transactions[0][0]);

        transactions[0][0].commit();
        transactions[0][0] = newTransaction(getManager(0));
        Assert.assertEquals(v1, KCVSUtil.get(store0, k, c1, transactions[0][0]));
    }

    @Test
    public void singleTransactionWithMultipleLocks() throws StorageException {
        tryWrites(getStore(0), getManager(0), transactions[0][0], getStore(0), transactions[0][0]);
        // tryWrites commits transactions. set committed tx references to null
        // to prevent a second commit attempt in close().
        transactions[0][0] = null;
    }

    @Test
    public void twoLocalTransactionsWithIndependentLocks() throws StorageException {
        tryWrites(getStore(0), getManager(0), transactions[0][0], getStore(0), transactions[0][1]);
        // tryWrites commits transactions. set committed tx references to null
        // to prevent a second commit attempt in close().
        transactions[0][0] = null;
        transactions[0][1] = null;
    }

    @Test
    public void twoTransactionsWithIndependentLocks() throws StorageException {
        tryWrites(getStore(0), getManager(0), transactions[0][0], getStore(1), transactions[1][0]);
        // tryWrites commits transactions. set committed tx references to null
        // to prevent a second commit attempt in close().
        transactions[0][0] = null;
        transactions[1][0] = null;
    }

    @Test
    public void expiredLocalLockIsIgnored() throws StorageException, InterruptedException {
        tryLocks(getStore(0), transactions[0][0], getStore(0), transactions[0][1], true);
    }

    @Test
    public void expiredRemoteLockIsIgnored() throws StorageException, InterruptedException {
        tryLocks(getStore(0), transactions[0][0], getStore(1), transactions[1][0], false);
    }

    @Test
    public void repeatLockingDoesNotExtendExpiration() throws StorageException, InterruptedException {
		// This test is intrinsically racy and unreliable. There's no guarantee
	    // that the thread scheduler will put our test thread back on a core in
		// a timely fashion after our Thread.sleep() argument elapses.
		// Theoretically, Thread.sleep could also receive spurious wakeups that
		// alter the timing of the test.
        long start = System.currentTimeMillis();
        long gracePeriodMS = 50L;
        long loopDurationMS = (EXPIRE_MS - gracePeriodMS);
        long targetMS = start + loopDurationMS;
        int steps = 20;

        KeyColumnValueStore store = getStore(0);

        // Initial lock acquisition by tx[0][0]
        store.acquireLock(k, k, null, transactions[0][0]);
        
        // Repeat lock acquistion until just before expiration
        for (int i = 0; i <= steps; i++) {
            if (targetMS <= System.currentTimeMillis()) {
                break;
            }
            store.acquireLock(k, k, null, transactions[0][0]);
            Thread.sleep(loopDurationMS / steps);
        }
        
        // tx[0][0]'s lock is about to expire (or already has)
        Thread.sleep(gracePeriodMS * 2);
        // tx[0][0]'s lock has expired (barring spurious wakeup)
        
        try {
        	// Lock (k,k) with tx[0][1] now that tx[0][0]'s lock has expired
            store.acquireLock(k, k, null, transactions[0][1]);
            // If acquireLock returns without throwing an Exception, we're OK
        } catch (StorageException e) {
            logger.debug("Relocking exception follows", e);
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
            ls[i] = new LockStressor(getManager(i),
                                     getStore(i),
                                     stressComplete,
                                     lockOperationsPerThread,
                                     KeyColumnValueStoreUtil.longToByteBuffer(i));
            stressPool.execute(ls[i]);
        }

        Assert.assertTrue("Timeout exceeded",
                stressComplete.await(maxWalltimeAllowedMS, TimeUnit.MILLISECONDS));
        // All runnables submitted to the executor are done
        
        for (int i = 0; i < CONCURRENCY; i++) {
            Assert.assertEquals(lockOperationsPerThread, ls[i].succeeded);
        }
    }

    private void tryWrites(KeyColumnValueStore store1, KeyColumnValueStoreManager checkmgr,
                           StoreTransaction tx1, KeyColumnValueStore store2,
                           StoreTransaction tx2) throws StorageException {
        Assert.assertNull(KCVSUtil.get(store1,k, c1, tx1));
        Assert.assertNull(KCVSUtil.get(store2,k, c2, tx2));

        store1.acquireLock(k, c1, null, tx1);
        store2.acquireLock(k, c2, null, tx2);

        store1.mutate(k, Arrays.<Entry>asList(new StaticBufferEntry(c1, v1)), NO_DELETIONS, tx1);
        store2.mutate(k, Arrays.<Entry>asList(new StaticBufferEntry(c2, v2)), NO_DELETIONS, tx2);

        tx1.commit();
        if (tx2 != tx1)
            tx2.commit();

        StoreTransaction checktx = newTransaction(checkmgr);
        Assert.assertEquals(v1, KCVSUtil.get(store1,k, c1, checktx));
        Assert.assertEquals(v2, KCVSUtil.get(store2,k, c2, checktx));
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
                Assert.assertTrue(e instanceof LockingException);
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

        IDAuthority idAuthority = getIDAuthority(0);

        idAuthority.setIDBlockSizer(blockSizer);
        long[] block = idAuthority.getIDBlock(0);
        Assert.assertEquals(1,block[0]);
        Assert.assertEquals(block[1], block[0] + blockSize);
        block = idAuthority.getIDBlock(0);

        Assert.assertEquals(1+blockSize,block[0]);
        Assert.assertEquals(block[1], block[0] + blockSize);
    }

    @Test
    public void testMultiIDAcquisition() throws StorageException, InterruptedException {
        final int numPartitions = 4;
        final int numAcquisitionsPerThreadPartition = 300;
        final int blockSize = 250;
        final IDBlockSizer blockSizer = new IDBlockSizer() {
            @Override
            public long getBlockSize(int partitionID) {
                return blockSize;
            }
        };

        final List<List<Long>> ids = new ArrayList<List<Long>>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            ids.add(Collections.synchronizedList(new ArrayList<Long>(numAcquisitionsPerThreadPartition * CONCURRENCY)));
        }

        Thread[] threads = new Thread[CONCURRENCY];
        for (int i = 0; i < CONCURRENCY; i++) {
            final IDAuthority idAuthority = getIDAuthority(i);
            idAuthority.setIDBlockSizer(blockSizer);

            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int j = 0; j < numAcquisitionsPerThreadPartition; j++) {
                            for (int p = 0; p < numPartitions; p++) {
                                long nextId = idAuthority.peekNextID(p);
                                long[] block = idAuthority.getIDBlock(p);
                                Assert.assertTrue(nextId <= block[0]);
                                Assert.assertEquals(block[0] + blockSize, block[1]);
                                Assert.assertFalse(ids.get(p).contains(block[0]));
                                ids.get(p).add(block[0]);
                            }
                        }
                    } catch (StorageException e) {
                        logger.error("Unexpected exception when testing multi-thread ID acqusition", e);
                        throw new RuntimeException(e);
                    }
                }
            });
            threads[i].start();
        }

        for (int i = 0; i < CONCURRENCY; i++) {
            threads[i].join();
        }

        for (int i = 0; i < numPartitions; i++) {
            List<Long> list = ids.get(i);
            Assert.assertEquals(numAcquisitionsPerThreadPartition * CONCURRENCY, list.size());
            Collections.sort(list);
            int pos = 0;
            int id = 1;
            while (pos < list.size()) {
                Assert.assertEquals(id, list.get(pos).longValue());
                id += blockSize;
                pos++;
            }
        }
    }


    @Test
    public void testLocalPartitionAcquisition() throws StorageException {
        for (int c = 0; c < CONCURRENCY; c++) {
            if (getManager(c).getFeatures().hasLocalKeyPartition()) {
                try {
                    StaticBuffer[] partition = getIDAuthority(c).getLocalIDPartition();
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
     * 
     * Run lots of acquireLock() and commit() ops on a provided store and txn.
     * 
     * Used by {@link LockKeyColumnValueStoreTest#parallelLockStressTest()}.
     * 
     * @author "Dan LaRocque <dalaro@hopcount.org>"
     *
     */
    private class LockStressor implements Runnable {
        
        private final KeyColumnValueStoreManager manager;
        private final KeyColumnValueStore store;
        private final CountDownLatch doneLatch;
        private final int opCount;
        private final StaticBuffer toLock;
        
        private int succeeded = 0;

        private LockStressor(KeyColumnValueStoreManager manager,
                             KeyColumnValueStore store,
                             CountDownLatch doneLatch,
                             int opCount,
                             StaticBuffer toLock) {
            this.manager = manager;
            this.store = store;
            this.doneLatch = doneLatch;
            this.opCount = opCount;
            this.toLock = toLock;
        }

        @Override
        public void run() {
            // Catch & log exceptions, then pass to the starter thread
            for (int opIndex = 0; opIndex < opCount; opIndex++) {
                StoreTransaction tx;

                try {
                    tx = newTransaction(manager);
                    store.acquireLock(toLock, toLock, null, tx);
                    store.mutate(toLock, ImmutableList.<Entry>of(), Arrays.asList(toLock), tx);
                    tx.commit();
                    succeeded++;
                } catch (Throwable t) {
                    logger.error("Unexpected locking-related exception", t);
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

    private KeyColumnValueStoreManager getManager(int index) throws StorageException {
        if (MANAGERS[index] != null)
            return MANAGERS[index];

        KeyColumnValueStoreManager newManager = openStorageManager(index);
        MANAGERS[index] = newManager;

        return newManager;
    }

    private KeyColumnValueStore getStore(int index) throws StorageException {
        if (STORES[index] != null)
            return STORES[index];

        KeyColumnValueStoreManager manager = getManager(index);
        KeyColumnValueStore newStore = manager.openDatabase(DB_NAME);

        StoreFeatures storeFeatures = manager.getFeatures();
        Configuration configuration = getStorageConfiguration(index);

        if (!storeFeatures.supportsLocking()) {
            if (storeFeatures.supportsTransactions()) {
                STORES[index] = new TransactionalLockStore(newStore);
            } else if (storeFeatures.supportsConsistentKeyOperations()) {
                ConsistentKeyLockConfiguration lockConfiguration = new ConsistentKeyLockConfiguration(configuration, "store" + index);
                STORES[index] = new ConsistentKeyLockStore(newStore, manager.openDatabase(DB_NAME + "_lock_"), lockConfiguration);
            } else throw new IllegalArgumentException("Store needs to support some form of locking");
        } else {
            STORES[index] = newStore;
        }

        return STORES[index];
    }

    private IDAuthority getIDAuthority(int index) throws StorageException {
        if (ID_AUTHORITIES[index] != null)
            return ID_AUTHORITIES[index];

        KeyColumnValueStoreManager manager = getManager(index);
        KeyColumnValueStore idStore = manager.openDatabase("ids");
        StoreFeatures storeFeatures = manager.getFeatures();
        Configuration configuration = getStorageConfiguration(index);

        if (storeFeatures.supportsTransactions())
            ID_AUTHORITIES[index] = new TransactionalIDManager(idStore, manager, configuration);
        else if (storeFeatures.supportsConsistentKeyOperations())
            ID_AUTHORITIES[index] = new ConsistentKeyIDManager(idStore, manager, configuration);
        else throw new IllegalArgumentException("Cannot open id store");

        return ID_AUTHORITIES[index];
    }

    private Configuration getStorageConfiguration(final int index) {
        return new BaseConfiguration() {{
            addProperty(ConsistentKeyLockStore.LOCAL_LOCK_MEDIATOR_PREFIX_KEY, "store" + index);
            addProperty(GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY, (short) index);
            addProperty(GraphDatabaseConfiguration.LOCK_EXPIRE_MS, EXPIRE_MS);
            addProperty(GraphDatabaseConfiguration.IDAUTHORITY_RETRY_COUNT_KEY, 50);
            addProperty(GraphDatabaseConfiguration.IDAUTHORITY_WAIT_MS_KEY, 100);
        }};
    }

    private void reopenIDAuthority(int index) throws StorageException {
        if (ID_AUTHORITIES[index] != null) {
            ID_AUTHORITIES[index].close();
            ID_AUTHORITIES[index] = null;
        }

        getIDAuthority(index);
    }
}
