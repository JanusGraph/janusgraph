package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.idmanagement.ConsistentKeyIDManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDPoolExhaustedException;
import com.thinkaurelius.titan.testutil.TestGraphConfigs;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@RunWith(Parameterized.class)
public abstract class IDAllocationTest {

    private static final Logger log =
            LoggerFactory.getLogger(IDAllocationTest.class);

    public static final int CONCURRENCY = 8;
    public static final String DB_NAME = "test";

    public static final long GET_ID_BLOCK_TIMEOUT_MS = 300000L;

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        List<Object[]> configurations = new ArrayList<Object[]>();

        ModifiableConfiguration c = getBasicConfig();
        configurations.add(new Object[]{c.getConfiguration()});

        c = getBasicConfig();
        c.set(IDAUTHORITY_UNIQUE_ID_BITS,9);
        c.set(IDAUTHORITY_UNIQUE_ID,511);
        configurations.add(new Object[]{c.getConfiguration()});

        c = getBasicConfig();
        c.set(IDAUTHORITY_UNIQUEID_RETRY_COUNT,10);
        c.set(IDAUTHORITY_WAIT_MS,10);
        c.set(IDAUTHORITY_UNIQUE_ID_BITS,7);
        c.set(IDAUTHORITY_RANDOMIZE_UNIQUE_ID,true);
        configurations.add(new Object[]{c.getConfiguration()});

        return configurations;
    }

    public static ModifiableConfiguration getBasicConfig() {
        ModifiableConfiguration c = GraphDatabaseConfiguration.buildConfiguration();
        c.set(IDAUTHORITY_UNIQUEID_RETRY_COUNT,50);
        c.set(IDAUTHORITY_WAIT_MS,100);
        c.set(IDS_BLOCK_SIZE,400);
        return c;
    }

    public KeyColumnValueStoreManager[] manager;
    public IDAuthority[] idAuthorities;

    public WriteConfiguration baseStoreConfiguration;

    public final int uidBitWidth;
    public final boolean hasFixedUid;
    public final long blockSize;
    public final long idUpperBoundBitWidth;
    public final long idUpperBound;

    public IDAllocationTest(WriteConfiguration baseConfig) {
        Preconditions.checkNotNull(baseConfig);
        TestGraphConfigs.applyOverrides(baseConfig);
        this.baseStoreConfiguration = baseConfig;
        Configuration config = StorageSetup.getConfig(baseConfig);
        hasFixedUid = !config.get(IDAUTHORITY_RANDOMIZE_UNIQUE_ID);
        uidBitWidth = config.get(IDAUTHORITY_UNIQUE_ID_BITS);
        blockSize = config.get(IDS_BLOCK_SIZE);
        idUpperBoundBitWidth = 30;
        idUpperBound = 1l<<idUpperBoundBitWidth;
    }

    @Before
    public void setUp() throws Exception {
        openStorageManager().clearStorage();
        open();
    }

    public abstract KeyColumnValueStoreManager openStorageManager() throws StorageException;

    public void open() throws StorageException {
        manager = new KeyColumnValueStoreManager[CONCURRENCY];
        idAuthorities = new IDAuthority[CONCURRENCY];

        for (int i = 0; i < CONCURRENCY; i++) {

            ModifiableConfiguration sc = StorageSetup.getConfig(baseStoreConfiguration.copy());
            sc.set(GraphDatabaseConfiguration.INSTANCE_RID_SHORT,(short)i);
            if (!sc.has(UNIQUE_INSTANCE_ID)) {
                String uniqueGraphId = getOrGenerateUniqueInstanceId(sc);
                log.debug("Setting unique instance id: {}", uniqueGraphId);
                sc.set(UNIQUE_INSTANCE_ID, uniqueGraphId);
            }

            manager[i] = openStorageManager();
            StoreFeatures storeFeatures = manager[i].getFeatures();
            KeyColumnValueStore idStore = manager[i].openDatabase("ids");
            if (storeFeatures.isKeyConsistent())
                idAuthorities[i] = new ConsistentKeyIDManager(idStore, manager[i], sc);
            else throw new IllegalArgumentException("Cannot open id store");
        }
    }

    @After
    public void tearDown() throws Exception {
        close();
    }

    public void close() throws StorageException {
        for (int i = 0; i < CONCURRENCY; i++) {
            idAuthorities[i].close();
            manager[i].close();
        }
    }

    private class InnerIDBlockSizer implements IDBlockSizer {

        @Override
        public long getBlockSize(int partitionID) {
            return blockSize;
        }

        @Override
        public long getIdUpperBound(int partitionID) {
            return idUpperBound;
        }
    }

    private void checkIdList(List<Long> ids) {
        Collections.sort(ids);
        for (int i=1;i<ids.size();i++) {
            long current = ids.get(i);
            long previous = ids.get(i-1);
            Assert.assertTrue(current>0);
            Assert.assertTrue(previous>0);
            Assert.assertTrue("ID block allocated twice: blockstart=" + current + ", indices=(" + i + ", " + (i-1) + ")", current!=previous);
            Assert.assertTrue("ID blocks allocated in non-increasing order: " + previous + " then " + current, current>previous);
            Assert.assertTrue(previous+blockSize<=current);

            if (hasFixedUid) {
                Assert.assertTrue(current + " vs " + previous, 0 == (current - previous) % blockSize);
                final long skipped = (current - previous) / blockSize;
                Assert.assertTrue(0 <= skipped);
            }
        }
    }

    @Test
    public void testAuthorityUniqueIDsAreDistinct() {
        /* Check that each IDAuthority was created with a unique id. Duplicate
         * values reflect a problem in either this test or the
         * implementation-under-test.
         */
        Set<String> uids = new HashSet<String>();
        String uidErrorMessage = "Uniqueness failure detected for config option " + UNIQUE_INSTANCE_ID.getName();
        for (int i = 0; i < CONCURRENCY; i++) {
            String uid = idAuthorities[i].getUniqueID();
            Assert.assertTrue(uidErrorMessage, !uids.contains(uid));
            uids.add(uid);
        }
        Assert.assertEquals(uidErrorMessage, CONCURRENCY, uids.size());
    }

    @Test
    public void testSimpleIDAcquisition() throws StorageException {
        final IDBlockSizer blockSizer = new InnerIDBlockSizer();
        idAuthorities[0].setIDBlockSizer(blockSizer);
        List<Long> ids = Lists.newArrayList();
        long previous = 0;
        for (int i=0;i<100;i++) {
            long[] block = idAuthorities[0].getIDBlock(0, GET_ID_BLOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            Assert.assertEquals(block[1], block[0] + blockSize);
            ids.add(block[0]);
            if (hasFixedUid) {
                if (previous!=0)
                    Assert.assertEquals(previous,block[0]);
                previous=block[1];
            }
        }
        checkIdList(ids);
    }

    @Test
    public void testIDExhaustion() throws StorageException {
        final int chunks = 30;
        final IDBlockSizer blockSizer = new IDBlockSizer() {
            @Override
            public long getBlockSize(int partitionID) {
                return ((1l<<(idUpperBoundBitWidth-uidBitWidth))-1)/chunks;
            }

            @Override
            public long getIdUpperBound(int partitionID) {
                return idUpperBound;
            }
        };
        idAuthorities[0].setIDBlockSizer(blockSizer);
        if (hasFixedUid) {
            for (int i=0;i<chunks;i++) {
                idAuthorities[0].getIDBlock(0, GET_ID_BLOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
            try {
                idAuthorities[0].getIDBlock(0, GET_ID_BLOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                Assert.fail();
            } catch (IDPoolExhaustedException e) {}
        } else {
            for (int i=0;i<(chunks*Math.max(1,(1<<uidBitWidth)/10));i++) {
                idAuthorities[0].getIDBlock(0, GET_ID_BLOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
            try {
                for (int i=0;i<(chunks*Math.max(1,(1<<uidBitWidth)*9/10));i++) {
                    idAuthorities[0].getIDBlock(0, GET_ID_BLOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                }
                Assert.fail();
            } catch (IDPoolExhaustedException e) {}
        }
    }

    @Test
    public void testMultiIDAcquisition() throws Throwable {
        final int numPartitions = 4;
        final int numAcquisitionsPerThreadPartition = 300;
        final IDBlockSizer blockSizer = new InnerIDBlockSizer();
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
                    maxIterations, idAuthority, ids);
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
            checkIdList(list);
        }

        es.shutdownNow();
    }


    @Test
    public void testLocalPartitionAcquisition() throws StorageException {
        for (int c = 0; c < CONCURRENCY; c++) {
            if (manager[c].getFeatures().hasLocalKeyPartition()) {
                try {
                    List<KeyRange> partitions = idAuthorities[c].getLocalIDPartition();
                    for (KeyRange range : partitions) {
                        Assert.assertEquals(range.getStart().length(), range.getEnd().length());
                        for (int i = 0; i < 2; i++) {
                            Assert.assertTrue(range.getAt(i).length() >= 4);
                        }
                    }
                } catch (UnsupportedOperationException e) {
                    Assert.fail();
                }
            }
        }
    }

    @Test
    public void testManyThreadsOneIDAuthority() throws StorageException, InterruptedException, ExecutionException {

        ExecutorService es = Executors.newFixedThreadPool(CONCURRENCY);

        final IDAuthority targetAuthority = idAuthorities[0];
        targetAuthority.setIDBlockSizer(new InnerIDBlockSizer());
        final int targetPartition = 0;
        final ConcurrentLinkedDeque<Long> blocks = new ConcurrentLinkedDeque<Long>();
        final int blocksPerThread = 40;
        Assert.assertTrue(0 < blocksPerThread);
        List <Future<Void>> futures = new ArrayList<Future<Void>>(CONCURRENCY);

        // Start some concurrent threads getting blocks the same ID authority and same partition in that authority
        for (int c = 0; c < CONCURRENCY; c++) {
            futures.add(es.submit(new Callable<Void>() {
                @Override
                public Void call() {
                    try {
                        getBlock();
                    } catch (StorageException e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                }

                private void getBlock() throws StorageException {
                    for (int i = 0; i < blocksPerThread; i++) {
                        long block[] = targetAuthority.getIDBlock(targetPartition,
                                GET_ID_BLOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                        Assert.assertNotNull(block);
                        blocks.add(block[0]);
                    }
                }
            }));
        }

        for (Future<Void> f : futures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                throw e;
            }
        }

        es.shutdownNow();

        /*
         * Sorting is a reasonable compromise; we can't expect to keep the list
         * of blocks in allocation-order without effectively serializing the
         * concurrent threads.
         *
         * A superior approach might be to insert a mock or spy object in the
         * IDAuthority implementation itself, rather than attempting to observe
         * allocations from concurrent threads -- maybe supportable with future
         * refactoring.
         */
        List<Long> sortedBlocks = new ArrayList<Long>(blocks.size());
        sortedBlocks.addAll(blocks);
        Collections.sort(sortedBlocks);

        checkIdList(sortedBlocks);
        Assert.assertEquals(blocksPerThread * CONCURRENCY, sortedBlocks.size());
    }


    private class IDStressor implements Runnable {

        private final int numRounds;
        private final int numPartitions;
        private final int maxIterations;
        private final IDAuthority authority;
        private final List<List<Long>> allocatedBlocks;

        private static final long sleepMS = 250L;

        private IDStressor(int numRounds, int numPartitions, int maxIterations,
                           IDAuthority authority, List<List<Long>> ids) {
            this.numRounds = numRounds;
            this.numPartitions = numPartitions;
            this.maxIterations = maxIterations;
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

                    final Long start = allocate(p);

                    if (null == start) {
                        Thread.sleep(sleepMS);
                        p--;
                    } else {
                        allocatedBlocks.get(p).add(start);
                        if (hasFixedUid) {
                            Assert.assertTrue("Previous block start "
                                    + lastStart[p] + " exceeds next block start "
                                    + start, lastStart[p] <= start);
                            lastStart[p] = start;
                        }
                    }
                }
            }
        }

        private Long allocate(int partitionIndex) {

            long[] block;
            try {
                block = authority.getIDBlock(partitionIndex, GET_ID_BLOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (StorageException e) {
                log.error("Unexpected exception while getting ID block", e);
                return null;
            }
            long start = block[0];
            /*
             * This is not guaranteed in the consistentkey implementation.
             * Writers of ID block claims in that implementation delete their
             * writes if they take too long. A peek can see this short-lived
             * block claim even though a subsequent getblock does not.
             */
//            Assert.assertTrue(nextId <= block[0]);
            Assert.assertEquals(block[0] + blockSize, block[1]);
            log.trace("Obtained ID block {},{}", block[0], block[1]);

            return start;
        }

        private boolean throwIterationsExceededException() {
            throw new RuntimeException(
                    "Exceeded maximum ID allocation iteration count ("
                            + maxIterations + "); too many timeouts?");
        }
    }

}
