package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.idmanagement.ConsistentKeyIDManager;
import com.thinkaurelius.titan.diskstorage.idmanagement.TransactionalIDManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@RunWith(Parameterized.class)
public abstract class IDAllocationTest {

    private static final Logger log =
            LoggerFactory.getLogger(IDAllocationTest.class);

    public static final int CONCURRENCY = 8;
    public static final String DB_NAME = "test";

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        List<Object[]> configurations = new ArrayList<Object[]>();

        BaseConfiguration c = new BaseConfiguration();
        c.addProperty(IDAUTHORITY_RETRY_COUNT_KEY, 50);
        c.addProperty(IDAUTHORITY_WAIT_MS_KEY, 100);
        configurations.add(new Object[]{c});

        c = new BaseConfiguration();
        c.addProperty(IDAUTHORITY_RETRY_COUNT_KEY, 50);
        c.addProperty(IDAUTHORITY_WAIT_MS_KEY, 100);
        c.addProperty(IDAUTHORITY_UNIQUE_ID_BITS_KEY,12);
        c.addProperty(IDAUTHORITY_UNIQUE_ID_KEY,1024);
        configurations.add(new Object[]{c});

        c = new BaseConfiguration();
        c.addProperty(IDAUTHORITY_RETRY_COUNT_KEY, 10);
        c.addProperty(IDAUTHORITY_WAIT_MS_KEY, 10);
        c.addProperty(IDAUTHORITY_UNIQUE_ID_BITS_KEY,7);
        c.addProperty(IDAUTHORITY_RANDOMIZE_UNIQUE_ID_KEY,true);
        configurations.add(new Object[]{c});

        return configurations;
    }

    public KeyColumnValueStoreManager[] manager;
    public IDAuthority[] idAuthorities;

    public Configuration baseStoreConfiguration;

    public final int uidBitWidth;
    public final boolean hasFixedUid;

    public IDAllocationTest(Configuration baseConfig) {
        Preconditions.checkNotNull(baseConfig);
        this.baseStoreConfiguration = baseConfig;
        hasFixedUid = !baseConfig.getBoolean(IDAUTHORITY_RANDOMIZE_UNIQUE_ID_KEY,false);
        uidBitWidth = baseConfig.getInt(IDAUTHORITY_UNIQUE_ID_BITS_KEY,IDAUTHORITY_UNIQUE_ID_BITS_DEFAULT);
    }

    @Before
    public void setUp() throws Exception {
        openStorageManager(0).clearStorage();
        open();
    }

    public abstract KeyColumnValueStoreManager openStorageManager(int id) throws StorageException;

    public void open() throws StorageException {
        manager = new KeyColumnValueStoreManager[CONCURRENCY];
        idAuthorities = new IDAuthority[CONCURRENCY];

        for (int i = 0; i < CONCURRENCY; i++) {
            manager[i] = openStorageManager(i);
            StoreFeatures storeFeatures = manager[i].getFeatures();

            Configuration sc = new BaseConfiguration();
            Iterator<String> keyiter = baseStoreConfiguration.getKeys();
            while (keyiter.hasNext()) {
                String key = keyiter.next();
                sc.addProperty(key,baseStoreConfiguration.getProperty(key));
            }
            sc.addProperty(GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY, (short) i);


            KeyColumnValueStore idStore = manager[i].openDatabase("ids");
            if (storeFeatures.supportsTransactions())
                idAuthorities[i] = new TransactionalIDManager(idStore, manager[i], sc);
            else if (storeFeatures.supportsConsistentKeyOperations())
                idAuthorities[i] = new ConsistentKeyIDManager(idStore, manager[i], sc);
            else throw new IllegalArgumentException("Cannot open id store");
        }
    }

    private long[] removeMarker(long[] block) {
        for (int i=0;i<block.length;i++)
            block[i]=removeMarker(block[i]);
        return block;
    }

    private long removeMarker(long block) {
        return block>>>uidBitWidth;
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
        long[] block = removeMarker(idAuthorities[0].getIDBlock(0));
        Assert.assertEquals(1, block[0]);
        Assert.assertEquals(block[1], block[0] + blockSize);
        block = removeMarker(idAuthorities[0].getIDBlock(0));
        if (hasFixedUid) Assert.assertEquals(1 + blockSize, block[0]);
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
            if (!hasFixedUid) continue;
            Collections.sort(list);

            int pos = 0;
            long id = 1;
            while (pos < list.size()) {
                long block = removeMarker(list.get(pos).longValue());
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
                Assert.assertTrue(block + " vs " + id, 0 == (block - id) % blockSize);
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
                        Assert.assertFalse(allocatedBlocks.get(p).contains(start));
                        allocatedBlocks.get(p).add(start);
                        if (hasFixedUid) {
                            start = removeMarker(start);
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
                block = authority.getIDBlock(partitionIndex);
            } catch (StorageException e) {
                log.error("Unexpected exception while getting ID block", e);
                return null;
            }
            long start = block[0];
            block = removeMarker(block);
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
