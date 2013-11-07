package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.idmanagement.ConsistentKeyIDManager;
import com.thinkaurelius.titan.diskstorage.idmanagement.TransactionalIDManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDPoolExhaustedException;
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
        c.addProperty(IDAUTHORITY_UNIQUE_ID_BITS_KEY,9);
        c.addProperty(IDAUTHORITY_UNIQUE_ID_KEY,511);
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
    public final long blockSize;
    public final long idUpperBoundBitWidth;
    public final long idUpperBound;

    public IDAllocationTest(Configuration baseConfig) {
        Preconditions.checkNotNull(baseConfig);
        this.baseStoreConfiguration = baseConfig;
        hasFixedUid = !baseConfig.getBoolean(IDAUTHORITY_RANDOMIZE_UNIQUE_ID_KEY,false);
        uidBitWidth = baseConfig.getInt(IDAUTHORITY_UNIQUE_ID_BITS_KEY,IDAUTHORITY_UNIQUE_ID_BITS_DEFAULT);
        blockSize = baseConfig.getLong(IDS_BLOCK_SIZE_KEY, 400);
        idUpperBoundBitWidth = 30;
        idUpperBound = 1l<<idUpperBoundBitWidth;
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

            BaseConfiguration sc = new BaseConfiguration();
            sc.copy(baseStoreConfiguration);
            sc.addProperty(GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY, (short) i);


            KeyColumnValueStore idStore = manager[i].openDatabase("ids");
            if (storeFeatures.supportsTransactions())
                idAuthorities[i] = new TransactionalIDManager(idStore, manager[i], sc);
            else if (storeFeatures.supportsConsistentKeyOperations())
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
            Assert.assertTrue(current!=previous);
            Assert.assertTrue(current>previous);
            Assert.assertTrue(previous+blockSize<=current);

            if (hasFixedUid) {
                Assert.assertTrue(current + " vs " + previous, 0 == (current - previous) % blockSize);
                final long skipped = (current - previous) / blockSize;
                Assert.assertTrue(0 <= skipped);
            }
        }
    }

    @Test
    public void testSimpleIDAcquisition() throws StorageException {
        final IDBlockSizer blockSizer = new InnerIDBlockSizer();
        idAuthorities[0].setIDBlockSizer(blockSizer);
        List<Long> ids = Lists.newArrayList();
        long previous = 0;
        for (int i=0;i<100;i++) {
            long[] block = idAuthorities[0].getIDBlock(0);
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
                idAuthorities[0].getIDBlock(0);
            }
            try {
                idAuthorities[0].getIDBlock(0);
                Assert.fail();
            } catch (IDPoolExhaustedException e) {}
        } else {
            for (int i=0;i<(chunks*Math.max(1,(1<<uidBitWidth)/10));i++) {
                idAuthorities[0].getIDBlock(0);
            }
            try {
                for (int i=0;i<(chunks*Math.max(1,(1<<uidBitWidth)*9/10));i++) {
                    idAuthorities[0].getIDBlock(0);
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
                block = authority.getIDBlock(partitionIndex);
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
