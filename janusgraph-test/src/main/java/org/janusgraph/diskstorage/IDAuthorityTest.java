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

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import com.google.common.base.Preconditions;
import org.janusgraph.StorageSetup;

import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.idmanagement.ConflictAvoidanceMode;
import org.janusgraph.diskstorage.idmanagement.ConsistentKeyIDAuthority;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;
import static org.junit.Assert.*;

import org.janusgraph.graphdb.database.idassigner.IDBlockSizer;
import org.janusgraph.graphdb.database.idassigner.IDPoolExhaustedException;
import org.janusgraph.testutil.TestGraphConfigs;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@RunWith(Parameterized.class)
public abstract class IDAuthorityTest {

    private static final Logger log =
            LoggerFactory.getLogger(IDAuthorityTest.class);

    public static final int CONCURRENCY = 8;
    public static final int MAX_NUM_PARTITIONS = 4;
    public static final String DB_NAME = "test";

    public static final Duration GET_ID_BLOCK_TIMEOUT = Duration.ofMillis(300000L);

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        final List<Object[]> configurations = new ArrayList<>();

        ModifiableConfiguration c = getBasicConfig();
        configurations.add(new Object[]{c.getConfiguration()});

        c = getBasicConfig();
        c.set(IDAUTHORITY_CAV_BITS,9);
        c.set(IDAUTHORITY_CAV_TAG,511);
        configurations.add(new Object[]{c.getConfiguration()});

        c = getBasicConfig();
        c.set(IDAUTHORITY_CAV_RETRIES,10);
        c.set(IDAUTHORITY_WAIT, Duration.ofMillis(10L));
        c.set(IDAUTHORITY_CAV_BITS,7);
        //c.set(IDAUTHORITY_RANDOMIZE_UNIQUEID,true);
        c.set(IDAUTHORITY_CONFLICT_AVOIDANCE, ConflictAvoidanceMode.GLOBAL_AUTO);
        configurations.add(new Object[]{c.getConfiguration()});

        return configurations;
    }

    public static ModifiableConfiguration getBasicConfig() {
        ModifiableConfiguration c = GraphDatabaseConfiguration.buildGraphConfiguration();
        c.set(IDAUTHORITY_WAIT, Duration.ofMillis(100L));
        c.set(IDS_BLOCK_SIZE,400);
        return c;
    }

    public KeyColumnValueStoreManager[] manager;
    public IDAuthority[] idAuthorities;

    public final WriteConfiguration baseStoreConfiguration;

    public final int uidBitWidth;
    public final boolean hasFixedUid;
    public final boolean hasEmptyUid;
    public final long blockSize;
    public final long idUpperBoundBitWidth;
    public final long idUpperBound;

    public IDAuthorityTest(WriteConfiguration baseConfig) {
        Preconditions.checkNotNull(baseConfig);
        TestGraphConfigs.applyOverrides(baseConfig);
        this.baseStoreConfiguration = baseConfig;
        Configuration config = StorageSetup.getConfig(baseConfig);
        uidBitWidth = config.get(IDAUTHORITY_CAV_BITS);
        //hasFixedUid = !config.get(IDAUTHORITY_RANDOMIZE_UNIQUEID);
        hasFixedUid = !ConflictAvoidanceMode.GLOBAL_AUTO.equals(config.get(IDAUTHORITY_CONFLICT_AVOIDANCE));
        hasEmptyUid = uidBitWidth==0;
        blockSize = config.get(IDS_BLOCK_SIZE);
        idUpperBoundBitWidth = 30;
        idUpperBound = 1L <<idUpperBoundBitWidth;
    }

    @Before
    public void setUp() throws Exception {
        StoreManager m = openStorageManager();
        m.clearStorage();
        m.close();
        open();
    }

    public abstract KeyColumnValueStoreManager openStorageManager() throws BackendException;

    public void open() throws BackendException {
        manager = new KeyColumnValueStoreManager[CONCURRENCY];
        idAuthorities = new IDAuthority[CONCURRENCY];

        for (int i = 0; i < CONCURRENCY; i++) {

            ModifiableConfiguration sc = StorageSetup.getConfig(baseStoreConfiguration.copy());
            //sc.set(GraphDatabaseConfiguration.INSTANCE_RID_SHORT,(short)i);
            sc.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_SUFFIX, (short)i);
            if (!sc.has(UNIQUE_INSTANCE_ID)) {
                String uniqueGraphId = getOrGenerateUniqueInstanceId(sc);
                log.debug("Setting unique instance id: {}", uniqueGraphId);
                sc.set(UNIQUE_INSTANCE_ID, uniqueGraphId);
            }
            sc.set(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS,MAX_NUM_PARTITIONS);

            manager[i] = openStorageManager();
            StoreFeatures storeFeatures = manager[i].getFeatures();
            KeyColumnValueStore idStore = manager[i].openDatabase("ids");
            if (storeFeatures.isKeyConsistent())
                idAuthorities[i] = new ConsistentKeyIDAuthority(idStore, manager[i], sc);
            else throw new IllegalArgumentException("Cannot open id store");
        }
    }

    @After
    public void tearDown() throws Exception {
        close();
    }

    public void close() throws BackendException {
        for (int i = 0; i < CONCURRENCY; i++) {
            idAuthorities[i].close();
            manager[i].close();
        }
    }

    private class InnerIDBlockSizer implements IDBlockSizer {

        @Override
        public long getBlockSize(int idNamespace) {
            return blockSize;
        }

        @Override
        public long getIdUpperBound(int idNamespace) {
            return idUpperBound;
        }
    }

    private void checkBlock(IDBlock block, LongSet ids) {
        assertEquals(blockSize,block.numIds());
        for (int i=0;i<blockSize;i++) {
            long id = block.getId(i);
            assertEquals(id,block.getId(i));
            assertFalse(ids.contains(id));
            assertTrue(id<idUpperBound);
            assertTrue(id>0);
            ids.add(id);
        }
        if (hasEmptyUid) {
            assertEquals(blockSize-1,block.getId(block.numIds()-1)-block.getId(0));
        }
        try {
            block.getId(blockSize);
            fail();
        } catch (ArrayIndexOutOfBoundsException ignored) {}
    }

//    private void checkIdList(List<Long> ids) {
//        Collections.sort(ids);
//        for (int i=1;i<ids.size();i++) {
//            long current = ids.get(i);
//            long previous = ids.get(i-1);
//            Assert.assertTrue(current>0);
//            Assert.assertTrue(previous>0);
//            Assert.assertTrue("ID block allocated twice: blockstart=" + current + ", indices=(" + i + ", " + (i-1) + ")", current!=previous);
//            Assert.assertTrue("ID blocks allocated in non-increasing order: " + previous + " then " + current, current>previous);
//            Assert.assertTrue(previous+blockSize<=current);
//
//            if (hasFixedUid) {
//                Assert.assertTrue(current + " vs " + previous, 0 == (current - previous) % blockSize);
//                final long skipped = (current - previous) / blockSize;
//                Assert.assertTrue(0 <= skipped);
//            }
//        }
//    }

    @Test
    public void testAuthorityUniqueIDsAreDistinct() {
        /* Check that each IDAuthority was created with a unique id. Duplicate
         * values reflect a problem in either this test or the
         * implementation-under-test.
         */
        final Set<String> uniqueIds = new HashSet<>();
        final String uidErrorMessage = "Uniqueness failure detected for config option " + UNIQUE_INSTANCE_ID.getName();
        for (int i = 0; i < CONCURRENCY; i++) {
            String uid = idAuthorities[i].getUniqueID();
            Assert.assertTrue(uidErrorMessage, !uniqueIds.contains(uid));
            uniqueIds.add(uid);
        }
        assertEquals(uidErrorMessage, CONCURRENCY, uniqueIds.size());
    }

    @Test
    public void testSimpleIDAcquisition() throws BackendException {
        final IDBlockSizer blockSizer = new InnerIDBlockSizer();
        idAuthorities[0].setIDBlockSizer(blockSizer);
        int numTrials = 100;
        LongSet ids = new LongHashSet((int)blockSize*numTrials);
        long previous = 0;
        for (int i=0;i<numTrials;i++) {
            IDBlock block = idAuthorities[0].getIDBlock(0, 0, GET_ID_BLOCK_TIMEOUT);
            checkBlock(block,ids);
            if (hasEmptyUid) {
                if (previous!=0)
                    assertEquals(previous+1, block.getId(0));
                previous=block.getId(block.numIds()-1);
            }
        }
    }

    @Test
    public void testIDExhaustion() throws BackendException {
        final int chunks = 30;
        final IDBlockSizer blockSizer = new IDBlockSizer() {
            @Override
            public long getBlockSize(int idNamespace) {
                return ((1L <<(idUpperBoundBitWidth-uidBitWidth))-1)/chunks;
            }

            @Override
            public long getIdUpperBound(int idNamespace) {
                return idUpperBound;
            }
        };
        idAuthorities[0].setIDBlockSizer(blockSizer);
        if (hasFixedUid) {
            for (int i=0;i<chunks;i++) {
                idAuthorities[0].getIDBlock(0,0,GET_ID_BLOCK_TIMEOUT);
            }
            try {
                idAuthorities[0].getIDBlock(0,0,GET_ID_BLOCK_TIMEOUT);
                Assert.fail();
            } catch (IDPoolExhaustedException ignored) {}
        } else {
            for (int i=0;i<(chunks*Math.max(1,(1<<uidBitWidth)/10));i++) {
                idAuthorities[0].getIDBlock(0,0,GET_ID_BLOCK_TIMEOUT);
            }
            try {
                for (int i=0;i<(chunks*Math.max(1,(1<<uidBitWidth)*9/10));i++) {
                    idAuthorities[0].getIDBlock(0,0,GET_ID_BLOCK_TIMEOUT);
                }
                Assert.fail();
            } catch (IDPoolExhaustedException ignored) {}
        }
    }

    @Test
    public void testLocalPartitionAcquisition() throws BackendException {
        for (int c = 0; c < CONCURRENCY; c++) {
            if (manager[c].getFeatures().hasLocalKeyPartition()) {
                try {
                    List<KeyRange> partitions = idAuthorities[c].getLocalIDPartition();
                    for (KeyRange range : partitions) {
                        assertEquals(range.getStart().length(), range.getEnd().length());
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
    public void testManyThreadsOneIDAuthority() throws InterruptedException, ExecutionException {

        ExecutorService es = Executors.newFixedThreadPool(CONCURRENCY);

        final IDAuthority targetAuthority = idAuthorities[0];
        targetAuthority.setIDBlockSizer(new InnerIDBlockSizer());
        final int targetPartition = 0;
        final int targetNamespace = 2;

        final ConcurrentLinkedQueue<IDBlock> blocks = new ConcurrentLinkedQueue<>();
        final int blocksPerThread = 40;
        final List <Future<Void>> futures = new ArrayList<>(CONCURRENCY);

        // Start some concurrent threads getting blocks the same ID authority and same partition in that authority
        for (int c = 0; c < CONCURRENCY; c++) {
            futures.add(es.submit(new Callable<Void>() {
                @Override
                public Void call() {
                    try {
                        getBlock();
                    } catch (BackendException e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                }

                private void getBlock() throws BackendException {
                    for (int i = 0; i < blocksPerThread; i++) {
                        IDBlock block = targetAuthority.getIDBlock(targetPartition,targetNamespace,
                               GET_ID_BLOCK_TIMEOUT);
                        Assert.assertNotNull(block);
                        blocks.add(block);
                    }
                }
            }));
        }

        for (Future<Void> f : futures) {
            f.get();
        }

        es.shutdownNow();

        assertEquals(blocksPerThread * CONCURRENCY, blocks.size());
        LongSet ids = new LongHashSet((int)blockSize*blocksPerThread*CONCURRENCY);
        for (IDBlock block : blocks) checkBlock(block,ids);
    }

    @Test
    public void testMultiIDAcquisition() throws Throwable {
        final int numPartitions = MAX_NUM_PARTITIONS;
        final int numAcquisitionsPerThreadPartition = 100;
        final IDBlockSizer blockSizer = new InnerIDBlockSizer();
        for (int i = 0; i < CONCURRENCY; i++) idAuthorities[i].setIDBlockSizer(blockSizer);
        final List<ConcurrentLinkedQueue<IDBlock>> ids = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            ids.add(new ConcurrentLinkedQueue<>());
        }

        final int maxIterations = numAcquisitionsPerThreadPartition * numPartitions * 2;
        final Collection<Future<?>> futures = new ArrayList<>(CONCURRENCY);
        ExecutorService es = Executors.newFixedThreadPool(CONCURRENCY);

        final Set<String> uniqueIds = new HashSet<>(CONCURRENCY);
        for (int i = 0; i < CONCURRENCY; i++) {
            final IDAuthority idAuthority = idAuthorities[i];
            final IDStressor stressRunnable = new IDStressor(
                    numAcquisitionsPerThreadPartition, numPartitions,
                    maxIterations, idAuthority, ids);
            uniqueIds.add(idAuthority.getUniqueID());
            futures.add(es.submit(stressRunnable));
        }

        // If this fails, it's likely to be a bug in the test rather than the
        // IDAuthority (the latter is technically possible, just less likely)
        assertEquals(CONCURRENCY, uniqueIds.size());

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        }

        for (int i = 0; i < numPartitions; i++) {
            ConcurrentLinkedQueue<IDBlock> list = ids.get(i);
            assertEquals(numAcquisitionsPerThreadPartition * CONCURRENCY, list.size());
            LongSet idSet = new LongHashSet((int)blockSize*list.size());
            for (IDBlock block : list) checkBlock(block,idSet);
        }

        es.shutdownNow();
    }


    private class IDStressor implements Runnable {

        private final int numRounds;
        private final int numPartitions;
        private final int maxIterations;
        private final IDAuthority authority;
        private final List<ConcurrentLinkedQueue<IDBlock>> allocatedBlocks;

        private static final long sleepMS = 250L;

        private IDStressor(int numRounds, int numPartitions, int maxIterations,
                           IDAuthority authority, List<ConcurrentLinkedQueue<IDBlock>> ids) {
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

                    final IDBlock block = allocate(p);

                    if (null == block) {
                        Thread.sleep(sleepMS);
                        p--;
                    } else {
                        allocatedBlocks.get(p).add(block);
                        if (hasEmptyUid) {
                            long start = block.getId(0);
                            Assert.assertTrue("Previous block start "
                                    + lastStart[p] + " exceeds next block start "
                                    + start, lastStart[p] <= start);
                            lastStart[p] = start;
                        }
                    }
                }
            }
        }

        private IDBlock allocate(int partitionIndex) {

            IDBlock block;
            try {
                block = authority.getIDBlock(partitionIndex,partitionIndex,GET_ID_BLOCK_TIMEOUT);
            } catch (BackendException e) {
                log.error("Unexpected exception while getting ID block", e);
                return null;
            }
            /*
             * This is not guaranteed in the consistent-key implementation.
             * Writers of ID block claims in that implementation delete their
             * writes if they take too long. A peek can see this short-lived
             * block claim even though a subsequent getIDBlock() does not.
             */
//            Assert.assertTrue(nextId <= block[0]);
            if (hasEmptyUid) assertEquals(block.getId(0)+ blockSize-1, block.getId(blockSize-1));
            log.trace("Obtained ID block {}", block);

            return block;
        }

        private void throwIterationsExceededException() {
            throw new RuntimeException(
                    "Exceeded maximum ID allocation iteration count ("
                            + maxIterations + "); too many timeouts?");
        }
    }

}
