// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.diskstorage.inmemory;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.IDBlock;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.idmanagement.ConsistentKeyIDAuthority;
import org.janusgraph.diskstorage.keycolumnvalue.KCVSProxy;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.idassigner.IDBlockSizer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An ID authority claims blocks of different partitions, or of different id namespaces, in parallel, and blocks of one
 * partition and namespace one at a time. The inmemory store isn't distributed, so a claim neither waits
 * {@code ids.authority.wait-time} nor fails as too slow, whatever the test holds its write up for. The one-at-a-time
 * test holds a claim's write open and checks that a second claim of the same partition and namespace waits for the
 * lock instead of writing as well; the lock covers the write along with the reads around it. Without a lock which all
 * claims share, the block sizer still can't change once a claim has used it.
 */
public class ConsistentKeyIDAuthorityLockingTest {

    //The longest that a claim, a wait for another claim, or the end of the claimants may take
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final long BLOCK_SIZE = 100;
    //The most claims a test runs at once
    private static final int CLAIMANTS = 8;

    private InMemoryStoreManager manager;
    private ClaimObservingStore idStore;
    private ConsistentKeyIDAuthority authority;
    private ExecutorService claimants;

    @BeforeEach
    public void setUp() throws BackendException {
        manager = new InMemoryStoreManager();
        idStore = new ClaimObservingStore(manager.openDatabase("ids"));
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID, "authority");
        authority = new ConsistentKeyIDAuthority(idStore, manager, config);
        authority.setIDBlockSizer(sizer(BLOCK_SIZE));
        claimants = Executors.newFixedThreadPool(CLAIMANTS);
    }

    private static IDBlockSizer sizer(long blockSize) {
        return sizer(blockSize, new Semaphore(0));
    }

    //A sizer which releases a permit for each claim it sizes; a claim sizes its block right before it takes the lock
    private static IDBlockSizer sizer(long blockSize, Semaphore sized) {
        return new IDBlockSizer() {
            @Override
            public long getBlockSize(int idNamespace) {
                sized.release();
                return blockSize;
            }

            @Override
            public long getIdUpperBound(int idNamespace) {
                return 1L << 40;
            }
        };
    }

    @AfterEach
    public void tearDown() throws Exception {
        try {
            idStore.releaseHeldClaimWrites();
            claimants.shutdownNow();
            assertTrue(claimants.awaitTermination(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "a claim didn't end");
        } finally {
            try {
                authority.close();
            } finally {
                manager.close();
            }
        }
    }

    //Every claim sizes its block with the sizer set before the first claim, which the claims read without a shared lock
    @Test
    public void theBlockSizerCantChangeOnceAClaimHasUsedIt() throws Exception {
        authority.setIDBlockSizer(sizer(BLOCK_SIZE / 2));
        assertEquals(BLOCK_SIZE / 2, authority.getIDBlock(0, 0, TIMEOUT).numIds());

        assertThrows(IllegalStateException.class, () -> authority.setIDBlockSizer(sizer(BLOCK_SIZE)));
        assertEquals(BLOCK_SIZE / 2, authority.getIDBlock(1, 0, TIMEOUT).numIds());
    }

    @Test
    public void claimsOnDifferentPartitionsRunInParallel() throws Exception {
        assertClaimsMeet(0, 0, 1, 0);
    }

    @Test
    public void claimsOnDifferentNamespacesRunInParallel() throws Exception {
        assertClaimsMeet(0, 0, 0, 1);
    }

    //Each claim's write waits for the other's, and a lock which both claims shared would never let the second one in
    private void assertClaimsMeet(int partition, int namespace, int otherPartition, int otherNamespace) throws Exception {
        idStore.claimWritesMeet = new CyclicBarrier(2);
        final Future<IDBlock> claim = claimants.submit(() -> authority.getIDBlock(partition, namespace, TIMEOUT));
        final Future<IDBlock> other = claimants.submit(() -> authority.getIDBlock(otherPartition, otherNamespace, TIMEOUT));
        assertNotNull(claim.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        assertNotNull(other.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        assertEquals(2, idStore.mostClaimWritesAtOnce.get());
        assertNoLockLeft();
    }

    //While one claim's write is held open, a second claim of the same partition and namespace waits for the lock, where
    //a claim without it would write as well; and once the first claim has finished, a third waits for the second's,
    //whose lock the first's release must leave in place
    @Test
    public void claimsOnOnePartitionAndNamespaceRunOneAtATime() throws Exception {
        final Semaphore claimsSized = new Semaphore(0);
        authority.setIDBlockSizer(sizer(BLOCK_SIZE, claimsSized));
        final CountDownLatch firstWrite = idStore.holdClaimWrites();
        final Future<IDBlock> first = claimants.submit(() -> authority.getIDBlock(3, 0, TIMEOUT));
        assertTrue(idStore.heldWrites.tryAcquire(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "the first claim didn't write");
        assertTrue(claimsSized.tryAcquire(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        final Future<IDBlock> second = claimWhichWaitsForTheLock(claimsSized);

        //The first claim's write goes on, and the second claim's is held in its turn
        final CountDownLatch secondWrite = idStore.holdClaimWrites();
        firstWrite.countDown();
        assertTrue(idStore.heldWrites.tryAcquire(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "the second claim didn't write");
        final Future<IDBlock> third = claimWhichWaitsForTheLock(claimsSized);

        secondWrite.countDown();
        final Set<Long> firstIds = new HashSet<>();
        for (Future<IDBlock> claim : Arrays.asList(first, second, third)) {
            firstIds.add(claim.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).getId(0));
        }
        assertEquals(3, firstIds.size(), "two claims took the same block");
        assertEquals(1, idStore.mostClaimWritesAtOnce.get(), "claims of one partition and namespace wrote at once");
        assertNoLockLeft();
    }

    //Submits a claim on partition 3 and namespace 0 while another claim's write is held open, and returns it once it
    //waits for the lock: it has sized its block, which leaves the lock as all that is left before its read and write,
    //and that it waits for the lock shows only in its state, which it reaches within the moment after sizing
    private Future<IDBlock> claimWhichWaitsForTheLock(Semaphore claimsSized) throws Exception {
        final AtomicReference<Thread> claimant = new AtomicReference<>();
        final Future<IDBlock> claim = claimants.submit(() -> {
            claimant.set(Thread.currentThread());
            return authority.getIDBlock(3, 0, TIMEOUT);
        });
        assertTrue(claimsSized.tryAcquire(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "the claim didn't size its block");
        final long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (!waitsForALock(claimant.get()) && idStore.claimWritesInFlight.get() == 1) {
            assertTrue(System.nanoTime() < deadline, "the claim neither waited for the lock nor wrote");
            Thread.sleep(1);
        }
        assertEquals(1, idStore.claimWritesInFlight.get(), "claims of one partition and namespace wrote at once");
        return claim;
    }

    @Test
    public void claimsOnOnePartitionAndNamespaceTakeDistinctBlocks() throws Exception {
        final List<Future<IDBlock>> futures = new ArrayList<>(CLAIMANTS);
        for (int i = 0; i < CLAIMANTS; i++) {
            futures.add(claimants.submit(() -> authority.getIDBlock(3, 0, TIMEOUT)));
        }
        final Set<Long> firstIds = new HashSet<>();
        for (Future<IDBlock> future : futures) {
            final IDBlock block = future.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertEquals(BLOCK_SIZE, block.numIds());
            firstIds.add(block.getId(0));
        }
        assertEquals(CLAIMANTS, firstIds.size(), "two claims took the same block");
        assertEquals(1, idStore.mostClaimWritesAtOnce.get(), "claims on one partition and namespace overlapped");
        assertNoLockLeft();
    }

    //The authority keeps a lock only while a claim of its partition and namespace runs or waits
    private void assertNoLockLeft() throws ReflectiveOperationException {
        final Field locks = ConsistentKeyIDAuthority.class.getDeclaredField("blockClaimLocks");
        locks.setAccessible(true);
        assertEquals(Collections.emptyMap(), locks.get(authority), "locks left once the claims have finished");
    }

    //Whether the thread waits for a lock, as a claim waits for the one of its partition and namespace: blocked on a
    //monitor, or parked on a lock of java.util.concurrent, either of which the thread reports as the lock it waits for
    private static boolean waitsForALock(Thread thread) {
        if (thread == null) {
            return false;
        }
        final Thread.State state = thread.getState();
        if (state != Thread.State.BLOCKED && state != Thread.State.WAITING && state != Thread.State.TIMED_WAITING) {
            return false;
        }
        final ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(thread.getId());
        return info != null && info.getLockInfo() != null;
    }

    /**
     * The ids store, which watches the writes of claims: how many were in flight at once. When a barrier is set, it makes
     * each wait for the other's, and while claim writes are held, it holds each open until they are released.
     */
    private static class ClaimObservingStore extends KCVSProxy {

        volatile CyclicBarrier claimWritesMeet;
        //The latch which the claim writes from now on wait for, and all those which claim writes have waited for
        private volatile CountDownLatch release;
        private final List<CountDownLatch> holds = new CopyOnWriteArrayList<>();
        //A permit for each claim write which is held
        final Semaphore heldWrites = new Semaphore(0);
        final AtomicInteger claimWritesInFlight = new AtomicInteger();
        final AtomicInteger mostClaimWritesAtOnce = new AtomicInteger();

        ClaimObservingStore(KeyColumnValueStore store) {
            super(store);
        }

        //Holds the claim writes from now on until the returned latch is counted down
        CountDownLatch holdClaimWrites() {
            final CountDownLatch hold = new CountDownLatch(1);
            holds.add(hold);
            release = hold;
            return hold;
        }

        void releaseHeldClaimWrites() {
            holds.forEach(CountDownLatch::countDown);
        }

        @Override
        public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh)
                throws BackendException {
            if (additions.isEmpty()) { //the rollback of a claim deletes it
                super.mutate(key, additions, deletions, txh);
                return;
            }
            final int inFlight = claimWritesInFlight.incrementAndGet();
            try {
                mostClaimWritesAtOnce.accumulateAndGet(inFlight, Math::max);
                final CyclicBarrier meet = claimWritesMeet;
                final CountDownLatch hold = release;
                try {
                    if (meet != null) {
                        meet.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    }
                    if (hold != null) {
                        heldWrites.release();
                        if (!hold.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                            throw new PermanentBackendException("The held claim write wasn't released");
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new PermanentBackendException("Interrupted while a claim write waited", e);
                } catch (BrokenBarrierException | TimeoutException e) {
                    throw new PermanentBackendException("The other claim's write didn't come", e);
                }
                super.mutate(key, additions, deletions, txh);
            } finally {
                claimWritesInFlight.decrementAndGet();
            }
        }
    }
}
