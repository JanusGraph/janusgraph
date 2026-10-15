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

package org.janusgraph.graphdb.idmanagement;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.IDAuthority;
import org.janusgraph.diskstorage.IDBlock;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.graphdb.database.idassigner.IDBlockSizer;
import org.janusgraph.graphdb.database.idassigner.IDPoolExhaustedException;
import org.janusgraph.graphdb.database.idassigner.StandardIDPool;
import org.janusgraph.graphdb.util.IntHashSet;
import org.janusgraph.graphdb.util.IntSet;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.easymock.EasyMock.expect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IDPoolTest {



    @Test
    public void testStandardIDPool1() throws InterruptedException {

        final MockIDAuthority idAuthority = new MockIDAuthority(200);
        testIDPoolWith(partitionID -> new StandardIDPool(idAuthority, partitionID, partitionID, Integer.MAX_VALUE, Duration.ofMillis(2000L), 0.2), 1000, 6, 100000);
    }

    @Test
    public void testStandardIDPool2() throws InterruptedException {
        final MockIDAuthority idAuthority = new MockIDAuthority(10000, Integer.MAX_VALUE, 2000);
        testIDPoolWith(partitionID -> new StandardIDPool(idAuthority, partitionID, partitionID, Integer.MAX_VALUE, Duration.ofMillis(4000), 0.1), 2, 5, 10000);
    }

    @Test
    public void testStandardIDPool3() throws InterruptedException {
        final MockIDAuthority idAuthority = new MockIDAuthority(200);
        testIDPoolWith(partitionID -> new StandardIDPool(idAuthority, partitionID, partitionID, Integer.MAX_VALUE, Duration.ofMillis(2000), 0.2), 10, 20, 100000);
    }

    private void testIDPoolWith(IDPoolFactory poolFactory, final int numPartitions,
                                       final int numThreads, final int attemptsPerThread) throws InterruptedException {
        final Random random = new Random();
        final IntSet[] ids = new IntSet[numPartitions];
        final StandardIDPool[] idPools = new StandardIDPool[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            ids[i] = new IntHashSet(attemptsPerThread * numThreads / numPartitions);
            int partition = i*100;
            idPools[i] = poolFactory.get(partition);
        }

        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {

            threads[i] = new Thread(() -> {
                for (int attempt = 0; attempt < attemptsPerThread; attempt++) {
                    int offset = random.nextInt(numPartitions);
                    long id = idPools[offset].nextID();
                    assertTrue(id < Integer.MAX_VALUE);
                    IntSet idSet = ids[offset];
                    synchronized (idSet) {
                        assertFalse(idSet.contains((int) id));
                        idSet.add((int) id);
                    }
                }
            });
            threads[i].start();
        }
        for (int i = 0; i < numThreads; i++) threads[i].join();
        for (final StandardIDPool idPool : idPools) idPool.close();
        //Verify consecutive id assignment
        for (int i = 0; i < ids.length; i++) {
            IntSet set = ids[i];
            int max = 0;
            int[] all = set.getAll();
            for (int id : all) if (id > max) max = id;
            for (int j=1;j<=max;j++) assertTrue(set.contains(j), i+ " contains: " + j);
        }
    }

    @Test
    public void testAllocationTimeout() {
        final MockIDAuthority idAuthority = new MockIDAuthority(10000, Integer.MAX_VALUE, 5000);
        StandardIDPool pool = new StandardIDPool(idAuthority, 1, 1, Integer.MAX_VALUE, Duration.ofMillis(4000), 0.1);

        assertThrows(JanusGraphException.class, pool::nextID);
    }

    @Test
    public void testAllocationTimeoutAndRecovery() throws BackendException {
        IMocksControl ctrl = EasyMock.createStrictControl();

        final int partition = 42;
        final int idNamespace = 777;
        final Duration timeout = Duration.ofSeconds(1L);

        final IDAuthority mockAuthority = ctrl.createMock(IDAuthority.class);

        // Sleep for two seconds, then throw a BackendException
        // this whole delegate could be deleted if we abstracted StandardIDPool's internal executor and stopwatches
        expect(mockAuthority.getIDBlock(partition, idNamespace, timeout)).andDelegateTo(new IDAuthority() {
            @Override
            public IDBlock getIDBlock(int partition, int idNamespace, Duration timeout) throws BackendException {
                assertThrows(InterruptedException.class, () -> Thread.sleep(2000L));
                throw new TemporaryBackendException("slow backend");
            }

            @Override
            public List<KeyRange> getLocalIDPartition() {
                throw new IllegalArgumentException();
            }

            @Override
            public void setIDBlockSizer(IDBlockSizer sizer) {
                throw new IllegalArgumentException();
            }

            @Override
            public void close() {
                throw new IllegalArgumentException();
            }

            @Override
            public String getUniqueID() {
                throw new IllegalArgumentException();
            }

            @Override
            public boolean supportsInterruption()
            {
                return true;
            }
        });
        // A block of more ids than StandardIDPool keeps in reserve, the larger of its RENEW_ID_COUNT and the renew-buffer
        // percentage of the block: handing out the block's first id then does not start fetching the next block in the
        // background, which the strict mock would count as an unexpected third call whenever that fetch got in before
        // verify()
        expect(mockAuthority.getIDBlock(partition, idNamespace, timeout)).andReturn(new IDBlock() {
            @Override
            public long numIds() {
                return 1000;
            }

            @Override
            public long getId(long index) {
                return 200 + index;
            }
        });
        expect(mockAuthority.supportsInterruption()).andStubReturn(true);

        ctrl.replay();
        StandardIDPool pool = new StandardIDPool(mockAuthority, partition, idNamespace, Integer.MAX_VALUE, timeout, 0.1);

        assertThrows(JanusGraphException.class, pool::nextID);

        long nextID = pool.nextID();
        assertEquals(200, nextID);

        ctrl.verify();
    }

    @Test
    public void testPoolExhaustion1() {
        MockIDAuthority idAuthority = new MockIDAuthority(200);
        int idUpper = 10000;
        StandardIDPool pool = new StandardIDPool(idAuthority, 0, 1, idUpper, Duration.ofMillis(2000), 0.2);
        for (int i = 1; i < idUpper * 2; i++) {
            try {
                long id = pool.nextID();
                assertTrue(id < idUpper);
            } catch (IDPoolExhaustedException e) {
                assertEquals(idUpper, i);
                break;
            }
        }
    }

    @Test
    public void testPoolExhaustion2() {
        int idUpper = 10000;
        MockIDAuthority idAuthority = new MockIDAuthority(200, idUpper);
        StandardIDPool pool = new StandardIDPool(idAuthority, 0, 1, Integer.MAX_VALUE, Duration.ofMillis(2000), 0.2);
        for (int i = 1; i < idUpper * 2; i++) {
            try {
                long id = pool.nextID();
                assertTrue(id < idUpper);
            } catch (IDPoolExhaustedException e) {
                assertEquals(idUpper, i);
                break;
            }
        }
    }

    interface IDPoolFactory {
        StandardIDPool get(int partitionID);
    }

    //An executor of the given number of threads, as a graph's VertexIDAssigner keeps for the renewals of its pools
    private static ThreadPoolExecutor sharedRenewalExecutor(int threads) {
        return new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    }

    //Whether the thread came to wait within the time, in the timed wait for a renewal here. The observation is what
    //counts: a thread in a timed wait is seen runnable for an instant whenever it wakes to check its time
    private static boolean cameToWait(Thread thread, long timeoutNanos) throws InterruptedException {
        final long deadline = System.nanoTime() + timeoutNanos;
        while (System.nanoTime() - deadline < 0) {
            final Thread.State state = thread.getState();
            if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
                return true;
            }
            Thread.sleep(10);
        }
        return false;
    }

    @Test
    public void testStandardIDPoolsSharingARenewalExecutor() throws InterruptedException {
        final MockIDAuthority idAuthority = new MockIDAuthority(200);
        final ThreadPoolExecutor renewals = sharedRenewalExecutor(2);
        try {
            testIDPoolWith(partitionID -> new StandardIDPool(idAuthority, partitionID, partitionID, Integer.MAX_VALUE,
                Duration.ofMillis(2000), 0.2, renewals), 10, 20, 100000);
            assertTrue(renewals.getCompletedTaskCount() > 0);
        } finally {
            renewals.shutdownNow();
        }
    }

    /**
     * Hands out blocks of ten ids. A call for partition 1 waits until the test lets it through, and the authority
     * records how many such calls run at once.
     */
    private static class GatedIDAuthority extends MockIDAuthority {
        private final Semaphore gate = new Semaphore(0);
        private final AtomicInteger running = new AtomicInteger();
        private final AtomicInteger mostRunning = new AtomicInteger();
        private final AtomicInteger calls = new AtomicInteger();

        GatedIDAuthority() {
            super(10);
        }

        @Override
        public IDBlock getIDBlock(int partition, int idNamespace, Duration timeout) throws BackendException {
            if (partition != 1) {
                return super.getIDBlock(partition, idNamespace, timeout);
            }
            calls.incrementAndGet();
            mostRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
            try {
                gate.acquireUninterruptibly();
                return super.getIDBlock(partition, idNamespace, timeout);
            } finally {
                running.decrementAndGet();
            }
        }

        @Override
        public boolean supportsInterruption() {
            return false;
        }
    }

    @Test
    public void aPoolOnASharedExecutorRenewsOneBlockAtATime() throws Exception {
        final GatedIDAuthority idAuthority = new GatedIDAuthority();
        final ThreadPoolExecutor renewals = sharedRenewalExecutor(4);
        try {
            //Long enough for the steps below to fit in the wait of the second request, also on a loaded machine
            final StandardIDPool pool = new StandardIDPool(idAuthority, 1, 1, Integer.MAX_VALUE,
                Duration.ofSeconds(5), 0.1, renewals);

            //The first renewal hangs, and the pool gives up waiting for it
            final JanusGraphException timedOut = assertThrows(JanusGraphException.class, pool::nextID);
            assertTrue(timedOut.getCause() instanceof TimeoutException, String.valueOf(timedOut.getCause()));

            //The next renewal waits for it, although the executor has idle threads, as on a thread of the pool's own
            final FutureTask<Long> next = new FutureTask<>(pool::nextID);
            final Thread requester = new Thread(next);
            requester.start();
            //Waiting for the renewal it started
            assertTrue(cameToWait(requester, TimeUnit.SECONDS.toNanos(10)), "the request did not wait for the renewal");
            Thread.sleep(100);
            assertEquals(1, idAuthority.calls.get(), "a second renewal of the pool started while the first ran");

            //Another pool's renewals are not held up
            final StandardIDPool other = new StandardIDPool(idAuthority, 2, 1, Integer.MAX_VALUE,
                Duration.ofSeconds(2), 0.1, renewals);
            assertEquals(1, other.nextID());
            other.close();

            //Lets through the hanging renewal, the next one, and those which follow it
            idAuthority.gate.release(100);
            assertEquals(11, next.get(10, TimeUnit.SECONDS), "the first id of the second block");
            pool.close();
            assertTrue(idAuthority.calls.get() >= 2);
            assertEquals(1, idAuthority.mostRunning.get(), "renewals of the pool ran at once");
        } finally {
            //A renewal held at the gate can't be interrupted: let it through before the executor is shut down
            idAuthority.gate.release(100);
            renewals.shutdownNow();
        }
    }

    @Test
    public void closingAPoolOnASharedExecutorWaitsForARenewalItGaveUpOn() throws Exception {
        final GatedIDAuthority idAuthority = new GatedIDAuthority();
        final ThreadPoolExecutor renewals = sharedRenewalExecutor(4);
        try {
            final StandardIDPool pool = new StandardIDPool(idAuthority, 1, 1, Integer.MAX_VALUE,
                Duration.ofMillis(300), 0.1, renewals);
            assertThrows(JanusGraphException.class, pool::nextID);

            final CompletableFuture<Void> closed = CompletableFuture.runAsync(pool::close);
            Thread.sleep(500);
            assertFalse(closed.isDone(), "the pool closed while its renewal ran");

            idAuthority.gate.release();
            closed.get(10, TimeUnit.SECONDS);
            assertEquals(0, idAuthority.running.get());
            assertFalse(renewals.isShutdown(), "the pool shut down the executor it shares");
        } finally {
            //A renewal held at the gate can't be interrupted: let it through before the executor is shut down
            idAuthority.gate.release(100);
            renewals.shutdownNow();
        }
    }

    @Test
    public void aRenewalCancelledWithAnInterruptLeavesTheNextOneOnTheSameThreadUninterrupted() {
        final AtomicInteger calls = new AtomicInteger();
        final AtomicBoolean nextInterrupted = new AtomicBoolean();
        //Blocks of 1,000 ids, more than a pool keeps in reserve, so that handing out the first id renews nothing
        final MockIDAuthority idAuthority = new MockIDAuthority(1000) {
            @Override
            public IDBlock getIDBlock(int partition, int idNamespace, Duration timeout) throws BackendException {
                if (calls.incrementAndGet() == 1) {
                    try {
                        Thread.sleep(TimeUnit.MINUTES.toMillis(1));
                    } catch (InterruptedException e) {
                        throw new TemporaryBackendException("interrupted", e);
                    }
                }
                nextInterrupted.set(Thread.currentThread().isInterrupted());
                return super.getIDBlock(partition, idNamespace, timeout);
            }
        };
        assertTrue(idAuthority.supportsInterruption());
        final ThreadPoolExecutor renewals = sharedRenewalExecutor(1);
        try {
            final StandardIDPool pool = new StandardIDPool(idAuthority, 1, 1, Integer.MAX_VALUE,
                Duration.ofMillis(500), 0.1, renewals);

            //The pool gives up on the first renewal, and cancels it with an interrupt
            assertThrows(JanusGraphException.class, pool::nextID);
            //The next renewal runs on the same thread
            assertEquals(1, pool.nextID());
            assertEquals(2, calls.get());
            assertFalse(nextInterrupted.get(), "the interrupt of the cancelled renewal reached the next one");
            pool.close();
        } finally {
            renewals.shutdownNow();
        }
    }

    @Test
    public void aFailedRenewalOnASharedExecutorSurfacesAsOnAThreadOfThePoolsOwn() throws BackendException {
        final IMocksControl ctrl = EasyMock.createNiceControl();
        final IDAuthority failingAuthority = ctrl.createMock(IDAuthority.class);
        expect(failingAuthority.getIDBlock(EasyMock.anyInt(), EasyMock.anyInt(), EasyMock.anyObject()))
            .andThrow(new TemporaryBackendException("storage unavailable")).anyTimes();
        ctrl.replay();
        final ThreadPoolExecutor renewals = sharedRenewalExecutor(1);
        try {
            final StandardIDPool pool = new StandardIDPool(failingAuthority, 1, 1, Integer.MAX_VALUE,
                Duration.ofMillis(2000), 0.1, renewals);

            final JanusGraphException failure = assertThrows(JanusGraphException.class, pool::nextID);
            assertTrue(failure.getCause() instanceof ExecutionException, String.valueOf(failure.getCause()));
            assertTrue(failure.getCause().getCause().getCause() instanceof TemporaryBackendException,
                String.valueOf(failure.getCause().getCause()));
            pool.close();
        } finally {
            renewals.shutdownNow();
        }
    }

    @Test
    public void aRenewalOnAShutDownExecutorFailsTheIdRequest() {
        final ThreadPoolExecutor renewals = sharedRenewalExecutor(1);
        renewals.shutdown();
        final StandardIDPool pool = new StandardIDPool(new MockIDAuthority(10), 1, 1, Integer.MAX_VALUE,
            Duration.ofMillis(2000), 0.1, renewals);
        try {
            final JanusGraphException failure = assertThrows(JanusGraphException.class, pool::nextID);
            assertTrue(failure.getCause() instanceof RejectedExecutionException, String.valueOf(failure.getCause()));
        } finally {
            pool.close();
        }
    }

}
