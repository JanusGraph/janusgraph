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

package org.janusgraph.diskstorage.keycolumnvalue.scan;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SplittableScanStore;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.janusgraph.diskstorage.keycolumnvalue.scan.StandardScannerExecutor.Row;

/**
 * Runs one {@link MultiThreadsRowsCollector} per disjoint key-space split of a
 * {@link SplittableScanStore}, each on its own merge thread, all feeding the same processing queue.
 * <p>
 * This removes the single-pipeline ceiling of a whole-table scan: instead of one set of per-query
 * data pullers bounded by one paged backend scan, {@code splitCount} independent pipelines stream
 * disjoint parts of the key space concurrently. Correctness relies on the {@link SplittableScanStore}
 * contract - splits tile the key space exactly (no gaps/overlaps), and within one split every query
 * iterates keys in the same order, which is all the per-split merge needs. Scan jobs never depend on
 * the global order in which rows of different keys are processed (they are handed to a pool of
 * processor threads anyway), so interleaving rows of different splits is safe.
 *
 * @author Oleksandr Porunov (alexandr.porunov@gmail.com)
 */
class PartitionedRowsCollector extends RowsCollector {

    private static final Logger log = LoggerFactory.getLogger(PartitionedRowsCollector.class);

    private final MultiThreadsRowsCollector[] units;
    private final Thread[] unitThreads;
    private final AtomicReference<Throwable> unitFailure = new AtomicReference<>();
    private volatile boolean interrupted = false;
    /**
     * The thread executing {@link #run()}; interrupted by {@link #interrupt()} so a cancellation
     * also unblocks the executor thread wherever it currently waits (joining the split threads here,
     * or already past run() in the scan teardown phase) - the same contract the single-collector
     * case gets from {@code MultiThreadsRowsCollector.collectorThread}.
     */
    private volatile Thread runnerThread;

    PartitionedRowsCollector(
        KeyColumnValueStore store,
        StoreFeatures storeFeatures,
        StoreTransaction storeTx,
        List<SliceQuery> queries,
        Predicate<StaticBuffer> keyFilter,
        BlockingQueue<Row> rowQueue,
        Configuration graphConfiguration,
        SplittableScanStore splittableStore,
        int splitCount) throws BackendException {

        super(store, rowQueue);
        this.units = new MultiThreadsRowsCollector[splitCount];
        this.unitThreads = new Thread[splitCount];
        try {
            for (int i = 0; i < splitCount; i++) {
                final int splitIndex = i;
                units[i] = new MultiThreadsRowsCollector(store, storeFeatures, storeTx, queries, keyFilter,
                    null, rowQueue, graphConfiguration,
                    query -> splittableStore.getKeysForSplit(query, storeTx, splitIndex, splitCount),
                    "-split" + splitIndex);
            }
        } catch (Throwable e) {
            // Unwind pullers of the splits that were already created before propagating; a cleanup
            // failure must neither mask the original exception nor skip the remaining units.
            for (MultiThreadsRowsCollector unit : units) {
                if (unit == null) continue;
                try {
                    unit.cleanup();
                } catch (Throwable cleanupFailure) {
                    log.warn("Could not clean up a scan split while unwinding a failed scan setup", cleanupFailure);
                }
            }
            throw e;
        }
    }

    @Override
    void run() throws InterruptedException, TemporaryBackendException {
        runnerThread = Thread.currentThread();
        try {
            for (int i = 0; i < units.length; i++) {
                final MultiThreadsRowsCollector unit = units[i];
                final int unitIndex = i;
                final Thread thread = new Thread(() -> {
                    try {
                        unit.run();
                        if (log.isDebugEnabled()) {
                            log.debug("Scan split {} finished: produced={} rows [{}]",
                                unitIndex, unit.getProducedCount(), unit.getPullersProgress());
                        }
                    } catch (Throwable t) {
                        // First failure wins; stop the sibling splits so the scan fails fast instead of
                        // spending hours streaming data that will be thrown away.
                        if (unitFailure.compareAndSet(null, t)) {
                            if (interrupted || t instanceof InterruptedException) {
                                // Cancellation unblocked this split mid-teardown; not a storage failure.
                                log.debug("Scan split {} exited on cancellation", unitIndex, t);
                            } else {
                                log.error("Scan split failed; interrupting the remaining splits", t);
                            }
                            interruptUnits();
                        } else if (!(t instanceof InterruptedException)) {
                            // Losing racers are still real failures; keep their root cause diagnosable.
                            log.warn("Additional scan split failure after the first reported one", t);
                        }
                    }
                }, "scan-split-collector-" + i);
                unitThreads[i] = thread;
                thread.start();
            }
            for (Thread thread : unitThreads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            // Scan cancellation: mirror the single-collector behavior (complete as interrupted) and
            // make sure every split's merge thread is woken, or they would block forever on queues
            // nobody drains once the scan is torn down.
            interrupted = true;
            interruptUnits();
        } catch (Throwable e) {
            // E.g. OutOfMemoryError starting a split thread: without the interrupts the merge threads
            // that DID start would leak, blocked on their data queues, once the pullers are cleaned up.
            interruptUnits();
            throw e;
        }
        final Throwable failure = unitFailure.get();
        if (failure != null && !interrupted) {
            if (failure instanceof TemporaryBackendException) throw (TemporaryBackendException) failure;
            if (failure instanceof InterruptedException) throw (InterruptedException) failure;
            throw new TemporaryBackendException("Scan split failed", failure);
        }
    }

    @Override
    long getProducedCount() {
        long total = 0;
        for (MultiThreadsRowsCollector unit : units) {
            total += unit.getProducedCount();
        }
        return total;
    }

    @Override
    String getPullersProgress() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < units.length; i++) {
            if (i > 0) sb.append("; ");
            sb.append(units[i].getPullersProgress());
        }
        return sb.toString();
    }

    @Override
    void join() throws InterruptedException {
        // Wait (bounded) for the split merge threads first so no straggler keeps feeding the row
        // queue after this returns; entries may be null when run() failed before starting them all.
        for (Thread thread : unitThreads) {
            if (thread != null) {
                thread.join(10_000);
            }
        }
        for (MultiThreadsRowsCollector unit : units) {
            unit.join();
        }
    }

    @Override
    void interrupt() {
        interrupted = true;
        interruptUnits();
        final Thread t = runnerThread;
        if (t != null) {
            t.interrupt();
        }
    }

    private void interruptUnits() {
        for (MultiThreadsRowsCollector unit : units) {
            unit.interrupt();
        }
    }

    @Override
    void cleanup() throws PermanentBackendException {
        for (MultiThreadsRowsCollector unit : units) {
            unit.cleanup();
        }
    }
}
