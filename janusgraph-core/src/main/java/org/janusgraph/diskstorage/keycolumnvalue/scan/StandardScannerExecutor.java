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

package org.janusgraph.diskstorage.keycolumnvalue.scan;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractFuture;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SplittableScanStore;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.util.system.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
class StandardScannerExecutor extends AbstractFuture<ScanMetrics> implements ScanJobFuture, Runnable {

    private static final Logger log =
            LoggerFactory.getLogger(StandardScannerExecutor.class);

    private static final int TIMEOUT_MS = 180000; // 3 minutes

    /**
     * Sentinel row enqueued once per processor after the row collector finishes. A processor that
     * takes it knows no more rows are coming and exits. This lets processors block on
     * {@link BlockingQueue#take()} (waking the instant a row is enqueued) instead of polling on a
     * timer - the timed poll previously added latency per scanned row on fast backends.
     */
    private static final Row POISON_PILL = new Row(null, null);

    private final ScanJob job;
    private final Consumer<ScanMetrics> finishJob;
    private final StoreFeatures storeFeatures;
    private final StoreTransaction storeTx;
    private final KeyColumnValueStore store;
    private final int numProcessors;
    private final int workBlockSize;
    private final Configuration jobConfiguration;
    private final Configuration graphConfiguration;
    private final ScanMetrics metrics;

    /** How often the running scan logs a one-line progress summary. */
    private static final long PROGRESS_LOG_PERIOD_MS = 30_000;

    private boolean collectorCleanedUp = false;
    private boolean storeTxRolledBack = false;
    /**
     * Written by the cancelling thread ({@link #interruptTask()}), read by the scan thread; volatile
     * together with {@link #rowsCollector} so that - whichever way the cancel/setup race goes - either
     * the canceller observes the collector and interrupts it, or the scan thread observes the flag.
     */
    private volatile boolean interrupted = false;

    /** Written by the scan thread during setup, read by the cancelling thread; see {@link #interrupted}. */
    private volatile RowsCollector rowsCollector;
    private volatile BlockingQueue<Row> processorQueue;
    private volatile int processorQueueCapacity;

    StandardScannerExecutor(final ScanJob job, final Consumer<ScanMetrics> finishJob,
                            final KeyColumnValueStore store, final StoreTransaction storeTx,
                            final StoreFeatures storeFeatures,
                            final int numProcessors, final int workBlockSize,
                            final Configuration jobConfiguration,
                            final Configuration graphConfiguration) {
        this.job = job;
        this.finishJob = finishJob;
        this.store = store;
        this.storeTx = storeTx;
        this.storeFeatures = storeFeatures;
        this.numProcessors = numProcessors;
        this.workBlockSize = workBlockSize;
        this.jobConfiguration = jobConfiguration;
        this.graphConfiguration = graphConfiguration;

        metrics = new StandardScanMetrics();
    }


    @Override
    public void run() {

        final long startTime = System.currentTimeMillis();

        try {
            job.workerIterationStart(jobConfiguration, graphConfiguration, metrics);

            List<SliceQuery> queries = job.getQueries();
            int numQueries = queries.size();

            Preconditions.checkArgument(numQueries > 0,"Must at least specify one query for job: %s",job);

            processorQueueCapacity =
                this.graphConfiguration.get(GraphDatabaseConfiguration.PAGE_SIZE) * numProcessors * numQueries;
            processorQueue = new LinkedBlockingQueue<>(processorQueueCapacity);

            if (numQueries > 1) {
                //It is assumed that the first query is the grounding query if multiple queries exist
                SliceQuery ground = queries.get(0);
                StaticBuffer start = ground.getSliceStart();
                Preconditions.checkArgument(start.equals(BufferUtil.zeroBuffer(1)),
                        "Expected start of first query to be a single 0s: %s",start);
                StaticBuffer end = ground.getSliceEnd();
                Preconditions.checkArgument(end.equals(BufferUtil.oneBuffer(end.length())),
                        "Expected end of first query to be all 1s: %s",end);
            }

            rowsCollector = buildScanner(processorQueue, queries);

            log.info("Scan job started: job={}, queries={}, processors={}, workBlockSize={}, collector={}, rowQueueCapacity={}",
                job.getClass().getSimpleName(), numQueries, numProcessors, workBlockSize,
                rowsCollector.getClass().getSimpleName(), processorQueueCapacity);

        }  catch (Throwable e) {
            log.error("Exception trying to setup the job:", e);
            cleanupSilent();
            endJobIterationSilently();
            setException(e);
            return;
        }

        if (interrupted) {
            // The scan was cancelled before the collector existed, so interruptTask() had nothing to
            // interrupt: stop here instead of running a whole scan whose result would be discarded.
            // (Both `interrupted` and `rowsCollector` are volatile, so a cancel concurrent with the
            // setup above either sees the collector and interrupts it, or is seen by this check.)
            cleanupSilent();
            endJobIterationSilently();
            setException(new InterruptedException("Scanner got interrupted"));
            return;
        }

        // Everything below runs under the try/finally: a failure starting a processor or the progress
        // logger (e.g. OutOfMemoryError creating a native thread) must still complete the future and
        // tear the pipeline down, or callers block forever on get().
        Processor[] processors = new Processor[numProcessors];
        Thread progressLogger = null;

        try {
            for (int i=0;i<processors.length;i++) {
                processors[i]= new Processor(job.clone(),processorQueue);
                processors[i].start();
            }

            progressLogger = startProgressLogger(startTime);

            rowsCollector.run();

            rowsCollector.join();

            // The collector has produced every row; enqueue one poison pill per processor so each
            // blocked take() wakes and the processor exits once it has drained the remaining rows.
            // Use a bounded offer rather than a blocking put: a live processor keeps draining the queue,
            // so in the normal case there is room and offer returns immediately. This runs once per scan
            // at teardown, never on the per-row path, so it cannot reintroduce hand-off latency. The
            // timeout only elapses if every processor has already died (e.g. all threw in the work-block
            // rollover) and left the queue full - a blocking put would then hang forever; instead we give
            // up and let the finally block force-terminate any stragglers.
            for (int i = 0; i < numProcessors; i++) {
                if (!processorQueue.offer(POISON_PILL, TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    log.error("Timed out enqueuing scan termination signal; processor(s) already terminated");
                    break;
                }
            }
            if (!Threads.waitForCompletion(processors,TIMEOUT_MS)) log.error("Processor did not terminate in time");

            cleanup();
            try {
                job.workerIterationEnd(metrics);
            } catch (IllegalArgumentException e) {
                // https://github.com/JanusGraph/janusgraph/pull/891
                log.warn("Exception occurred processing worker iteration end. See PR 891.", e);
            }

            if (interrupted) {
                setException(new InterruptedException("Scanner got interrupted"));
            } else {
                logCompletion(startTime);
                finishJob.accept(metrics);
                set(metrics);
            }
        } catch (Throwable e) {
            if (interrupted) {
                // Cancellation routinely unblocks a waiting call with an InterruptedException; that is
                // teardown noise, not a job failure.
                log.debug("Exception occurred during cancelled job teardown:", e);
            } else {
                log.error("Exception occurred during job execution:", e);
            }
            endJobIterationSilently();
            setException(e);
        } finally {
            if (progressLogger != null) {
                progressLogger.interrupt();
            }
            Threads.terminate(processors);
            cleanupSilent();
        }
    }

    /**
     * Periodically logs pipeline state so a long scan (e.g. a reindex of a large graph) shows WHERE
     * time is spent: {@code produced} is the storage-side (producer) row count, {@code processed} the
     * worker-side count, and the queue fill discriminates the bottleneck - a near-empty queue means
     * the scan is storage-bound (producers can't keep up), a near-full queue means it is
     * processing/index-bound (workers can't keep up).
     */
    private Thread startProgressLogger(final long startTime) {
        final Thread progressLogger = new Thread(() -> {
            long lastProduced = 0;
            long lastProcessed = 0;
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(PROGRESS_LOG_PERIOD_MS);
                } catch (InterruptedException e) {
                    return;
                }
                final long produced = rowsCollector.getProducedCount();
                final long processed = metrics.get(ScanMetrics.Metric.SUCCESS) + metrics.get(ScanMetrics.Metric.FAILURE);
                final double windowSeconds = PROGRESS_LOG_PERIOD_MS / 1000d;
                log.info("Scan progress: produced={} rows ({}/s), processed={} rows ({}/s, failures={}), rowQueue={}/{}, elapsed={}s",
                    produced, Math.round((produced - lastProduced) / windowSeconds),
                    processed, Math.round((processed - lastProcessed) / windowSeconds),
                    metrics.get(ScanMetrics.Metric.FAILURE),
                    processorQueue.size(), processorQueueCapacity,
                    (System.currentTimeMillis() - startTime) / 1000);
                if (log.isDebugEnabled()) {
                    log.debug("Scan producers: {}", rowsCollector.getPullersProgress());
                }
                lastProduced = produced;
                lastProcessed = processed;
            }
        }, "scan-progress-" + job.getClass().getSimpleName() + "-" + Integer.toHexString(System.identityHashCode(this)));
        progressLogger.setDaemon(true);
        progressLogger.start();
        return progressLogger;
    }

    private void logCompletion(final long startTime) {
        final long elapsedMs = Math.max(1, System.currentTimeMillis() - startTime);
        final long success = metrics.get(ScanMetrics.Metric.SUCCESS);
        final long failure = metrics.get(ScanMetrics.Metric.FAILURE);
        log.info("Scan job finished: job={}, processedRows={} (failures={}), elapsed={}s, avgRate={} rows/s",
            job.getClass().getSimpleName(), success + failure, failure,
            elapsedMs / 1000, Math.round((success + failure) * 1000d / elapsedMs));
    }

    private RowsCollector buildScanner(BlockingQueue<Row> processorQueue, List<SliceQuery> queries) throws BackendException {
        if(!storeFeatures.hasConsistentScan()) {
            return new SingleThreadRowsCollector(store, storeTx, queries,
                job.getKeyFilter(), processorQueue);
        }
        final int splitCount = unorderedScanSplitCount();
        if (splitCount > 1) {
            return new PartitionedRowsCollector(store, storeFeatures, storeTx, queries,
                job.getKeyFilter(), processorQueue, graphConfiguration, (SplittableScanStore) store, splitCount);
        }
        return new MultiThreadsRowsCollector(store, storeFeatures, storeTx, queries,
            job.getKeyFilter(), job.getKeysToScan(), processorQueue, graphConfiguration);
    }

    /**
     * Split-parallel collection applies only to whole-key-space scans: a targeted scan
     * ({@code getKeysToScan() != null}) already fetches an explicit key list and gains nothing
     * from key-space partitioning.
     */
    private int unorderedScanSplitCount() {
        if (job.getKeysToScan() != null || !(store instanceof SplittableScanStore)) {
            return 1;
        }
        return Math.max(1, ((SplittableScanStore) store).getUnorderedScanSplitCount());
    }

    @Override
    protected void interruptTask() {
        interrupted = true;
        // Cancellation can arrive before run() has built the collector; the interrupted flag alone
        // is enough then - run() completes the future as interrupted once it gets going.
        final RowsCollector collector = rowsCollector;
        if (collector != null) {
            collector.interrupt();
        }
    }

    /**
     * Idempotent teardown, called once on the regular completion path and once more from the
     * final cleanup-on-failure path. Collector cleanup and the transaction rollback are tracked
     * separately: the rollback must run even when the collector cleanup throws, a rollback
     * failure must not mask the collector failure (it is attached as suppressed), and a failed
     * rollback stays retryable by the later cleanup attempt instead of being skipped forever.
     */
    private void cleanup() throws BackendException {
        Throwable collectorFailure = null;
        try {
            if (!collectorCleanedUp) {
                if(rowsCollector != null){
                    rowsCollector.cleanup();
                }
                // Marked only on success so a failed cleanup (e.g. an iterator close error) stays
                // retryable by the later cleanup attempt, exactly like the rollback below.
                collectorCleanedUp = true;
            }
        } catch (Throwable t) {
            collectorFailure = t;
            throw t;
        } finally {
            if (!storeTxRolledBack) {
                try {
                    storeTx.rollback();
                    storeTxRolledBack = true;
                } catch (BackendException | RuntimeException | Error rollbackFailure) {
                    if (collectorFailure == null) {
                        throw rollbackFailure;
                    }
                    if (rollbackFailure instanceof Error) {
                        // A JVM-level Error outranks the collector failure - propagate it as primary
                        // and keep the collector failure visible as suppressed.
                        rollbackFailure.addSuppressed(collectorFailure);
                        throw rollbackFailure;
                    }
                    // Keep the collector failure primary; losing it to a rollback failure would hide
                    // the root cause of the cleanup problem.
                    collectorFailure.addSuppressed(rollbackFailure);
                }
            }
        }
    }

    /**
     * Truly silent variant of {@link #cleanup()} for teardown paths: cleanup() can also propagate
     * RuntimeException/Error (e.g. from the transaction rollback), and letting anything escape here
     * would skip the subsequent future completion - callers of get() would block forever - or kill
     * the scan thread from a finally block.
     */
    private void cleanupSilent() {
        try {
            cleanup();
        } catch (Throwable ex) {
            log.error("Encountered exception when trying to clean up after failure",ex);
        }
    }

    /**
     * {@code workerIterationEnd} runs user job code; on teardown paths a failure in it must neither
     * prevent the future from completing (a throw before {@code setException} would leave callers of
     * {@code get()} blocked forever) nor mask the primary failure being reported.
     */
    private void endJobIterationSilently() {
        try {
            job.workerIterationEnd(metrics);
        } catch (Throwable e) {
            log.warn("Exception occurred while ending the job iteration during scan teardown", e);
        }
    }

    @Override
    public ScanMetrics getIntermediateResult() {
        return metrics;
    }

    static class Row {

        final StaticBuffer key;
        final Map<SliceQuery,EntryList> entries;

        Row(StaticBuffer key, Map<SliceQuery, EntryList> entries) {
            this.key = key;
            this.entries = entries;
        }
    }



    private class Processor extends Thread {

        private ScanJob job;
        private final BlockingQueue<Row> processorQueue;

        private int numProcessed;


        private Processor(ScanJob job, BlockingQueue<Row> processorQueue) {
            this.job = job;
            this.processorQueue = processorQueue;

            this.numProcessed = 0;
        }

        @Override
        public void run() {
            try {
                job.workerIterationStart(jobConfiguration, graphConfiguration, metrics);
                while (true) {
                    Row row = processorQueue.take(); //Blocks until a row (or the POISON_PILL) is available
                    if (row == POISON_PILL) break; //Collector finished: no more rows will be produced
                    if (numProcessed>=workBlockSize) {
                        //Setup new chunk of work
                        job.workerIterationEnd(metrics);
                        job = job.clone();
                        job.workerIterationStart(jobConfiguration, graphConfiguration, metrics);
                        numProcessed=0;
                    }
                    try {
                        job.process(row.key,row.entries,metrics);
                        metrics.increment(ScanMetrics.Metric.SUCCESS);
                    } catch (Throwable ex) {
                        log.error("Exception processing row ["+row.key+"]: ",ex);
                        metrics.increment(ScanMetrics.Metric.FAILURE);
                    }
                    numProcessed++;
                }
            } catch (InterruptedException e) {
                log.error("Processing thread interrupted while waiting on queue or processing data", e);
            } catch (Throwable e) {
                log.error("Unexpected error processing data",e);
            } finally {
                job.workerIterationEnd(metrics);
            }
        }
    }




}



