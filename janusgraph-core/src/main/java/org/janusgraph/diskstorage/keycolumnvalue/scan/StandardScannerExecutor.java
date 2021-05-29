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
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
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
class StandardScannerExecutor extends AbstractFuture<ScanMetrics> implements JanusGraphManagement.IndexJobFuture, Runnable {

    private static final Logger log =
            LoggerFactory.getLogger(StandardScannerExecutor.class);

    private static final int TIMEOUT_MS = 180000; // 60 seconds
    static final int TIME_PER_TRY = 10; // 10 milliseconds

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

    private boolean hasCompleted = false;
    private boolean interrupted = false;

    private RowsCollector rowsCollector;

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

        BlockingQueue<Row> processorQueue;

        try {
            job.workerIterationStart(jobConfiguration, graphConfiguration, metrics);

            List<SliceQuery> queries = job.getQueries();
            int numQueries = queries.size();

            processorQueue = new LinkedBlockingQueue<>(
                this.graphConfiguration.get(GraphDatabaseConfiguration.PAGE_SIZE) * numProcessors * numQueries);

            Preconditions.checkArgument(numQueries > 0,"Must at least specify one query for job: %s",job);

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

        }  catch (Throwable e) {
            log.error("Exception trying to setup the job:", e);
            cleanupSilent();
            job.workerIterationEnd(metrics);
            setException(e);
            return;
        }

        Processor[] processors = new Processor[numProcessors];
        for (int i=0;i<processors.length;i++) {
            processors[i]= new Processor(job.clone(),processorQueue);
            processors[i].start();
        }

        try {
            rowsCollector.run();

            rowsCollector.join();

            for (Processor processor : processors) {
                processor.finish();
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
                finishJob.accept(metrics);
                set(metrics);
            }
        } catch (Throwable e) {
            log.error("Exception occurred during job execution:", e);
            job.workerIterationEnd(metrics);
            setException(e);
        } finally {
            Threads.terminate(processors);
            cleanupSilent();
        }
    }

    private RowsCollector buildScanner(BlockingQueue<Row> processorQueue, List<SliceQuery> queries) throws BackendException {
        if(!storeFeatures.hasConsistentScan()) {
            return new SingleThreadRowsCollector(store, storeTx, queries,
                job.getKeyFilter(), processorQueue);
        } else {
            return new MultiThreadsRowsCollector(store, storeFeatures, storeTx, queries,
                job.getKeyFilter(), processorQueue, graphConfiguration);
        }
    }

    @Override
    protected void interruptTask() {
        interrupted = true;
        rowsCollector.interrupt();
    }

    private void cleanup() throws BackendException {
        if (!hasCompleted) {
            hasCompleted = true;
            if(rowsCollector != null){
                rowsCollector.cleanup();
            }
            storeTx.rollback();
        }
    }

    private void cleanupSilent() {
        try {
            cleanup();
        } catch (BackendException ex) {
            log.error("Encountered exception when trying to clean up after failure",ex);
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

        private volatile boolean finished;
        private int numProcessed;


        private Processor(ScanJob job, BlockingQueue<Row> processorQueue) {
            this.job = job;
            this.processorQueue = processorQueue;

            this.finished = false;
            this.numProcessed = 0;
        }

        @Override
        public void run() {
            try {
                job.workerIterationStart(jobConfiguration, graphConfiguration, metrics);
                while (!finished || !processorQueue.isEmpty()) {
                    Row row;
                    while ((row=processorQueue.poll(TIME_PER_TRY,TimeUnit.MILLISECONDS))!=null) {
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
                }
            } catch (InterruptedException e) {
                log.error("Processing thread interrupted while waiting on queue or processing data", e);
            } catch (Throwable e) {
                log.error("Unexpected error processing data",e);
            } finally {
                job.workerIterationEnd(metrics);
            }
        }

        public void finish() {
            this.finished=true;
        }
    }




}



