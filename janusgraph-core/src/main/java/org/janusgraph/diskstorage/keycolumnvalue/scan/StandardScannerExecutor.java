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
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.janusgraph.util.system.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
class StandardScannerExecutor extends AbstractFuture<ScanMetrics> implements JanusGraphManagement.IndexJobFuture, Runnable {

    private static final Logger log =
            LoggerFactory.getLogger(StandardScannerExecutor.class);

    private static final int QUEUE_SIZE = 1000;
    private static final int TIMEOUT_MS = 180000; // 60 seconds
    private static final int MAX_KEY_LENGTH = 128; //in bytes

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

    private List<SliceQuery> queries;
    private int numQueries;
    private List<BlockingQueue<SliceResult>> dataQueues;
    private DataPuller[] pullThreads;

    StandardScannerExecutor(final ScanJob job, final Consumer<ScanMetrics> finishJob,
                            final KeyColumnValueStore store, final StoreTransaction storeTx,
                            final StoreFeatures storeFeatures,
                            final int numProcessors, final int workBlockSize,
                            final Configuration jobConfiguration,
                            final Configuration graphConfiguration) throws BackendException {
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

    private final DataPuller addDataPuller(SliceQuery sq, StoreTransaction stx) throws BackendException {
        BlockingQueue<SliceResult> queue = new LinkedBlockingQueue<SliceResult>(QUEUE_SIZE);
        dataQueues.add(queue);

        DataPuller dp = new DataPuller(sq, queue,
                KCVSUtil.getKeys(store,sq,storeFeatures,MAX_KEY_LENGTH,stx),job.getKeyFilter());
        dp.start();
        return dp;
    }

    @Override
    public void run() {
        try {
            job.workerIterationStart(jobConfiguration, graphConfiguration, metrics);

            queries = job.getQueries();
            numQueries = queries.size();
            Preconditions.checkArgument(numQueries>0,"Must at least specify one query for job: %s",job);
            if (numQueries>1) {
                //It is assumed that the first query is the grounding query if multiple queries exist
                SliceQuery ground = queries.get(0);
                StaticBuffer start = ground.getSliceStart();
                Preconditions.checkArgument(start.equals(BufferUtil.zeroBuffer(1)),
                        "Expected start of first query to be a single 0s: %s",start);
                StaticBuffer end = ground.getSliceEnd();
                Preconditions.checkArgument(end.equals(BufferUtil.oneBuffer(end.length())),
                        "Expected end of first query to be all 1s: %s",end);
            }
            dataQueues = new ArrayList<BlockingQueue<SliceResult>>(numQueries);
            pullThreads = new DataPuller[numQueries];

            for (int pos=0;pos<numQueries;pos++) {
                pullThreads[pos]=addDataPuller(queries.get(pos),storeTx);
            }
        }  catch (Throwable e) {
            log.error("Exception trying to setup the job:", e);
            cleanupSilent();
            job.workerIterationEnd(metrics);
            setException(e);
            return;
        }

        BlockingQueue<Row> processorQueue = new LinkedBlockingQueue<>(QUEUE_SIZE);

        Processor[] processors = new Processor[numProcessors];
        for (int i=0;i<processors.length;i++) {
            processors[i]= new Processor(job.clone(),processorQueue);
            processors[i].start();
        }

        try {
            SliceResult[] currentResults = new SliceResult[numQueries];
            while (!interrupted) {
                for (int i = 0; i < numQueries; i++) {
                    if (currentResults[i]!=null) continue;
                    BlockingQueue<SliceResult> queue = dataQueues.get(i);

                    SliceResult qr = queue.poll(10,TimeUnit.MILLISECONDS); //Try very short time to see if we are done
                    if (qr==null) {
                        if (pullThreads[i].isFinished()) continue; //No more data to be expected
                        qr = queue.poll(TIMEOUT_MS,TimeUnit.MILLISECONDS); //otherwise, give it more time
                        if (qr==null && !pullThreads[i].isFinished())
                            throw new TemporaryBackendException("Timed out waiting for next row data - storage error likely");
                    }
                    currentResults[i]=qr;
                }
                SliceResult conditionQuery = currentResults[0];
                if (conditionQuery==null) break; //Termination condition - primary query has no more data
                final StaticBuffer key = conditionQuery.key;

                Map<SliceQuery,EntryList> queryResults = new HashMap<>(numQueries);
                for (int i=0;i<currentResults.length;i++) {
                    SliceQuery query = queries.get(i);
                    EntryList entries = EntryList.EMPTY_LIST;
                    if (currentResults[i]!=null && currentResults[i].key.equals(key)) {
                        assert query.equals(currentResults[i].query);
                        entries = currentResults[i].entries;
                        currentResults[i]=null;
                    }
                    queryResults.put(query,entries);
                }
                processorQueue.put(new Row(key, queryResults));
            }

            for (int i = 0; i < pullThreads.length; i++) {
                pullThreads[i].join(10);
                if (pullThreads[i].isAlive()) {
                    log.warn("Data pulling thread [{}] did not terminate. Forcing termination",i);
                    pullThreads[i].interrupt();
                }
            }

            for (int i=0; i<processors.length;i++) {
                processors[i].finish();
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
            log.error("Exception occured during job execution: {}",e);
            job.workerIterationEnd(metrics);
            setException(e);
        } finally {
            Threads.terminate(processors);
            cleanupSilent();
        }
    }

    @Override
    protected void interruptTask() {
        interrupted = true;
    }

    private void cleanup() throws BackendException {
        if (!hasCompleted) {
            hasCompleted = true;
            if (pullThreads!=null) {
                for (int i = 0; i < pullThreads.length; i++) {
                    if (pullThreads[i].isAlive()) {
                        pullThreads[i].interrupt();
                    }
                }
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

    private static class Row {

        final StaticBuffer key;
        final Map<SliceQuery,EntryList> entries;

        private Row(StaticBuffer key, Map<SliceQuery, EntryList> entries) {
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
                    while ((row=processorQueue.poll(100,TimeUnit.MILLISECONDS))!=null) {
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
                log.error("Unexpected error processing data: {}",e);
            } finally {
                job.workerIterationEnd(metrics);
            }
        }

        public void finish() {
            this.finished=true;
        }
    }


    private static class DataPuller extends Thread {

        private final BlockingQueue<SliceResult> queue;
        private final KeyIterator keyIter;
        private final SliceQuery query;
        private final Predicate<StaticBuffer> keyFilter;
        private volatile boolean finished;

        private DataPuller(SliceQuery query, BlockingQueue<SliceResult> queue,
                           KeyIterator keyIter, Predicate<StaticBuffer> keyFilter) {
            this.query = query;
            this.queue = queue;
            this.keyIter = keyIter;
            this.keyFilter = keyFilter;
            this.finished = false;
        }

        @Override
        public void run() {
            try {
                while (keyIter.hasNext()) {
                    StaticBuffer key = keyIter.next();
                    RecordIterator<Entry> entries = keyIter.getEntries();
                    if (!keyFilter.test(key)) continue;
                    EntryList entryList = StaticArrayEntryList.ofStaticBuffer(entries, StaticArrayEntry.ENTRY_GETTER);
                    queue.put(new SliceResult(query, key, entryList));
                }
                finished = true;
            } catch (InterruptedException e) {
                log.error("Data-pulling thread interrupted while waiting on queue or data", e);
            } catch (Throwable e) {
                log.error("Could not load data from storage: {}",e);
            } finally {
                try {
                    keyIter.close();
                } catch (IOException e) {
                    log.warn("Could not close storage iterator ", e);
                }
            }
        }

        public boolean isFinished() {
            return finished;
        }
    }

    private static class SliceResult {

        final SliceQuery query;
        final StaticBuffer key;
        final EntryList entries;

        private SliceResult(SliceQuery query, StaticBuffer key, EntryList entries) {
            this.query = query;
            this.key = key;
            this.entries = entries;
        }
    }



}



