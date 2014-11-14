package com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractFuture;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntryList;
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
class StandardScannerExecutor extends AbstractFuture<ScanMetrics> implements StandardScanner.ScanResult, Runnable {

    private static final Logger log =
            LoggerFactory.getLogger(StandardScannerExecutor.class);

    private static final int QUEUE_SIZE = 1000;
    private static final int TIMEOUT_MS = 60000; // 60 seconds
    private static final int MAX_KEY_LENGTH = 128; //in bytes

    private final ScanJob job;
    private final Consumer<ScanMetrics> finishJob;
    private final StoreFeatures storeFeatures;
    private final StoreTransaction storeTx;
    private final KeyColumnValueStore store;
    private final int numProcessors;
    private final Configuration jobConfiguration;
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
                            final int numProcessors, final Configuration jobConfiguration) throws BackendException {
        this.job = job;
        this.finishJob = finishJob;
        this.store = store;
        this.storeTx = storeTx;
        this.storeFeatures = storeFeatures;
        this.numProcessors = numProcessors;
        this.jobConfiguration = jobConfiguration;

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
            job.setup(jobConfiguration,metrics);

            queries = job.getQueries();
            numQueries = queries.size();
            Preconditions.checkArgument(numQueries>0,"Must at least specify one query for job: %s",job);
            if (numQueries>1) {
                //It is assumed that the first query is the grounding query if multiple queries exist
                SliceQuery ground = queries.get(0);
                StaticBuffer start = ground.getSliceStart();
                Preconditions.checkArgument(start.equals(BufferUtil.zeroBuffer(start.length())),
                        "Expected start of first query to be all 0s: %s",start);
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
            setException(e);
            return;
        }

        ThreadPoolExecutor processor = new ThreadPoolExecutor(numProcessors, numProcessors, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(QUEUE_SIZE));
        processor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
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
                    if (currentResults[i]!=null && currentResults[i].key.equals(key)) {
                        queryResults.put(currentResults[i].query, currentResults[i].entries);
                        currentResults[i]=null;
                    }
                }
                processor.submit(new RowProcessor(key, queryResults));

            }

            for (int i = 0; i < pullThreads.length; i++) {
                pullThreads[i].join(10);
                if (pullThreads[i].isAlive()) {
                    log.warn("Data pulling thread [{}] did not terminate. Forcing termination",i);
                    pullThreads[i].interrupt();
                }
            }

            processor.shutdown();
            processor.awaitTermination(TIMEOUT_MS,TimeUnit.MILLISECONDS);
            if (!processor.isTerminated()) log.error("Processor did not terminate in time");

            cleanup();
            job.teardown(metrics);
            if (interrupted) {
                setException(new InterruptedException("Scanner got interrupted"));
            } else {
                finishJob.accept(metrics);
                set(metrics);
            }
        } catch (Throwable e) {
            log.error("Exception occured during job execution: {}",e);
            setException(e);
        } finally {
            processor.shutdownNow();
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

    private class RowProcessor implements Runnable {

        private final StaticBuffer key;
        private final Map<SliceQuery,EntryList> entries;

        private RowProcessor(StaticBuffer key, Map<SliceQuery, EntryList> entries) {
            this.key = key;
            this.entries = entries;
        }

        @Override
        public void run() {
            try {
                job.process(key,entries,metrics);
                metrics.increment(ScanMetrics.Metric.SUCCESS);
            } catch (Throwable ex) {
                log.error("Exception processing row ["+key+"]: ",ex);
                metrics.increment(ScanMetrics.Metric.FAILURE);
            }
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



