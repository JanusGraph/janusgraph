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

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVSUtil;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.janusgraph.diskstorage.keycolumnvalue.scan.StandardScannerExecutor.Row;
import static org.janusgraph.diskstorage.keycolumnvalue.scan.StandardScannerExecutor.TIME_PER_TRY;

/**
 * Uses separate thread per query. May be used for {@link KeyColumnValueStore}
 * that preserves keys order while running parallel scans (f.e. Cassandra)
 *
 * @author Sergii Karpenko (sergiy.karpenko@gmail.com)
 */

class MultiThreadsRowsCollector extends RowsCollector {

    private static final int MAX_KEY_LENGTH = 128; //in bytes

    private static final Logger log = LoggerFactory.getLogger(MultiThreadsRowsCollector.class);

    private final StoreFeatures storeFeatures;
    private final StoreTransaction storeTx;
    private final List<SliceQuery> queries;
    private final Predicate<StaticBuffer> keyFilter;
    private final Configuration graphConfiguration;
    private final DataPuller[] pullThreads;
    private final BlockingQueue<SliceResult>[] dataQueues;
    private boolean interrupted = false;

    MultiThreadsRowsCollector(
        KeyColumnValueStore store,
        StoreFeatures storeFeatures,
        StoreTransaction storeTx,
        List<SliceQuery> queries,
        Predicate<StaticBuffer> keyFilter,
        BlockingQueue<Row> rowQueue,
        Configuration graphConfiguration) throws BackendException {

        super(store, rowQueue);
        this.storeFeatures = storeFeatures;
        this.storeTx = storeTx;
        this.queries = queries;
        this.keyFilter = keyFilter;
        this.graphConfiguration = graphConfiguration;

        this.dataQueues = new BlockingQueue[queries.size()];
        this.pullThreads = new DataPuller[queries.size()];

        setUp(queries);
    }

    private void setUp(List<SliceQuery> queries) throws BackendException {

        int pos = 0;
        for(SliceQuery sliceQuery : queries){
            addDataPuller(sliceQuery, storeTx, pos);
            pos++;
        }
    }

    void run() throws InterruptedException, TemporaryBackendException {
        int numQueries = queries.size();
        SliceResult[] currentResults = new SliceResult[numQueries];
        while (!interrupted) {
            collectDataFromPullers(currentResults, numQueries);

            SliceResult conditionQuery = currentResults[0];
            if (conditionQuery==null) break; //Termination condition - primary query has no more data
            final StaticBuffer key = conditionQuery.key;

            Row e = buildRow(numQueries, currentResults, key);

            rowQueue.put(e);
        }
    }

    private void collectDataFromPullers(SliceResult[] currentResults, int numQueries) throws InterruptedException, TemporaryBackendException {
        for (int i = 0; i < numQueries; i++) {
            if (currentResults[i]!=null) continue;
            BlockingQueue<SliceResult> queue = dataQueues[i];

            SliceResult qr = queue.poll(TIME_PER_TRY, TimeUnit.MILLISECONDS); //Try very short time to see if we are done
            if (qr==null) {
                DataPuller dataPuller = pullThreads[i];
                if (dataPuller.isFinished()) continue; //No more data to be expected
                while (!dataPuller.isFinished() && qr == null) {
                    qr = queue.poll(TIME_PER_TRY, TimeUnit.MILLISECONDS);
                }
                if (qr==null && !dataPuller.isFinished())
                    throw new TemporaryBackendException("Timed out waiting for next row data - storage error likely");
            }
            currentResults[i]=qr;
        }
    }

    private Row buildRow(int numQueries, SliceResult[] currentResults, StaticBuffer key) {
        Map<SliceQuery,EntryList> queryResults = new HashMap<>(numQueries);
        for (int i = 0; i< currentResults.length; i++) {
            SliceQuery query = queries.get(i);
            EntryList entries = EntryList.EMPTY_LIST;
            if (currentResults[i]!=null && currentResults[i].key.equals(key)) {
                assert query.equals(currentResults[i].query);
                entries = currentResults[i].entries;
                currentResults[i]=null;
            }
            queryResults.put(query,entries);
        }
        return new Row(key, queryResults);
    }

    @Override
    void join() throws InterruptedException {
        int i = 0;
        for (DataPuller dataPuller : pullThreads) {
            dataPuller.join(10);
            if (dataPuller.isAlive()) {
                log.warn("Data pulling thread [{}] did not terminate. Forcing termination",i);
                if (storeFeatures.supportsInterruption()) {
                    dataPuller.interrupt();
                } else {
                    log.warn("Store does not support interruption, so data pulling thread [{}] cannot be interrupted", i);
                    dataPuller.finished = true;
                }
            }
            i++;
        }
    }

    @Override
    void interrupt() {
        interrupted = true;
    }

    @Override
    void cleanup() {
        if (pullThreads!=null) {
            for (DataPuller pullThread : pullThreads) {
                if (pullThread.isAlive()) {
                    if (storeFeatures.supportsInterruption()) {
                        pullThread.interrupt();
                    } else {
                        log.warn("Store does not support interruption, so data pulling thread cannot be interrupted");
                        pullThread.finished = true;
                    }
                }
            }
        }
    }

    private void addDataPuller(SliceQuery sq, StoreTransaction stx, int pos) throws BackendException {
        final BlockingQueue<SliceResult> queue = new LinkedBlockingQueue<>(
            this.graphConfiguration.get(GraphDatabaseConfiguration.PAGE_SIZE));
        dataQueues[pos] = queue;

        DataPuller dp = new DataPuller(sq, queue,
            KCVSUtil.getKeys(store,sq,storeFeatures, MAX_KEY_LENGTH,stx), keyFilter);
        pullThreads[pos] = dp;
        dp.setName("data-puller-" + pos); // setting the name for thread dumps!
        dp.start();
    }

    private static class DataPuller extends Thread {

        private final BlockingQueue<SliceResult> queue;
        private final KeyIterator keyIterator;
        private final SliceQuery query;
        private final Predicate<StaticBuffer> keyFilter;
        private volatile boolean finished;

        private DataPuller(SliceQuery query, BlockingQueue<SliceResult> queue,
                           KeyIterator keyIterator, Predicate<StaticBuffer> keyFilter) {
            this.query = query;
            this.queue = queue;
            this.keyIterator = keyIterator;
            this.keyFilter = keyFilter;
            this.finished = false;
        }

        @Override
        public void run() {
            try {
                while (keyIterator.hasNext()) {
                    StaticBuffer key = keyIterator.next();
                    RecordIterator<Entry> entries = keyIterator.getEntries();
                    if (!keyFilter.test(key)) continue;
                    EntryList entryList = EntryArrayList.of(entries);
                    queue.put(new SliceResult(query, key, entryList));
                }
            } catch (InterruptedException e) {
                log.error("Data-pulling thread interrupted while waiting on queue or data", e);
            } catch (Throwable e) {
                log.error("Could not load data from storage", e);
            } finally {
                try {
                    keyIterator.close();
                } catch (IOException e) {
                    log.warn("Could not close storage iterator ", e);
                }
                finished=true;
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
