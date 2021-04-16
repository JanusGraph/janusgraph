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
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.MultiSlicesQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.Predicate;

import static org.janusgraph.diskstorage.keycolumnvalue.scan.StandardScannerExecutor.Row;

/**
 *  Uses one thread for all queries. May be used for {@link KeyColumnValueStore}
 *  that do not guarantee keys order between different scans (f.e. Aerospike)
 * @author Sergii Karpenko (sergiy.karpenko@gmail.com)
 */

class SingleThreadRowsCollector extends RowsCollector {

    private static final Logger log = LoggerFactory.getLogger(SingleThreadRowsCollector.class);

    private final StoreTransaction storeTx;
    private final Predicate<StaticBuffer> keyFilter;
    private KeySlicesIterator keyIterator;

    private boolean interrupted = false;

    SingleThreadRowsCollector(
        KeyColumnValueStore store,
        StoreTransaction storeTx,
        List<SliceQuery> queries,
        Predicate<StaticBuffer> keyFilter,
        BlockingQueue<Row> rowQueue) throws BackendException {

        super(store, rowQueue);
        this.storeTx = storeTx;
        this.keyFilter = keyFilter;

        setUp(queries);
    }

    private void setUp(List<SliceQuery> queries) throws BackendException {
        keyIterator = store.getKeys(new MultiSlicesQuery(queries), storeTx);
    }

    void run()  {
        try {
            while (!interrupted && keyIterator.hasNext()) {
                StaticBuffer key = keyIterator.next();
                Map<SliceQuery, RecordIterator<Entry>> sliceToEntriesMap = keyIterator.getEntries();
                if (!keyFilter.test(key)) continue;
                Map<SliceQuery, EntryList> rowEntries = new HashMap<>(sliceToEntriesMap.size());
                sliceToEntriesMap.forEach((sliceQuery, entryList) -> rowEntries.put(sliceQuery, EntryArrayList.of(entryList)));
                rowQueue.put(new Row(key, rowEntries));
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
        }
    }

    @Override
    void join() {
        //no need to wait
    }

    @Override
    void interrupt() {
        interrupted = true;
    }

    @Override
    void cleanup() throws PermanentBackendException {
        try {
            keyIterator.close();
        } catch (IOException e) {
            throw new PermanentBackendException(e);
        }
    }

}
