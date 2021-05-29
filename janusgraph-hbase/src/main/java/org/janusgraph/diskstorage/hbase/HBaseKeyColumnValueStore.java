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

package org.janusgraph.diskstorage.hbase;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KCVSUtil;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.MultiSlicesQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.janusgraph.util.system.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 * Here are some areas that might need work:
 * <p>
 * - batching? (consider HTable#batch, HTable#setAutoFlush(false)
 * - tuning HTable#setWriteBufferSize (?)
 * - writing a server-side filter to replace ColumnCountGetFilter, which drops
 * all columns on the row where it reaches its limit.  This requires getSlice,
 * currently, to impose its limit on the client side.  That obviously won't
 * scale.
 * - RowMutations for combining Puts+Deletes (need a newer HBase than 0.92 for this)
 * - (maybe) fiddle with HTable#setRegionCachePrefetch and/or #prewarmRegionCache
 * <p>
 * There may be other problem areas.  These are just the ones of which I'm aware.
 */
public class HBaseKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger logger = LoggerFactory.getLogger(HBaseKeyColumnValueStore.class);

    private final String tableName;
    private final HBaseStoreManager storeManager;

    // When using shortened CF names, columnFamily is the shortname and storeName is the longname
    // When not using shortened CF names, they are the same
    //private final String columnFamily;
    private final String storeName;
    // This is columnFamily.getBytes()
    private final byte[] columnFamilyBytes;
    private final HBaseGetter entryGetter;

    private final ConnectionMask cnx;

    HBaseKeyColumnValueStore(HBaseStoreManager storeManager, ConnectionMask cnx, String tableName, String columnFamily, String storeName) {
        this.storeManager = storeManager;
        this.cnx = cnx;
        this.tableName = tableName;
        //this.columnFamily = columnFamily;
        this.storeName = storeName;
        this.columnFamilyBytes = Bytes.toBytes(columnFamily);
        this.entryGetter = new HBaseGetter(storeManager.getMetaDataSchema(storeName));
    }

    @Override
    public void close() throws BackendException {
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer, EntryList> result = getHelper(Collections.singletonList(query.getKey()), getFilter(query));
        return Iterables.getOnlyElement(result.values(), EntryList.EMPTY_LIST);
    }

    @Override
    public Map<StaticBuffer,EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        return getHelper(keys, getFilter(query));
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer, KCVMutation> mutations = ImmutableMap.of(key, new KCVMutation(additions, deletions));
        mutateMany(mutations, txh);
    }

    @Override
    public void acquireLock(StaticBuffer key,
                            StaticBuffer column,
                            StaticBuffer expectedValue,
                            StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        return executeKeySliceQuery(query.getKeyStart().as(StaticBuffer.ARRAY_FACTORY),
                query.getKeyEnd().as(StaticBuffer.ARRAY_FACTORY),
                new FilterList(FilterList.Operator.MUST_PASS_ALL),
                query);
    }

    @Override
    public String getName() {
        return storeName;
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        return executeKeySliceQuery(new FilterList(FilterList.Operator.MUST_PASS_ALL), query);
    }

    @Override
    public KeySlicesIterator getKeys(MultiSlicesQuery queries, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    public static Filter getFilter(SliceQuery query) {
        byte[] colStartBytes = query.getSliceStart().length() > 0 ? query.getSliceStart().as(StaticBuffer.ARRAY_FACTORY) : null;
        byte[] colEndBytes = query.getSliceEnd().length() > 0 ? query.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY) : null;

        Filter filter = new ColumnRangeFilter(colStartBytes, true, colEndBytes, false);

        if (query.hasLimit()) {
            filter = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                    filter,
                    new ColumnPaginationFilter(query.getLimit(), colStartBytes));
        }

        logger.debug("Generated HBase Filter {}", filter);

        return filter;
    }

    private Map<StaticBuffer,EntryList> getHelper(List<StaticBuffer> keys, Filter getFilter) throws BackendException {
        List<Get> requests = new ArrayList<>(keys.size());
        {
            for (StaticBuffer key : keys) {
                Get g = new Get(key.as(StaticBuffer.ARRAY_FACTORY)).addFamily(columnFamilyBytes).setFilter(getFilter);
                try {
                    g.setTimeRange(0, Long.MAX_VALUE);
                } catch (IOException e) {
                    throw new PermanentBackendException(e);
                }
                requests.add(g);
            }
        }

        final Map<StaticBuffer,EntryList> resultMap = new HashMap<>(keys.size());

        try {
            TableMask table = null;
            final Result[] results;

            try {
                table = cnx.getTable(tableName);
                results = table.get(requests);
            } finally {
                IOUtils.closeQuietly(table);
            }

            if (results == null)
                return KCVSUtil.emptyResults(keys);

            assert results.length==keys.size();

            for (int i = 0; i < results.length; i++) {
                final Result result = results[i];
                NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> f = result.getMap();

                if (f == null) { // no result for this key
                    resultMap.put(keys.get(i), EntryList.EMPTY_LIST);
                    continue;
                }

                // actual key with <timestamp, value>
                NavigableMap<byte[], NavigableMap<Long, byte[]>> r = f.get(columnFamilyBytes);
                resultMap.put(keys.get(i), (r == null)
                                            ? EntryList.EMPTY_LIST
                                            : StaticArrayEntryList.ofBytes(r.entrySet(), entryGetter));
            }

            return resultMap;
        } catch (InterruptedIOException e) {
            // added to support traversal interruption
            Thread.currentThread().interrupt();
            throw new PermanentBackendException(e);
        } catch (IOException e) {
            throw new TemporaryBackendException(e);
        }
    }

    private void mutateMany(Map<StaticBuffer, KCVMutation> mutations, StoreTransaction txh) throws BackendException {
        storeManager.mutateMany(ImmutableMap.of(storeName, mutations), txh);
    }

    private KeyIterator executeKeySliceQuery(FilterList filters, @Nullable SliceQuery columnSlice) throws BackendException {
        return executeKeySliceQuery(null, null, filters, columnSlice);
    }

    private KeyIterator executeKeySliceQuery(@Nullable byte[] startKey,
                                            @Nullable byte[] endKey,
                                            FilterList filters,
                                            @Nullable SliceQuery columnSlice) throws BackendException {
        Scan scan = new Scan().addFamily(columnFamilyBytes);

        try {
            scan.setTimeRange(0, Long.MAX_VALUE);
        } catch (IOException e) {
            throw new PermanentBackendException(e);
        }

        if (startKey != null)
            scan.setStartRow(startKey);

        if (endKey != null)
            scan.setStopRow(endKey);

        if (columnSlice != null) {
            filters.addFilter(getFilter(columnSlice));
        }

        TableMask table = null;

        try {
            table = cnx.getTable(tableName);
            return new RowIterator(table, table.getScanner(scan.setFilter(filters)), columnFamilyBytes);
        } catch (IOException e) {
            IOUtils.closeQuietly(table);
            throw new PermanentBackendException(e);
        }
    }

    private class RowIterator implements KeyIterator {
        private final Closeable table;
        private final Iterator<Result> rows;
        private final byte[] columnFamilyBytes;

        private Result currentRow;
        private boolean isClosed;

        public RowIterator(Closeable table, ResultScanner rows, byte[] columnFamilyBytes) {
            this.table = table;
            this.columnFamilyBytes = Arrays.copyOf(columnFamilyBytes, columnFamilyBytes.length);
            this.rows = Iterators.filter(rows.iterator(), result -> null != result && null != result.getRow());
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            return new RecordIterator<Entry>() {
                private final Iterator<Map.Entry<byte[], NavigableMap<Long, byte[]>>> kv;
                {
                    final Map<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = currentRow.getMap();
                    Preconditions.checkNotNull(map);
                    kv = map.get(columnFamilyBytes).entrySet().iterator();
                }

                @Override
                public boolean hasNext() {
                    ensureOpen();
                    return kv.hasNext();
                }

                @Override
                public Entry next() {
                    ensureOpen();
                    return StaticArrayEntry.ofBytes(kv.next(), entryGetter);
                }

                @Override
                public void close() {
                    isClosed = true;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public boolean hasNext() {
            ensureOpen();
            return rows.hasNext();
        }

        @Override
        public StaticBuffer next() {
            ensureOpen();

            currentRow = rows.next();
            return StaticArrayBuffer.of(currentRow.getRow());
        }

        @Override
        public void close() {
            IOUtils.closeQuietly(table);
            isClosed = true;
            logger.debug("RowIterator closed table {}", table);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void ensureOpen() {
            if (isClosed)
                throw new IllegalStateException("Iterator has been closed.");
        }
    }

    private static class HBaseGetter implements StaticArrayEntry.GetColVal<Map.Entry<byte[], NavigableMap<Long, byte[]>>, byte[]> {

        private final EntryMetaData[] schema;

        private HBaseGetter(EntryMetaData[] schema) {
            this.schema = schema;
        }

        @Override
        public byte[] getColumn(Map.Entry<byte[], NavigableMap<Long, byte[]>> element) {
            return element.getKey();
        }

        @Override
        public byte[] getValue(Map.Entry<byte[], NavigableMap<Long, byte[]>> element) {
            return element.getValue().lastEntry().getValue();
        }

        @Override
        public EntryMetaData[] getMetaSchema(Map.Entry<byte[], NavigableMap<Long, byte[]>> element) {
            return schema;
        }

        @Override
        public Object getMetaData(Map.Entry<byte[], NavigableMap<Long, byte[]>> element, EntryMetaData meta) {
            switch(meta) {
                case TIMESTAMP:
                    return element.getValue().lastEntry().getKey();
                default:
                    throw new UnsupportedOperationException("Unsupported meta data: " + meta);
            }
        }
    }
}
