package com.thinkaurelius.titan.diskstorage.hbase;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntryList;
import com.thinkaurelius.titan.util.system.IOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.*;

/**
 * Here are some areas that might need work:
 * <p/>
 * - batching? (consider HTable#batch, HTable#setAutoFlush(false)
 * - tuning HTable#setWriteBufferSize (?)
 * - writing a server-side filter to replace ColumnCountGetFilter, which drops
 * all columns on the row where it reaches its limit.  This requires getSlice,
 * currently, to impose its limit on the client side.  That obviously won't
 * scale.
 * - RowMutations for combining Puts+Deletes (need a newer HBase than 0.92 for this)
 * - (maybe) fiddle with HTable#setRegionCachePrefetch and/or #prewarmRegionCache
 * <p/>
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

    private final Configuration hconf;

    HBaseKeyColumnValueStore(HBaseStoreManager storeManager, Configuration hconf, String tableName, String columnFamily, String storeName) {
        this.storeManager = storeManager;
        this.hconf = hconf;
        this.tableName = tableName;
        //this.columnFamily = columnFamily;
        this.storeName = storeName;
        this.columnFamilyBytes = columnFamily.getBytes();
    }

    @Override
    public void close() throws StorageException {
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        byte[] keyBytes = key.as(StaticBuffer.ARRAY_FACTORY);

        Get g = new Get(keyBytes).addFamily(columnFamilyBytes);
        try {
            g.setTimeRange(0, Long.MAX_VALUE);
        } catch (IOException e) {
            throw new PermanentStorageException(e);
        }

        try {
            HConnection cnx = null;
            HTableInterface table = null;

            try {
                cnx = HConnectionManager.createConnection(hconf);
                table = cnx.getTable(tableName);
                return table.exists(g);
            } finally {
                IOUtils.closeQuietly(table);
                IOUtils.closeQuietly(cnx);
            }
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        }
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        Map<StaticBuffer, EntryList> result = getHelper(Arrays.asList(query.getKey()), getFilter(query));
        return Iterables.getOnlyElement(result.values(), EntryList.EMPTY_LIST);
    }

    @Override
    public Map<StaticBuffer,EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
        return getHelper(keys, getFilter(query));
    }

    public static Filter getFilter(SliceQuery query) {
        byte[] colStartBytes = query.getSliceEnd().length() > 0 ? query.getSliceStart().as(StaticBuffer.ARRAY_FACTORY) : null;
        byte[] colEndBytes = query.getSliceEnd().length() > 0 ? query.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY) : null;

        Filter filter = new ColumnRangeFilter(colStartBytes, true, colEndBytes, false);

        if (query.hasLimit()) {
            filter = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                    filter,
                    new ColumnPaginationFilter(query.getLimit(), 0));
        }

        logger.debug("Generated HBase Filter {}", filter);

        return filter;
    }

    private Map<StaticBuffer,EntryList> getHelper(List<StaticBuffer> keys, Filter getFilter) throws StorageException {
        List<Get> requests = new ArrayList<Get>(keys.size());
        {
            for (StaticBuffer key : keys) {
                Get g = new Get(key.as(StaticBuffer.ARRAY_FACTORY)).addFamily(columnFamilyBytes).setFilter(getFilter);
                try {
                    g.setTimeRange(0, Long.MAX_VALUE);
                } catch (IOException e) {
                    throw new PermanentStorageException(e);
                }
                requests.add(g);
            }
        }

        Map<StaticBuffer,EntryList> resultMap = new HashMap<StaticBuffer,EntryList>(keys.size());

        try {
            HConnection cnx = null;
            HTableInterface table = null;
            Result[] results = null;

            try {
                cnx = HConnectionManager.createConnection(hconf);
                table = cnx.getTable(tableName);
                results = table.get(requests);
            } finally {
                IOUtils.closeQuietly(table);
                IOUtils.closeQuietly(cnx);
            }

            if (results == null)
                return KCVSUtil.emptyResults(keys);

            assert results.length==keys.size();

            for (int i=0; i<results.length; i++) {
                Result result = results[i];
                Map<byte[], byte[]> fmap = result.getFamilyMap(columnFamilyBytes);
                EntryList entries;
                if (fmap == null) entries = EntryList.EMPTY_LIST;
                else entries = StaticArrayEntryList.ofBytes(fmap.entrySet(),MapEntryGetter.INSTANCE);
                resultMap.put(keys.get(i), entries);
            }

            return resultMap;
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        }
    }


    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws StorageException {
        Map<StaticBuffer, KCVMutation> mutations = ImmutableMap.of(key, new KCVMutation(additions, deletions));
        mutateMany(mutations, txh);
    }

    public void mutateMany(Map<StaticBuffer, KCVMutation> mutations, StoreTransaction txh) throws StorageException {
        storeManager.mutateMany(ImmutableMap.of(storeName, mutations), txh);
    }

    @Override
    public void acquireLock(StaticBuffer key,
                            StaticBuffer column,
                            StaticBuffer expectedValue,
                            StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws StorageException {
        return executeKeySliceQuery(query.getKeyStart().as(StaticBuffer.ARRAY_FACTORY),
                query.getKeyEnd().as(StaticBuffer.ARRAY_FACTORY),
                new FilterList(FilterList.Operator.MUST_PASS_ALL),
                query);
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws StorageException {
        return executeKeySliceQuery(new FilterList(FilterList.Operator.MUST_PASS_ALL), query);
    }

    public KeyIterator executeKeySliceQuery(FilterList filters, @Nullable SliceQuery columnSlice) throws StorageException {
        return executeKeySliceQuery(null, null, filters, columnSlice);
    }

    public KeyIterator executeKeySliceQuery(@Nullable byte[] startKey,
                                            @Nullable byte[] endKey,
                                            FilterList filters,
                                            @Nullable SliceQuery columnSlice) throws StorageException {
        Scan scan = new Scan().addFamily(columnFamilyBytes);

        try {
            scan.setTimeRange(0, Long.MAX_VALUE);
        } catch (IOException e) {
            throw new PermanentStorageException(e);
        }

        if (startKey != null)
            scan.setStartRow(startKey);

        if (endKey != null)
            scan.setStopRow(endKey);

        if (columnSlice != null) {
            filters.addFilter(getFilter(columnSlice));
        }

        HConnection cnx = null;
        HTableInterface table = null;

        try {
            cnx = HConnectionManager.createConnection(hconf);
            table = cnx.getTable(tableName);
            return new RowIterator(cnx, table, table.getScanner(scan.setFilter(filters)));
        } catch (IOException e) {
            IOUtils.closeQuietly(table);
            IOUtils.closeQuietly(cnx);
            throw new PermanentStorageException(e);
        }
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws StorageException {
        return storeManager.getLocalKeyPartition();
    }

    @Override
    public String getName() {
        return storeName;
    }

    private class RowIterator implements KeyIterator {
        private final HConnection cnx;
        private final HTableInterface table;
        private final Iterator<Result> rows;

        private Result currentRow;
        private boolean isClosed;

        public RowIterator(HConnection cnx, HTableInterface table, ResultScanner rows) {
            this.cnx = cnx;
            this.table = table;
            this.rows = Iterators.filter(rows.iterator(), new Predicate<Result>() {
                @Override
                public boolean apply(@Nullable Result result) {
                    if (result == null)
                        return false;

                    try {
                        StaticBuffer id = StaticArrayBuffer.of(result.getRow());
                        id.getLong(0);
                    } catch (NumberFormatException e) {
                        return false;
                    }

                    return true;
                }
            });
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            return new RecordIterator<Entry>() {
                private final Iterator<Map.Entry<byte[], byte[]>> kv = currentRow.getFamilyMap(columnFamilyBytes).entrySet().iterator();

                @Override
                public boolean hasNext() {
                    ensureOpen();
                    return kv.hasNext();
                }

                @Override
                public Entry next() {
                    ensureOpen();
                    Map.Entry<byte[], byte[]> column = kv.next();
                    return StaticArrayEntry.ofBytes(column, MapEntryGetter.INSTANCE);
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
            IOUtils.closeQuietly(cnx);
            isClosed = true;
            logger.debug("Closed RowIterator references: {}, {}", table, cnx);
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

    private static enum MapEntryGetter implements StaticArrayEntry.GetColVal<Map.Entry<byte[],byte[]>,byte[]> {
        INSTANCE;

        @Override
        public byte[] getColumn(Map.Entry<byte[], byte[]> element) {
            return element.getKey();
        }

        @Override
        public byte[] getValue(Map.Entry<byte[], byte[]> element) {
            return element.getValue();
        }
    }
}
