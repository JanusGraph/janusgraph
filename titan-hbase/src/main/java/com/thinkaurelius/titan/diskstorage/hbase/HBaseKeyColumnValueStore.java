package com.thinkaurelius.titan.diskstorage.hbase;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.util.system.IOUtils;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
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
    private final HTablePool pool;
    private final HBaseStoreManager storeManager;

    // When using shortened CF names, columnFamily is the shortname and storeName is the longname
    // When not using shortened CF names, they are the same
    private final String columnFamily;
    private final String storeName;
    // This is columnFamily.getBytes()
    private final byte[] columnFamilyBytes;

    HBaseKeyColumnValueStore(HBaseStoreManager storeManager, HTablePool pool, String tableName, String columnFamily, String storeName) {
        this.storeManager = storeManager;
        this.tableName = tableName;
        this.pool = pool;
        this.columnFamily = columnFamily;
        this.storeName = storeName;
        this.columnFamilyBytes = columnFamily.getBytes();
    }

    @Override
    public void close() throws StorageException {
        try {
            pool.close();
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        }
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        byte[] keyBytes = key.as(StaticBuffer.ARRAY_FACTORY);

        Get g = new Get(keyBytes).addFamily(columnFamilyBytes);

        try {
            HTableInterface table = null;

            try {
                table = pool.getTable(tableName);
                return table.exists(g);
            } finally {
                IOUtils.closeQuietly(table);
            }
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        }
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        List<List<Entry>> result = getHelper(Arrays.asList(query.getKey()), getFilter(query));
        return (result.isEmpty()) ? Collections.<Entry>emptyList() : result.get(0);
    }

    @Override
    public List<List<Entry>> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
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

        return filter;
    }

    private List<List<Entry>> getHelper(List<StaticBuffer> keys, Filter getFilter) throws StorageException {
        List<Get> requests = new ArrayList<Get>(keys.size());
        {
            for (StaticBuffer key : keys) {
                requests.add(new Get(key.as(StaticBuffer.ARRAY_FACTORY)).addFamily(columnFamilyBytes).setFilter(getFilter));
            }
        }

        List<List<Entry>> results = new ArrayList<List<Entry>>();

        try {
            HTableInterface table = null;
            Result[] r = null;

            try {
                table = pool.getTable(tableName);
                r = table.get(requests);
            } finally {
                IOUtils.closeQuietly(table);
            }

            if (r == null)
                return Collections.emptyList();

            for (Result result : r) {
                List<Entry> entries = new ArrayList<Entry>(result.size());
                Map<byte[], byte[]> fmap = result.getFamilyMap(columnFamilyBytes);

                if (null != fmap) {
                    for (Map.Entry<byte[], byte[]> ent : fmap.entrySet()) {
                        entries.add(StaticBufferEntry.of(new StaticArrayBuffer(ent.getKey()), new StaticArrayBuffer(ent.getValue())));
                    }
                }

                results.add(entries);
            }

            return results;
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

        if (startKey != null)
            scan.setStartRow(startKey);

        if (endKey != null)
            scan.setStopRow(endKey);

        if (columnSlice != null) {
            filters.addFilter(getFilter(columnSlice));
        }

        try {
            return new RowIterator(pool.getTable(tableName).getScanner(scan.setFilter(filters)));
        } catch (IOException e) {
            throw new PermanentStorageException(e);
        }
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return storeName;
    }

    /**
     * Convert deletions to a Delete command.
     *
     * @param cfName    The name of the ColumnFamily deletions belong to
     * @param key       The row key
     * @param deletions The name of the columns to delete (a.k.a deletions)
     * @return Delete command or null if deletions were null or empty.
     */
    private static Delete makeDeletionCommand(byte[] cfName, byte[] key, List<StaticBuffer> deletions) {
        Preconditions.checkArgument(!deletions.isEmpty());

        Delete deleteCommand = new Delete(key);
        for (StaticBuffer del : deletions) {
            deleteCommand.deleteColumn(cfName, del.as(StaticBuffer.ARRAY_FACTORY));
        }
        return deleteCommand;
    }

    /**
     * Convert modification entries into Put command.
     *
     * @param cfName        The name of the ColumnFamily modifications belong to
     * @param key           The row key
     * @param modifications The entries to insert/update.
     * @return Put command or null if additions were null or empty.
     */
    private static Put makePutCommand(byte[] cfName, byte[] key, List<Entry> modifications) {
        Preconditions.checkArgument(!modifications.isEmpty());

        Put putCommand = new Put(key);
        for (Entry e : modifications) {
            putCommand.add(cfName, e.getArrayColumn(), e.getArrayValue());
        }
        return putCommand;
    }

    public static List<Row> makeBatch(byte[] cfName, byte[] key, List<Entry> additions, List<StaticBuffer> deletions) {
        if (additions.isEmpty() && deletions.isEmpty()) return Collections.emptyList();

        List<Row> batch = new ArrayList<Row>(2);

        if (!additions.isEmpty()) {
            Put putCommand = makePutCommand(cfName, key, additions);
            batch.add(putCommand);
        }

        if (!deletions.isEmpty()) {
            Delete deleteCommand = makeDeletionCommand(cfName, key, deletions);
            batch.add(deleteCommand);
        }
        return batch;
    }

    private class RowIterator implements KeyIterator {
        private final Iterator<Result> rows;

        private Result currentRow;
        private boolean isClosed;

        public RowIterator(ResultScanner rows) {
            this.rows = Iterators.filter(rows.iterator(), new Predicate<Result>() {
                @Override
                public boolean apply(@Nullable Result result) {
                    if (result == null)
                        return false;

                    try {
                        StaticBuffer id = new StaticArrayBuffer(result.getRow());
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
                    return StaticBufferEntry.of(new StaticArrayBuffer(column.getKey()), new StaticArrayBuffer(column.getValue()));
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
            return new StaticArrayBuffer(currentRow.getRow());
        }

        @Override
        public void close() {
            isClosed = true;
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
}
