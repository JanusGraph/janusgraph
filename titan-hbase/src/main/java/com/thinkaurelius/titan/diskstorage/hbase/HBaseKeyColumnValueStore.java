package com.thinkaurelius.titan.diskstorage.hbase;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Mutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Experimental HBase store.
 * <p/>
 * This is not ready for production.  It's pretty slow.
 * <p/>
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

    private static final Logger log = LoggerFactory.getLogger(HBaseKeyColumnValueStore.class);

    private final String tableName;
    private final HTablePool pool;

    // This is cf.getBytes()
    private final String columnFamily;
    private final byte[] columnFamilyBytes;
    private final HBaseStoreManager storeManager;

    HBaseKeyColumnValueStore(HTablePool pool, String tableName,
                             String columnFamily, HBaseStoreManager storeManager) {
        this.tableName = tableName;
        this.pool = pool;
        this.columnFamily = columnFamily;
        this.columnFamilyBytes = columnFamily.getBytes();
        this.storeManager = storeManager;
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
    public ByteBuffer get(ByteBuffer key, ByteBuffer column,
                          StoreTransaction txh) throws StorageException {

        byte[] keyBytes = toArray(key);
        byte[] colBytes = toArray(column);

        Get g = new Get(keyBytes);
        g.addColumn(columnFamilyBytes, colBytes);
        try {
            g.setMaxVersions(1);
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }

        try {
            HTableInterface table = null;
            Result r = null;

            try {
                table = pool.getTable(tableName);
                r = table.get(g);
            } finally {
                if (null != table)
                    table.close();
            }

            if (null == r) {
                return null;
            } else if (1 == r.size()) {
                return ByteBuffer.wrap(r.getValue(columnFamilyBytes, colBytes));
            } else if (0 == r.size()) {
                return null;
            } else {
                log.warn("Found {} results for key {}, column {}, family {} (expected 0 or 1 results)",
                        new Object[]{r.size(),
                                new String(Hex.encodeHex(keyBytes)),
                                new String(Hex.encodeHex(colBytes)),
                                new String(Hex.encodeHex(columnFamilyBytes))}
                );
                return null;
            }
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        }
    }

    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column,
                                     StoreTransaction txh) throws StorageException {
        return null != get(key, column, txh);
    }


    @Override
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {

        byte[] keyBytes = toArray(key);

        Get g = new Get(keyBytes);
        g.addFamily(columnFamilyBytes);

        try {
            HTableInterface table = null;
            try {
                table = pool.getTable(tableName);
                return table.exists(g);
            } finally {
                if (null != table)
                    table.close();
            }
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        }
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
                                ByteBuffer columnEnd, int limit, StoreTransaction txh) throws StorageException {

        byte[] colStartBytes = columnEnd.hasRemaining() ? toArray(columnStart) : null;
        byte[] colEndBytes = columnEnd.hasRemaining() ? toArray(columnEnd) : null;

        Filter colRangeFilter = new ColumnRangeFilter(colStartBytes, true, colEndBytes, false);
        Filter limitFilter = new ColumnPaginationFilter(limit, 0);

        FilterList bothFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL, colRangeFilter,
                limitFilter);

        return getHelper(key, bothFilters);
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
                                ByteBuffer columnEnd, StoreTransaction txh) throws StorageException {

        byte[] colStartBytes = columnEnd.hasRemaining() ? toArray(columnStart) : null;
        byte[] colEndBytes = columnEnd.hasRemaining() ? toArray(columnEnd) : null;

        Filter colRangeFilter = new ColumnRangeFilter(colStartBytes, true, colEndBytes, false);

        return getHelper(key, colRangeFilter);
    }

    private List<Entry> getHelper(ByteBuffer key,
                                  Filter getFilter) throws StorageException {

        byte[] keyBytes = toArray(key);

        Get g = new Get(keyBytes);
        g.addFamily(columnFamilyBytes);
        g.setFilter(getFilter);

        List<Entry> ret = null;

        try {
            HTableInterface table = null;
            Result r = null;

            try {
                table = pool.getTable(tableName);
                r = table.get(g);
            } finally {
                if (null != table)
                    table.close();
            }

            if (null == r)
                return new ArrayList<Entry>(0);

            int resultCount = r.size();

            ret = new ArrayList<Entry>(resultCount);

            Map<byte[], byte[]> fmap = r.getFamilyMap(columnFamilyBytes);

            if (null != fmap) {
                for (Map.Entry<byte[], byte[]> ent : fmap.entrySet()) {
                    ret.add(new Entry(ByteBuffer.wrap(ent.getKey()), ByteBuffer.wrap(ent.getValue())));
                }
            }

            return ret;
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        }
    }

    /*
     * This method exists because HBase's API generally deals in
     * whole byte[] arrays.  That is, data are always assumed to
     * begin at index zero and run through the native length of
     * the array.  These assumptions are reflected, for example,
     * in the classes hbase.client.Get and hbase.client.Scan.
     * These assumptions about arrays are not generally true when
     * dealing with ByteBuffers.
     * <p>
     * This method first checks whether the array backing the
     * ByteBuffer argument indeed satisfies the assumptions described
     * above.  If so, then this method returns the backing array.
     * In other words, this case returns {@code b.array()}.
     * <p>
     * If the ByteBuffer argument does not satisfy the array
     * assumptions described above, then a new native array of length
     * {@code b.limit()} is created.  The ByteBuffer's contents
     * are copied into the new native array without modifying the
     * state of {@code b} (using {@code b.duplicate()}).  The new
     * native array is then returned.
     *
     */
    static byte[] toArray(ByteBuffer b) {
        if (0 == b.arrayOffset() && b.limit() == b.array().length)
            return b.array();

        byte[] result = new byte[b.limit()];
        b.duplicate().get(result);
        return result;
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions,
                       List<ByteBuffer> deletions, StoreTransaction txh) throws StorageException {

        byte[] keyBytes = toArray(key);

        // TODO use RowMutations (requires 0.94.x-ish HBase)
        // error handling through the legacy batch() method sucks
        //RowMutations rms = new RowMutations(keyBytes);
        int totalsize = 0;

        if (null != additions)
            totalsize += additions.size();
        if (null != deletions)
            totalsize += deletions.size();

        List<Row> batchOps = new ArrayList<Row>(totalsize);

        // Deletes
        if (null != deletions && 0 != deletions.size()) {
            Delete d = new Delete(keyBytes);

            for (ByteBuffer del : deletions) {
                d.deleteColumn(columnFamilyBytes, toArray(del.duplicate()));
            }

            batchOps.add(d);
        }

        // Inserts
        if (null != additions && 0 != additions.size()) {
            Put p = new Put(keyBytes);

            for (Entry e : additions) {
                byte[] colBytes = toArray(e.getColumn().duplicate());
                byte[] valBytes = toArray(e.getValue().duplicate());

                p.add(columnFamilyBytes, colBytes, valBytes);
            }

            batchOps.add(p);
        }

        try {
            HTableInterface table = null;
            try {
                table = pool.getTable(tableName);
                table.batch(batchOps);
                table.flushCommits();
            } finally {
                if (null != table)
                    table.close();
            }
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        } catch (InterruptedException e) {
            throw new TemporaryStorageException(e);
        }
    }

    public void mutateMany(
            Map<ByteBuffer, Mutation> mutations,
            StoreTransaction txh) throws StorageException {
        storeManager.mutateMany(ImmutableMap.of(columnFamily, mutations), txh);
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column,
                            ByteBuffer expectedValue, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction txh) throws StorageException {
        Scan s = new Scan().addFamily(columnFamilyBytes);
        FilterList fl = new FilterList();
        // returns first instance of a row, then skip to next row
        fl.addFilter(new FirstKeyOnlyFilter());
        // only return the Key, don't return the value
        fl.addFilter(new KeyOnlyFilter());
        s.setFilter(fl);

        final ResultScanner scanner;

        try {
            scanner = pool.getTable(tableName).getScanner(s);
        } catch (IOException e) {
            throw new PermanentStorageException(e);
        }

        return new RecordIterator<ByteBuffer>() {
            /* we need to check if key is long serializable because HBase returns weird rows sometimes */
            private final Iterator<Result> results = Iterators.filter(scanner.iterator(), new Predicate<Result>() {
                @Override
                public boolean apply(@Nullable Result result) {
                    if (result == null)
                        return false;

                    try {
                        ByteBuffer id = ByteBuffer.wrap(result.getRow());
                        id.getLong();
                    } catch (NumberFormatException e) {
                        return false;
                    }

                    return true;
                }
            });

            @Override
            public boolean hasNext() throws StorageException {
                return results.hasNext();
            }

            @Override
            public ByteBuffer next() throws StorageException {
                return ByteBuffer.wrap(results.next().getRow());
            }

            @Override
            public void close() throws StorageException {
                scanner.close();
            }
        };
    }

    @Override
    public ByteBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return columnFamily;
    }

}
