package com.thinkaurelius.titan.diskstorage.hbase;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;

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

    private static final Logger logger = LoggerFactory.getLogger(HBaseKeyColumnValueStore.class);

    private final String tableName;
    private final HTablePool pool;

    private final String columnFamily;
    // This is columnFamily.getBytes()
    private final byte[] columnFamilyBytes;

    HBaseKeyColumnValueStore(HTablePool pool, String tableName, String columnFamily) {
        this.tableName = tableName;
        this.pool = pool;
        this.columnFamily = columnFamily;
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
    public ByteBuffer get(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        byte[] keyBytes = ByteBufferUtil.getArray(key);
        byte[] colBytes = ByteBufferUtil.getArray(column);

        Get g = new Get(keyBytes).addColumn(columnFamilyBytes, colBytes);

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
                IOUtils.closeQuietly(table);
            }

            if (null == r) {
                return null;
            } else if (1 == r.size()) {
                return ByteBuffer.wrap(r.getValue(columnFamilyBytes, colBytes));
            } else if (0 == r.size()) {
                return null;
            } else {
                logger.warn("Found {} results for key {}, column {}, family {} (expected 0 or 1 results)",
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
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column, StoreTransaction txh) throws StorageException {
        return null != get(key, column, txh);
    }


    @Override
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {
        byte[] keyBytes = ByteBufferUtil.getArray(key);

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
    public List<Entry> getSlice(ByteBuffer key,
                                ByteBuffer columnStart,
                                ByteBuffer columnEnd,
                                int limit,
                                StoreTransaction txh) throws StorageException {
        byte[] colStartBytes = columnEnd.hasRemaining() ? ByteBufferUtil.getArray(columnStart) : null;
        byte[] colEndBytes = columnEnd.hasRemaining() ? ByteBufferUtil.getArray(columnEnd) : null;

        Filter colRangeFilter = new ColumnRangeFilter(colStartBytes, true, colEndBytes, false);
        Filter limitFilter = new ColumnPaginationFilter(limit, 0);

        return getHelper(key, new FilterList(FilterList.Operator.MUST_PASS_ALL, colRangeFilter, limitFilter));
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key,
                                ByteBuffer columnStart,
                                ByteBuffer columnEnd,
                                StoreTransaction txh) throws StorageException {

        byte[] colStartBytes = columnEnd.hasRemaining()
                                 ? ByteBufferUtil.getArray(columnStart) : null;
        byte[] colEndBytes   = columnEnd.hasRemaining()
                                 ? ByteBufferUtil.getArray(columnEnd) : null;

        return getHelper(key, new ColumnRangeFilter(colStartBytes, true, colEndBytes, false));
    }

    private List<Entry> getHelper(ByteBuffer key, Filter getFilter) throws StorageException {
        byte[] keyBytes = ByteBufferUtil.getArray(key);

        Get g = new Get(keyBytes).addFamily(columnFamilyBytes).setFilter(getFilter);

        List<Entry> ret;

        try {
            HTableInterface table = null;
            Result r = null;

            try {
                table = pool.getTable(tableName);
                r = table.get(g);
            } finally {
                IOUtils.closeQuietly(table);
            }

            if (r == null)
                return Collections.emptyList();

            ret = new ArrayList<Entry>(r.size());

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

    @Override
    public void mutate(ByteBuffer key,
                       List<Entry> additions,
                       List<ByteBuffer> deletions,
                       StoreTransaction txh) throws StorageException {
        // TODO: use RowMutations (requires 0.94.x-ish HBase), error handling through the legacy batch() method sucks
        List<Row> batch = makeBatch(columnFamilyBytes, ByteBufferUtil.getArray(key), additions, deletions);

        if (batch.isEmpty())
            return; // nothing to apply

        try {
            HTableInterface table = null;

            try {
                table = pool.getTable(tableName);
                table.batch(batch);
                table.flushCommits();
            } finally {
                IOUtils.closeQuietly(table);
            }
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        } catch (InterruptedException e) {
            throw new TemporaryStorageException(e);
        }
    }

    @Override
    public void acquireLock(ByteBuffer key,
                            ByteBuffer column,
                            ByteBuffer expectedValue,
                            StoreTransaction txh) throws StorageException {
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

    /**
     * Convert deletions to a Delete command.
     *
     * @param cfName The name of the ColumnFamily deletions belong to
     * @param key The row key
     * @param deletions The name of the columns to delete (a.k.a deletions)
     *
     * @return Delete command or null if deletions were null or empty.
     */
    private final static Delete makeDeletionCommand(byte[] cfName, byte[] key, List<ByteBuffer> deletions) {
        if (deletions == null || deletions.size() == 0)
            return null;

        Delete deleteCommand = new Delete(key);

        for (ByteBuffer del : deletions) {
            deleteCommand.deleteColumn(cfName, ByteBufferUtil.getArray(del));
        }

        return deleteCommand;
    }

    /**
     * Convert modification entries into Put command.
     *
     * @param cfName The name of the ColumnFamily modifications belong to
     * @param key The row key
     * @param modifications The entries to insert/update.
     *
     * @return Put command or null if additions were null or empty.
     */
    private final static Put makePutCommand(byte[] cfName, byte[] key, List<Entry> modifications) {
        if (modifications == null || modifications.size() == 0)
            return null;

        Put putCommand = new Put(key);

        for (Entry e : modifications) {
            putCommand.add(cfName, ByteBufferUtil.getArray(e.getColumn()), ByteBufferUtil.getArray(e.getValue()));
        }

        return putCommand;
    }

    public final static List<Row> makeBatch(byte[] cfName, byte[] key, List<Entry> additions, List<ByteBuffer> deletions) {
        Put putCommand = makePutCommand(cfName, key, additions);
        Delete deleteCommand = makeDeletionCommand(cfName, key, deletions);

        if (putCommand == null && deleteCommand == null)
            return Collections.emptyList();

        List<Row> batch = new ArrayList<Row>(2);

        if (putCommand != null)
            batch.add(putCommand);

        if (deleteCommand != null)
            batch.add(deleteCommand);

        return batch;
    }
}
