package com.thinkaurelius.titan.diskstorage.hbase;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
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
        return getHelper(query.getKey(), getFilter(query));
    }

    public static Filter getFilter(SliceQuery query) {
        byte[] colStartBytes = query.getSliceEnd().length()>0 ? query.getSliceStart().as(StaticBuffer.ARRAY_FACTORY) : null;
        byte[] colEndBytes = query.getSliceEnd().length()>0 ? query.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY) : null;

        Filter filter = new ColumnRangeFilter(colStartBytes, true, colEndBytes, false);

        if (query.hasLimit()) {
            filter = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                    filter,
                    new ColumnPaginationFilter(query.getLimit(), 0));
        }

        return filter;
    }

    private List<Entry> getHelper(StaticBuffer key, Filter getFilter) throws StorageException {
        byte[] keyBytes = key.as(StaticBuffer.ARRAY_FACTORY);

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
                    ret.add(StaticBufferEntry.of(new StaticArrayBuffer(ent.getKey()), new StaticArrayBuffer(ent.getValue())));
                }
            }

            return ret;
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        }
    }

    @Override
    public void mutate(StaticBuffer key,
                       List<Entry> additions,
                       List<StaticBuffer> deletions,
                       StoreTransaction txh) throws StorageException {
        // TODO: use RowMutations (requires 0.94.x-ish HBase), error handling through the legacy batch() method sucks
        List<Row> batch = makeBatch(columnFamilyBytes, key.as(StaticBuffer.ARRAY_FACTORY), additions, deletions);

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
    public void acquireLock(StaticBuffer key,
                            StaticBuffer column,
                            StaticBuffer expectedValue,
                            StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    /**
     *
     * IMPORTANT: Makes the assumption that all keys are 8 byte longs
     *
     * @param txh
     * @return
     * @throws StorageException
     */
    @Override
    public RecordIterator<StaticBuffer> getKeys(StoreTransaction txh) throws StorageException {
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

        return new RecordIterator<StaticBuffer>() {
            /* we need to check if key is long serializable because HBase returns weird rows sometimes */
            private final Iterator<Result> results = Iterators.filter(scanner.iterator(), new Predicate<Result>() {
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

            @Override
            public boolean hasNext() throws StorageException {
                return results.hasNext();
            }

            @Override
            public StaticBuffer next() throws StorageException {
                return new StaticArrayBuffer(results.next().getRow());
            }

            @Override
            public void close() throws StorageException {
                scanner.close();
            }
        };
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
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
    private final static Delete makeDeletionCommand(byte[] cfName, byte[] key, List<StaticBuffer> deletions) {
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
     * @param cfName The name of the ColumnFamily modifications belong to
     * @param key The row key
     * @param modifications The entries to insert/update.
     *
     * @return Put command or null if additions were null or empty.
     */
    private final static Put makePutCommand(byte[] cfName, byte[] key, List<Entry> modifications) {
        Preconditions.checkArgument(!modifications.isEmpty());

        Put putCommand = new Put(key);
        for (Entry e : modifications) {
            putCommand.add(cfName, e.getArrayColumn(), e.getArrayValue());
        }
        return putCommand;
    }

    public final static List<Row> makeBatch(byte[] cfName, byte[] key, List<Entry> additions, List<StaticBuffer> deletions) {
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
}
