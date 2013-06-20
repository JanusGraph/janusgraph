package com.thinkaurelius.titan.diskstorage.cassandra.astyanax;

import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.*;
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.*;

import static com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager.Partitioner;
import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;

public class AstyanaxOrderedKeyColumnValueStore implements KeyColumnValueStore {

    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);


    private final Keyspace keyspace;
    private final String columnFamilyName;
    private final ColumnFamily<ByteBuffer, ByteBuffer> columnFamily;
    private final RetryPolicy retryPolicy;
    private final AstyanaxStoreManager storeManager;


    AstyanaxOrderedKeyColumnValueStore(String columnFamilyName,
                                       Keyspace keyspace,
                                       AstyanaxStoreManager storeManager,
                                       RetryPolicy retryPolicy) {
        this.keyspace = keyspace;
        this.columnFamilyName = columnFamilyName;
        this.retryPolicy = retryPolicy;
        this.storeManager = storeManager;

        columnFamily = new ColumnFamily<ByteBuffer, ByteBuffer>(
                this.columnFamilyName,
                ByteBufferSerializer.get(),
                ByteBufferSerializer.get());
    }


    ColumnFamily<ByteBuffer, ByteBuffer> getColumnFamily() {
        return columnFamily;
    }

    @Override
    public void close() throws StorageException {
        //Do nothing
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        try {
            // See getSlice() below for a warning suppression justification
            @SuppressWarnings("rawtypes")
            RowQuery rq = (RowQuery) keyspace.prepareQuery(columnFamily)
                    .withRetryPolicy(retryPolicy.duplicate())
                    .setConsistencyLevel(getTx(txh).getReadConsistencyLevel().getAstyanaxConsistency())
                    .getKey(key.asByteBuffer());
            @SuppressWarnings("unchecked")
            OperationResult<ColumnList<ByteBuffer>> r = rq.withColumnRange(EMPTY, EMPTY, false, 1).execute();
            return 0 < r.getResult().size();
        } catch (ConnectionException e) {
            throw new TemporaryStorageException(e);
        }
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        ByteBuffer key = query.getKey().asByteBuffer();
        List<Entry> slice = getNamesSlice(Arrays.asList(query.getKey()), query, txh).get(key.duplicate());
        return (slice == null) ? Collections.<Entry>emptyList() : slice;
    }

    @Override
    public List<List<Entry>> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
        return Lists.newArrayList(getNamesSlice(keys, query, txh).values());
    }

    public Map<ByteBuffer, List<Entry>> getNamesSlice(List<StaticBuffer> keys,
                                                      SliceQuery query,
                                                      StoreTransaction txh) throws StorageException {
        ByteBuffer[] requestKeys = new ByteBuffer[keys.size()];
        {
            for (int i = 0; i < keys.size(); i++) {
                requestKeys[i] = keys.get(i).asByteBuffer();
            }
        }

        /*
         * RowQuery<K,C> should be parameterized as
         * RowQuery<ByteBuffer,ByteBuffer>. However, this causes the following
         * compilation error when attempting to call withColumnRange on a
         * RowQuery<ByteBuffer,ByteBuffer> instance:
         *
         * java.lang.Error: Unresolved compilation problem: The method
         * withColumnRange(ByteBuffer, ByteBuffer, boolean, int) is ambiguous
         * for the type RowQuery<ByteBuffer,ByteBuffer>
         *
         * The compiler substitutes ByteBuffer=C for both startColumn and
         * endColumn, compares it to its identical twin with that type
         * hard-coded, and dies.
         *
         */
        RowSliceQuery rq = keyspace.prepareQuery(columnFamily)
                                   .setConsistencyLevel(getTx(txh).getReadConsistencyLevel().getAstyanaxConsistency())
                                   .withRetryPolicy(retryPolicy.duplicate())
                                   .getKeySlice(requestKeys);

        // Thank you, Astyanax, for making builder pattern useful :(
        int limit = ((query.hasLimit()) ? query.getLimit() : Integer.MAX_VALUE - 1);
        rq.withColumnRange(query.getSliceStart().asByteBuffer(),
                           query.getSliceEnd().asByteBuffer(),
                           false,
                           limit + 1);

        OperationResult<Rows<ByteBuffer, ByteBuffer>> r;
        try {
            r = (OperationResult<Rows<ByteBuffer, ByteBuffer>>) rq.execute();
        } catch (ConnectionException e) {
            throw new TemporaryStorageException(e);
        }

        return convertResult(r.getResult(), query.getSliceEnd().asByteBuffer(), limit);
    }

    private Map<ByteBuffer, List<Entry>> convertResult(Rows<ByteBuffer, ByteBuffer> rows, ByteBuffer lastColumn, int limit) {
        Map<ByteBuffer, List<Entry>> result = new HashMap<ByteBuffer, List<Entry>>();

        for (Row<ByteBuffer, ByteBuffer> row : rows) {
            assert result.get(row.getKey()) == null;

            int i = 0;
            List<Entry> entries = new ArrayList<Entry>();

            for (Column<ByteBuffer> c : row.getColumns()) {
                ByteBuffer colName = c.getName();

                // Cassandra treats the end of a slice column range inclusively, but
                // this method's contract promises to treat it exclusively. Check
                // for the final column in the Cassandra results and skip it if
                // found.
                if (colName.equals(lastColumn)) {
                    break;
                }

                entries.add(new ByteBufferEntry(colName, c.getByteBufferValue()));

                if (++i == limit) {
                    break;
                }
            }

            result.put(row.getKey().duplicate(), entries);
        }

        return result;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws StorageException {
        mutateMany(ImmutableMap.of(key, new KCVMutation(additions, deletions)), txh);
    }

    public void mutateMany(Map<StaticBuffer, KCVMutation> mutations, StoreTransaction txh) throws StorageException {
        storeManager.mutateMany(ImmutableMap.of(columnFamilyName, mutations), txh);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordIterator<StaticBuffer> getKeys(StoreTransaction txh) throws StorageException {
        return getKeys((SliceQuery) null, txh);
    }

    @Override
    public KeyIterator getKeys(@Nullable SliceQuery sliceQuery, StoreTransaction txh) throws StorageException {
        if (storeManager.getPartitioner() != Partitioner.RANDOM)
            throw new PermanentStorageException("This operation is only allowed when random partitioner (md5 or murmur3) is used.");

        AllRowsQuery allRowsQuery = keyspace.prepareQuery(columnFamily).getAllRows();

        if (sliceQuery != null) {
            int limit = (sliceQuery.hasLimit()) ? sliceQuery.getLimit() : Integer.MAX_VALUE;
            allRowsQuery.withColumnRange(sliceQuery.getSliceStart().asByteBuffer(),
                                         sliceQuery.getSliceEnd().asByteBuffer(),
                                         false,
                                         limit);
        }

        Rows<ByteBuffer, ByteBuffer> result;
        try {
            /* Note: we need to fetch columns for each row as well to remove "range ghosts" */
            OperationResult op = allRowsQuery.setRowLimit(storeManager.getPageSize()) // pre-fetch that many rows at a time
                                             .setConcurrencyLevel(1) // one execution thread for fetching portion of rows
                                             .setExceptionCallback(new ExceptionCallback() {
                                                 private int retries = 0;

                                                 @Override
                                                 public boolean onException(ConnectionException e) {
                                                     try {
                                                         return retries > 2; // make 3 re-tries
                                                     } finally {
                                                         retries++;
                                                     }
                                                 }
                                             }).execute();

            result = ((OperationResult<Rows<ByteBuffer, ByteBuffer>>) op).getResult();
        } catch (ConnectionException e) {
            throw new PermanentStorageException(e);
        }

        return new RowIterator(result);
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws StorageException {
        // this query could only be done when byte-ordering partitioner is used
        // because Cassandra operates on tokens internally which means that even contiguous
        // range of keys (e.g. time slice) with random partitioner could produce disjoint set of tokens
        // returning ambiguous results to the user.
        Partitioner partitioner = storeManager.getPartitioner();
        if (partitioner != Partitioner.BYTEORDER && partitioner != Partitioner.LOCALBYTEORDER)
            throw new PermanentStorageException("getKeys(KeyRangeQuery could only be used with byte-ordering partitioner.");

        ByteBuffer start = query.getKeyStart().asByteBuffer(), end = query.getKeyEnd().asByteBuffer();
        int limit = (query.hasLimit()) ? query.getLimit() : Integer.MAX_VALUE;

        RowSliceQuery rowSlice = keyspace.prepareQuery(columnFamily)
                                         .setConsistencyLevel(getTx(txh).getReadConsistencyLevel().getAstyanaxConsistency())
                                         .withRetryPolicy(retryPolicy.duplicate())
                                         .getKeyRange(start, end, null, null, Integer.MAX_VALUE);

        // Astyanax is bad at builder pattern :(
        rowSlice.withColumnRange(query.getSliceStart().asByteBuffer(),
                                 query.getSliceEnd().asByteBuffer(),
                                 false,
                                 limit);

        try {
            return new RowIterator(((OperationResult<Rows<ByteBuffer, ByteBuffer>>) rowSlice.execute()).getResult());
        } catch (ConnectionException e) {
            throw new TemporaryStorageException(e);
        }
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return columnFamilyName;
    }

    private static class KeyIterationPredicate implements Predicate<Row<ByteBuffer, ByteBuffer>> {
        @Override
        public boolean apply(@Nullable Row<ByteBuffer, ByteBuffer> row) {
            return (row != null) && row.getColumns().size() > 0;
        }
    }

    private static class RowIterator implements KeyIterator {
        private final Iterator<Row<ByteBuffer, ByteBuffer>> rows;
        private Row<ByteBuffer, ByteBuffer> currentRow;
        private boolean isClosed;

        public RowIterator(Rows<ByteBuffer, ByteBuffer> rows) {
            this.rows = Iterators.filter(rows.iterator(), new KeyIterationPredicate());
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            return new RecordIterator<Entry>() {
                private final Iterator<Column<ByteBuffer>> columns = currentRow.getColumns().iterator();

                @Override
                public boolean hasNext() throws StorageException {
                    ensureOpen();
                    return columns.hasNext();
                }

                @Override
                public Entry next() throws StorageException {
                    ensureOpen();

                    Column<ByteBuffer> column = columns.next();
                    return new ByteBufferEntry(column.getName(), column.getByteBufferValue());
                }

                @Override
                public void close() throws StorageException {
                    isClosed = true;
                }
            };
        }

        @Override
        public boolean hasNext() throws StorageException {
            ensureOpen();
            return rows.hasNext();
        }

        @Override
        public StaticBuffer next() throws StorageException {
            ensureOpen();

            currentRow = rows.next();
            return new StaticByteBuffer(currentRow.getKey());
        }

        @Override
        public void close() throws StorageException {
            isClosed = true;
        }

        private void ensureOpen() {
            if (isClosed)
                throw new IllegalStateException("Iterator has been closed.");
        }
    }
}
