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

package org.janusgraph.diskstorage.cassandra.astyanax;

import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.*;
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.cassandra.utils.CassandraHelper;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.*;

import static org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager.Partitioner;
import static org.janusgraph.diskstorage.cassandra.CassandraTransaction.getTx;

public class AstyanaxKeyColumnValueStore implements KeyColumnValueStore {

    private final Keyspace keyspace;
    private final String columnFamilyName;
    private final ColumnFamily<ByteBuffer, ByteBuffer> columnFamily;
    private final RetryPolicy retryPolicy;
    private final AstyanaxStoreManager storeManager;
    private final AstyanaxGetter entryGetter;

    AstyanaxKeyColumnValueStore(String columnFamilyName,
                                Keyspace keyspace,
                                AstyanaxStoreManager storeManager,
                                RetryPolicy retryPolicy) {
        this.keyspace = keyspace;
        this.columnFamilyName = columnFamilyName;
        this.retryPolicy = retryPolicy;
        this.storeManager = storeManager;

        entryGetter = new AstyanaxGetter(storeManager.getMetaDataSchema(columnFamilyName));

        columnFamily = new ColumnFamily<ByteBuffer, ByteBuffer>(
                this.columnFamilyName,
                ByteBufferSerializer.get(),
                ByteBufferSerializer.get());

    }


    ColumnFamily<ByteBuffer, ByteBuffer> getColumnFamily() {
        return columnFamily;
    }

    @Override
    public void close() throws BackendException {
        //Do nothing
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer, EntryList> result = getNamesSlice(query.getKey(), query, txh);
        return Iterables.getOnlyElement(result.values(),EntryList.EMPTY_LIST);
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        return getNamesSlice(keys, query, txh);
    }

    public Map<StaticBuffer, EntryList> getNamesSlice(StaticBuffer key,
                                                    SliceQuery query, StoreTransaction txh) throws BackendException {
        return getNamesSlice(ImmutableList.of(key),query,txh);
    }


    public Map<StaticBuffer, EntryList> getNamesSlice(List<StaticBuffer> keys,
                                                      SliceQuery query, StoreTransaction txh) throws BackendException {
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
                .setConsistencyLevel(getTx(txh).getReadConsistencyLevel().getAstyanax())
                .withRetryPolicy(retryPolicy.duplicate())
                .getKeySlice(CassandraHelper.convert(keys));

        // Thank you, Astyanax, for making builder pattern useful :(
        rq.withColumnRange(query.getSliceStart().asByteBuffer(),
                query.getSliceEnd().asByteBuffer(),
                false,
                query.getLimit() + (query.hasLimit()?1:0)); //Add one for potentially removed last column

        OperationResult<Rows<ByteBuffer, ByteBuffer>> r;
        try {
            r = (OperationResult<Rows<ByteBuffer, ByteBuffer>>) rq.execute();
        } catch (ConnectionException e) {
            throw new TemporaryBackendException(e);
        }

        Rows<ByteBuffer,ByteBuffer> rows = r.getResult();
        Map<StaticBuffer, EntryList> result = new HashMap<StaticBuffer, EntryList>(rows.size());

        for (Row<ByteBuffer, ByteBuffer> row : rows) {
            assert !result.containsKey(row.getKey());
            result.put(StaticArrayBuffer.of(row.getKey()),
                  CassandraHelper.makeEntryList(row.getColumns(),entryGetter, query.getSliceEnd(), query.getLimit()));
        }

        return result;
    }

    private static class AstyanaxGetter implements StaticArrayEntry.GetColVal<Column<ByteBuffer>,ByteBuffer> {

        private final EntryMetaData[] schema;

        private AstyanaxGetter(EntryMetaData[] schema) {
            this.schema = schema;
        }


        @Override
        public ByteBuffer getColumn(Column<ByteBuffer> element) {
            return element.getName();
        }

        @Override
        public ByteBuffer getValue(Column<ByteBuffer> element) {
            return element.getByteBufferValue();
        }

        @Override
        public EntryMetaData[] getMetaSchema(Column<ByteBuffer> element) {
            return schema;
        }

        @Override
        public Object getMetaData(Column<ByteBuffer> element, EntryMetaData meta) {
            switch(meta) {
                case TIMESTAMP:
                    return element.getTimestamp();
                case TTL:
                    return element.getTtl();
                default:
                    throw new UnsupportedOperationException("Unsupported meta data: " + meta);
            }
        }
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        mutateMany(ImmutableMap.of(key, new KCVMutation(additions, deletions)), txh);
    }

    public void mutateMany(Map<StaticBuffer, KCVMutation> mutations, StoreTransaction txh) throws BackendException {
        storeManager.mutateMany(ImmutableMap.of(columnFamilyName, mutations), txh);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(@Nullable SliceQuery sliceQuery, StoreTransaction txh) throws BackendException {
        if (storeManager.getPartitioner() != Partitioner.RANDOM)
            throw new PermanentBackendException("This operation is only allowed when random partitioner (md5 or murmur3) is used.");

        AllRowsQuery allRowsQuery = keyspace.prepareQuery(columnFamily).getAllRows();

        if (sliceQuery != null) {
            allRowsQuery.withColumnRange(sliceQuery.getSliceStart().asByteBuffer(),
                    sliceQuery.getSliceEnd().asByteBuffer(),
                    false,
                    sliceQuery.getLimit());
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
            throw new PermanentBackendException(e);
        }

        return new RowIterator(result.iterator(), sliceQuery);
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        // this query could only be done when byte-ordering partitioner is used
        // because Cassandra operates on tokens internally which means that even contiguous
        // range of keys (e.g. time slice) with random partitioner could produce disjoint set of tokens
        // returning ambiguous results to the user.
        Partitioner partitioner = storeManager.getPartitioner();
        if (partitioner != Partitioner.BYTEORDER)
            throw new PermanentBackendException("getKeys(KeyRangeQuery could only be used with byte-ordering partitioner.");

        ByteBuffer start = query.getKeyStart().asByteBuffer(), end = query.getKeyEnd().asByteBuffer();

        RowSliceQuery rowSlice = keyspace.prepareQuery(columnFamily)
                .setConsistencyLevel(getTx(txh).getReadConsistencyLevel().getAstyanax())
                .withRetryPolicy(retryPolicy.duplicate())
                .getKeyRange(start, end, null, null, Integer.MAX_VALUE);

        // Astyanax is bad at builder pattern :(
        rowSlice.withColumnRange(query.getSliceStart().asByteBuffer(),
                query.getSliceEnd().asByteBuffer(),
                false,
                query.getLimit());

        // Omit final the query's keyend from the result, if present in result
        final Rows<ByteBuffer, ByteBuffer> r;
        try {
            r = ((OperationResult<Rows<ByteBuffer, ByteBuffer>>) rowSlice.execute()).getResult();
        } catch (ConnectionException e) {
            throw new TemporaryBackendException(e);
        }
        Iterator<Row<ByteBuffer, ByteBuffer>> i =
                Iterators.filter(r.iterator(), new KeySkipPredicate(query.getKeyEnd().asByteBuffer()));
        return new RowIterator(i, query);
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

    private static class KeySkipPredicate implements Predicate<Row<ByteBuffer, ByteBuffer>> {

        private final ByteBuffer skip;

        public KeySkipPredicate(ByteBuffer skip) {
            this.skip = skip;
        }

        @Override
        public boolean apply(@Nullable Row<ByteBuffer, ByteBuffer> row) {
            return (row != null) && !row.getKey().equals(skip);
        }
    }

    private class RowIterator implements KeyIterator {
        private final Iterator<Row<ByteBuffer, ByteBuffer>> rows;
        private Row<ByteBuffer, ByteBuffer> currentRow;
        private final SliceQuery sliceQuery;
        private boolean isClosed;

        public RowIterator(Iterator<Row<ByteBuffer, ByteBuffer>> rowIter, SliceQuery sliceQuery) {
            this.rows = Iterators.filter(rowIter, new KeyIterationPredicate());
            this.sliceQuery = sliceQuery;
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            if (sliceQuery == null)
                throw new IllegalStateException("getEntries() requires SliceQuery to be set.");

            return new RecordIterator<Entry>() {
                private final Iterator<Entry> columns =
                        CassandraHelper.makeEntryIterator(currentRow.getColumns(),
                                entryGetter,
                                sliceQuery.getSliceEnd(),sliceQuery.getLimit());

                @Override
                public boolean hasNext() {
                    ensureOpen();
                    return columns.hasNext();
                }

                @Override
                public Entry next() {
                    ensureOpen();
                    return columns.next();
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
            return StaticArrayBuffer.of(currentRow.getKey());
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
