package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;

public class CassandraEmbeddedKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger log = LoggerFactory.getLogger(CassandraEmbeddedKeyColumnValueStore.class);

    private final String keyspace;
    private final String columnFamily;
    private final CassandraEmbeddedStoreManager storeManager;


    public CassandraEmbeddedKeyColumnValueStore(
            String keyspace,
            String columnFamily,
            CassandraEmbeddedStoreManager storeManager) throws RuntimeException {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.storeManager = storeManager;
    }

    @Override
    public void close() throws StorageException {
    }

    static ByteBuffer getInternal(String keyspace,
                                  String columnFamily,
                                  ByteBuffer key,
                                  ByteBuffer column,
                                  org.apache.cassandra.db.ConsistencyLevel cl) throws StorageException {

        QueryPath slicePath = new QueryPath(columnFamily);

        SliceByNamesReadCommand namesCmd = new SliceByNamesReadCommand(
                keyspace, key.duplicate(), slicePath, Arrays.asList(column.duplicate()));

        List<Row> rows = read(namesCmd, cl);

        if (null == rows || 0 == rows.size())
            return null;

        if (1 < rows.size())
            throw new PermanentStorageException("Received " + rows.size()
                    + " rows from a single-key-column cassandra read");

        assert 1 == rows.size();

        Row r = rows.get(0);

        if (null == r) {
            log.warn("Null Row object retrieved from Cassandra StorageProxy");
            return null;
        }

        ColumnFamily cf = r.cf;
        if (null == cf)
            return null;

        if (cf.isMarkedForDelete())
            return null;

        IColumn c = cf.getColumn(column.duplicate());
        if (null == c)
            return null;

        // These came up during testing
        if (c.isMarkedForDelete())
            return null;

        return org.apache.cassandra.utils.ByteBufferUtil.clone(c.value());
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column,
                            StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery keyRangeQuery, StoreTransaction txh) throws StorageException {
        IPartitioner partitioner = StorageService.getPartitioner();

        // see rant about this in Astyanax implementation
        if (partitioner instanceof RandomPartitioner || partitioner instanceof Murmur3Partitioner)
            throw new PermanentStorageException("This operation is only supported when byte-ordered partitioner is used.");

        return new RowIterator(keyRangeQuery, storeManager.getPageSize());
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws StorageException {
        return new RowIterator(getMinimumToken(), getMaximumToken(), query, storeManager.getPageSize());
    }

    private List<Row> getKeySlice(Token start,
                                  Token end,
                                  @Nullable SliceQuery sliceQuery,
                                  int pageSize) throws StorageException {
        IPartitioner<?> partitioner = StorageService.getPartitioner();

        SliceRange columnSlice = new SliceRange();
        if (sliceQuery == null) {
            columnSlice.setStart(ArrayUtils.EMPTY_BYTE_ARRAY)
                    .setFinish(ArrayUtils.EMPTY_BYTE_ARRAY)
                    .setCount(5);
        } else {
            columnSlice.setStart(sliceQuery.getSliceStart().asByteBuffer())
                    .setFinish(sliceQuery.getSliceEnd().asByteBuffer())
                    .setCount(sliceQuery.hasLimit() ? sliceQuery.getLimit() : Integer.MAX_VALUE);
        }
        /* Note: we need to fetch columns for each row as well to remove "range ghosts" */
        SlicePredicate predicate = new SlicePredicate().setSlice_range(columnSlice);

        RowPosition startPosition = start.minKeyBound(partitioner);
        RowPosition endPosition = end.minKeyBound(partitioner);

        List<Row> rows;

        try {
            IDiskAtomFilter filter = ThriftValidation.asIFilter(predicate, Schema.instance.getComparator(keyspace, columnFamily));

            rows = StorageProxy.getRangeSlice(new RangeSliceCommand(keyspace,
                    new ColumnParent(columnFamily),
                    filter,
                    new Bounds<RowPosition>(startPosition, endPosition),
                    null,
                    pageSize), ConsistencyLevel.QUORUM);
        } catch (Exception e) {
            throw new PermanentStorageException(e);
        }

        return rows;
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        return storeManager.getLocalKeyPartition();
    }


    @Override
    public String getName() {
        return columnFamily;
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {

        QueryPath slicePath = new QueryPath(columnFamily);
        // TODO key.asByteBuffer() may entail an unnecessary buffer copy
        ReadCommand sliceCmd = new SliceFromReadCommand(
                keyspace,          // Keyspace name
                key.asByteBuffer(),// Row key
                slicePath,         // ColumnFamily
                ByteBufferUtil.EMPTY_BYTE_BUFFER, // Start column name (empty means begin at first result)
                ByteBufferUtil.EMPTY_BYTE_BUFFER, // End column name (empty means max out the count)
                false,             // Reverse results? (false=no)
                1);                // Max count of Columns to return

        List<Row> rows = read(sliceCmd, getTx(txh).getReadConsistencyLevel().getDBConsistency());

        if (null == rows || 0 == rows.size())
            return false;
        
        /*
         * Find at least one live column
		 * 
		 * Note that the rows list may contain arbitrarily many
		 * marked-for-delete elements. Therefore, we can't assume that we're
		 * dealing with a singleton even though we set the maximum column count
		 * to 1.
		 */
        for (Row r : rows) {
            if (null == r || null == r.cf)
                continue;

            if (r.cf.isMarkedForDelete())
                continue;

            for (IColumn ic : r.cf)
                if (!ic.isMarkedForDelete())
                    return true;
        }

        return false;
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {

        QueryPath slicePath = new QueryPath(columnFamily);
        ReadCommand sliceCmd = new SliceFromReadCommand(
                keyspace,                      // Keyspace name
                query.getKey().asByteBuffer(), // Row key
                slicePath,                     // ColumnFamily
                query.getSliceStart().asByteBuffer(),  // Start column name (empty means begin at first result)
                query.getSliceEnd().asByteBuffer(),   // End column name (empty means max out the count)
                false,                         // Reverse results? (false=no)
                query.getLimit());             // Max count of Columns to return

        List<Row> slice = read(sliceCmd, getTx(txh).getReadConsistencyLevel().getDBConsistency());

        if (null == slice || 0 == slice.size())
            return new ArrayList<Entry>(0);

        int sliceSize = slice.size();
        if (1 < sliceSize)
            throw new PermanentStorageException("Received " + sliceSize + " rows for single key");

        Row r = slice.get(0);

        if (null == r) {
            log.warn("Null Row object retrieved from Cassandra StorageProxy");
            return new ArrayList<Entry>(0);
        }

        ColumnFamily cf = r.cf;

        if (null == cf) {
            log.debug("null ColumnFamily (\"{}\")", columnFamily);
            return new ArrayList<Entry>(0);
        }

        if (cf.isMarkedForDelete())
            return new ArrayList<Entry>(0);

        return cfToEntries(cf, query.getSliceEnd());
    }

    @Override
    public List<List<Entry>> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions,
                       List<StaticBuffer> deletions, StoreTransaction txh) throws StorageException {
        Map<StaticBuffer, KCVMutation> mutations = ImmutableMap.of(key, new
                KCVMutation(additions, deletions));
        mutateMany(mutations, txh);
    }


    public void mutateMany(Map<StaticBuffer, KCVMutation> mutations,
                           StoreTransaction txh) throws StorageException {
        storeManager.mutateMany(ImmutableMap.of(columnFamily, mutations), txh);
    }

    private static List<Row> read(ReadCommand cmd, org.apache.cassandra.db.ConsistencyLevel clvl) throws StorageException {
        ArrayList<ReadCommand> cmdHolder = new ArrayList<ReadCommand>(1);
        cmdHolder.add(cmd);
        return read(cmdHolder, clvl);
    }

    private static List<Row> read(List<ReadCommand> cmds, org.apache.cassandra.db.ConsistencyLevel clvl) throws StorageException {
        try {
            return StorageProxy.read(cmds, clvl);
        } catch (IOException e) {
            throw new PermanentStorageException(e);
        } catch (UnavailableException e) {
            throw new TemporaryStorageException(e);
        } catch (RequestTimeoutException e) {
            throw new PermanentStorageException(e);
        } catch (IsBootstrappingException e) {
            throw new TemporaryStorageException(e);
        }
    }


    private List<Entry> cfToEntries(ColumnFamily cf,
                                    StaticBuffer columnEnd) throws StorageException {

        assert !cf.isMarkedForDelete();

        Collection<IColumn> columns = cf.getSortedColumns();
        List<Entry> result = new ArrayList<Entry>(columns.size());

        /*
         * We want to call columnEnd.equals() on column name ByteBuffers in the
         * loop below. But columnEnd is a StaticBuffer, and it doesn't have an
         * equals() method that accepts ByteBuffer. We create a ByteBuffer copy
         * of columnEnd just for equals() comparisons in the for loop below.
         * 
         * TODO remove this if StaticBuffer's equals() accepts ByteBuffer
         */
        ByteBuffer columnEndBB = columnEnd.asByteBuffer();

        // Populate Entries into return collection
        for (IColumn icol : columns) {
            if (null == icol)
                throw new PermanentStorageException("Unexpected null IColumn");

            if (icol.isMarkedForDelete())
                continue;

            ByteBuffer name = org.apache.cassandra.utils.ByteBufferUtil.clone(icol.name());
            ByteBuffer value = org.apache.cassandra.utils.ByteBufferUtil.clone(icol.value());

            if (columnEndBB.equals(name))
                continue;

            result.add(new ByteBufferEntry(name, value));
        }

        return result;
    }

    private class RowIterator implements KeyIterator {
        private final Token maximumToken;
        private final SliceQuery sliceQuery;

        private Iterator<Row> keys;
        private ByteBuffer lastSeenKey = null;
        private Row currentRow;
        private int pageSize;

        private boolean isClosed;

        public RowIterator(KeyRangeQuery keyRangeQuery, int pageSize) throws StorageException {
            this(StorageService.getPartitioner().getToken(keyRangeQuery.getKeyStart().asByteBuffer()),
                    StorageService.getPartitioner().getToken(keyRangeQuery.getKeyEnd().asByteBuffer()),
                    keyRangeQuery,
                    pageSize);
        }

        public RowIterator(Token minimum, Token maximum, SliceQuery sliceQuery, int pageSize) throws StorageException {
            this.keys = getRowsIterator(getKeySlice(minimum, maximum, sliceQuery, pageSize));
            this.pageSize = pageSize;
            this.sliceQuery = sliceQuery;
            this.maximumToken = maximum;
        }

        @Override
        public boolean hasNext() {
            try {
                return hasNextInternal();
            } catch (StorageException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public StaticBuffer next() {
            ensureOpen();

            if (!hasNext())
                throw new NoSuchElementException();

            currentRow = keys.next();
            ByteBuffer currentKey = currentRow.key.key.duplicate();

            try {
                return new StaticByteBuffer(currentKey);
            } finally {
                lastSeenKey = currentKey;
            }
        }

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            if (sliceQuery == null)
                throw new IllegalStateException("getEntries() requires SliceQuery to be set.");

            try {
                return new RecordIterator<Entry>() {
                    final Iterator<Entry> columns = cfToEntries(currentRow.cf, sliceQuery.getSliceEnd()).iterator();

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
            } catch (StorageException e) {
                throw new IllegalStateException(e);
            }
        }

        private final boolean hasNextInternal() throws StorageException {
            ensureOpen();

            if (keys == null)
                return false;

            boolean hasNext = keys.hasNext();

            if (!hasNext && lastSeenKey != null) {
                Token lastSeenToken = StorageService.getPartitioner().getToken(lastSeenKey.duplicate());

                // let's check if we reached key upper bound already so we can skip one useless call to Cassandra
                if (maximumToken != getMinimumToken() && lastSeenToken.equals(maximumToken)) {
                    return false;
                }

                List<Row> newKeys = getKeySlice(StorageService.getPartitioner().getToken(lastSeenKey), maximumToken, sliceQuery, pageSize);

                keys = getRowsIterator(newKeys, lastSeenKey);
                hasNext = keys.hasNext();
            }

            return hasNext;
        }

        private void ensureOpen() {
            if (isClosed)
                throw new IllegalStateException("Iterator has been closed.");
        }

        private Iterator<Row> getRowsIterator(List<Row> rows) {
            if (rows == null)
                return null;

            return Iterators.filter(rows.iterator(), new Predicate<Row>() {
                @Override
                public boolean apply(@Nullable Row row) {
                    return !(row == null || row.cf == null || row.cf.isMarkedForDelete() || row.cf.hasOnlyTombstones());
                }
            });
        }

        private Iterator<Row> getRowsIterator(List<Row> rows, final ByteBuffer exceptKey) {
            Iterator<Row> rowIterator = getRowsIterator(rows);

            if (rowIterator == null)
                return null;

            return Iterators.filter(rowIterator, new Predicate<Row>() {
                @Override
                public boolean apply(@Nullable Row row) {
                    return row != null && !row.key.key.equals(exceptKey);
                }
            });
        }
    }

    private static Token getMinimumToken() throws PermanentStorageException {
        IPartitioner partitioner = StorageService.getPartitioner();

        if (partitioner instanceof RandomPartitioner) {
            return ((RandomPartitioner) partitioner).getMinimumToken();
        } else if (partitioner instanceof Murmur3Partitioner) {
            return ((Murmur3Partitioner) partitioner).getMinimumToken();
        } else if (partitioner instanceof ByteOrderedPartitioner) {
            //TODO: This makes the assumption that its an EdgeStore (i.e. 8 byte keys)
            return new BytesToken(com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil.zeroByteBuffer(8));
        } else {
            throw new PermanentStorageException("Unsupported partitioner: " + partitioner);
        }
    }

    private static Token getMaximumToken() throws PermanentStorageException {
        IPartitioner partitioner = StorageService.getPartitioner();

        if (partitioner instanceof RandomPartitioner) {
            return new BigIntegerToken(RandomPartitioner.MAXIMUM);
        } else if (partitioner instanceof Murmur3Partitioner) {
            return new LongToken(Murmur3Partitioner.MAXIMUM);
        } else if (partitioner instanceof ByteOrderedPartitioner) {
            //TODO: This makes the assumption that its an EdgeStore (i.e. 8 byte keys)
            return new BytesToken(com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil.oneByteBuffer(8));
        } else {
            throw new PermanentStorageException("Unsupported partitioner: " + partitioner);
        }
    }
}
