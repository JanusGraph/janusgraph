package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.cassandra.utils.CassandraHelper;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
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
    public List<KeyRange> getLocalKeyPartition() throws StorageException {
        return storeManager.getLocalKeyPartition();
    }


    @Override
    public String getName() {
        return columnFamily;
    }

    //TODO: remove
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

        List<Row> rows = read(sliceCmd, getTx(txh).getReadConsistencyLevel().getDB());

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
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {

        QueryPath slicePath = new QueryPath(columnFamily);
        ReadCommand sliceCmd = new SliceFromReadCommand(
                keyspace,                      // Keyspace name
                query.getKey().asByteBuffer(), // Row key
                slicePath,                     // ColumnFamily
                query.getSliceStart().asByteBuffer(),  // Start column name (empty means begin at first result)
                query.getSliceEnd().asByteBuffer(),   // End column name (empty means max out the count)
                false,                         // Reverse results? (false=no)
                query.getLimit() + (query.hasLimit()?1:0));             // Max count of Columns to return, add 1 in case of limit since we might have to filter one out at the end

        List<Row> slice = read(sliceCmd, getTx(txh).getReadConsistencyLevel().getDB());

        if (null == slice || 0 == slice.size())
            return EntryList.EMPTY_LIST;

        int sliceSize = slice.size();
        if (1 < sliceSize)
            throw new PermanentStorageException("Received " + sliceSize + " rows for single key");

        Row r = slice.get(0);

        if (null == r) {
            log.warn("Null Row object retrieved from Cassandra StorageProxy");
            return EntryList.EMPTY_LIST;
        }

        ColumnFamily cf = r.cf;

        if (null == cf) {
            log.debug("null ColumnFamily (\"{}\")", columnFamily);
            return EntryList.EMPTY_LIST;
        }

        if (cf.isMarkedForDelete())
            return EntryList.EMPTY_LIST;

        return CassandraHelper.makeEntryList(
                Iterables.filter(cf.getSortedColumns(),FilterDeletedColumns.INSTANCE),
                CassandraEmbeddedGetter.INSTANCE,
                query.getSliceEnd(),
                query.getLimit());

    }

    private enum FilterDeletedColumns implements Predicate<IColumn> {

        INSTANCE;

        @Override
        public boolean apply(@Nullable IColumn iColumn) {
            return !iColumn.isMarkedForDelete();
        }
    }

    @Override
    public Map<StaticBuffer,EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
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

    private static enum CassandraEmbeddedGetter implements StaticArrayEntry.GetColVal<IColumn,ByteBuffer> {
        INSTANCE;

        @Override
        public ByteBuffer getColumn(IColumn element) {
            return org.apache.cassandra.utils.ByteBufferUtil.clone(element.name());
        }

        @Override
        public ByteBuffer getValue(IColumn element) {
            return org.apache.cassandra.utils.ByteBufferUtil.clone(element.value());
        }
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
                return StaticArrayBuffer.of(currentKey);
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

            return new RecordIterator<Entry>() {
                final Iterator<Entry> columns = CassandraHelper.makeEntryIterator(
                        Iterables.filter(currentRow.cf.getSortedColumns(),FilterDeletedColumns.INSTANCE),
                CassandraEmbeddedGetter.INSTANCE,
                        sliceQuery.getSliceEnd(),
                        sliceQuery.getLimit());

                 //cfToEntries(currentRow.cf, sliceQuery).iterator();

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
