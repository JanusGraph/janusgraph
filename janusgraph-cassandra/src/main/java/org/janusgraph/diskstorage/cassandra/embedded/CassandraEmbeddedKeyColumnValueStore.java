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

package org.janusgraph.diskstorage.cassandra.embedded;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.cassandra.utils.CassandraHelper;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.*;
import static org.janusgraph.diskstorage.cassandra.CassandraTransaction.getTx;

public class CassandraEmbeddedKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger log = LoggerFactory.getLogger(CassandraEmbeddedKeyColumnValueStore.class);

    private final String keyspace;
    private final String columnFamily;
    private final CassandraEmbeddedStoreManager storeManager;
    private final TimestampProvider times;
    private final CassandraEmbeddedGetter entryGetter;

    public CassandraEmbeddedKeyColumnValueStore(
            String keyspace,
            String columnFamily,
            CassandraEmbeddedStoreManager storeManager) throws RuntimeException {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.storeManager = storeManager;
        this.times = this.storeManager.getTimestampProvider();
        entryGetter = new CassandraEmbeddedGetter(storeManager.getMetaDataSchema(columnFamily),times);
    }

    @Override
    public void close() throws BackendException {
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column,
                            StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery keyRangeQuery, StoreTransaction txh) throws BackendException {
        IPartitioner partitioner = StorageService.getPartitioner();

        // see rant about this in Astyanax implementation
        if (partitioner instanceof RandomPartitioner || partitioner instanceof Murmur3Partitioner)
            throw new PermanentBackendException("This operation is only supported when byte-ordered partitioner is used.");

        return new RowIterator(keyRangeQuery, storeManager.getPageSize(), txh);
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        return new RowIterator(getMinimumToken(), getMaximumToken(), query, storeManager.getPageSize(), txh);
    }


    /**
     * Create a RangeSliceCommand and run it against the StorageProxy.
     * <p>
     * To match the behavior of the standard Cassandra thrift API endpoint, the
     * {@code nowMillis} argument should be the number of milliseconds since the
     * UNIX Epoch (e.g. System.currentTimeMillis() or equivalent obtained
     * through a {@link TimestampProvider}). This is per
     * {@link org.apache.cassandra.thrift.CassandraServer#get_range_slices(ColumnParent, SlicePredicate, KeyRange, ConsistencyLevel)},
     * which passes the server's System.currentTimeMillis() to the
     * {@code RangeSliceCommand} constructor.
     */
    private List<Row> getKeySlice(Token start,
                                  Token end,
                                  @Nullable SliceQuery sliceQuery,
                                  int pageSize,
                                  long nowMillis) throws BackendException {
        IPartitioner partitioner = StorageService.getPartitioner();

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
            CFMetaData cfm = Schema.instance.getCFMetaData(keyspace, columnFamily);
            IDiskAtomFilter filter = ThriftValidation.asIFilter(predicate, cfm, null);

            RangeSliceCommand cmd = new RangeSliceCommand(keyspace, columnFamily, nowMillis, filter, new Bounds<RowPosition>(startPosition, endPosition), pageSize);

            rows = StorageProxy.getRangeSlice(cmd, ConsistencyLevel.QUORUM);
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }

        return rows;
    }

    @Override
    public String getName() {
        return columnFamily;
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {

        /**
         * This timestamp mimics the timestamp used by
         * {@link org.apache.cassandra.thrift.CassandraServer#get(ByteBuffer,ColumnPath,ConsistencyLevel)}.
         *
         * That method passes the server's System.currentTimeMillis() to
         * {@link ReadCommand#create(String, ByteBuffer, String, long, IDiskAtomFilter)}.
         * {@code create(...)} in turn passes that timestamp to the SliceFromReadCommand constructor.
         */
        final long nowMillis = times.getTime().toEpochMilli();
        Composite startComposite = CellNames.simpleDense(query.getSliceStart().asByteBuffer());
        Composite endComposite = CellNames.simpleDense(query.getSliceEnd().asByteBuffer());
        SliceQueryFilter sqf = new SliceQueryFilter(startComposite, endComposite,
                false, query.getLimit() + (query.hasLimit()?1:0));
        ReadCommand sliceCmd = new SliceFromReadCommand(keyspace, query.getKey().asByteBuffer(), columnFamily, nowMillis, sqf);

        List<Row> slice = read(sliceCmd, getTx(txh).getReadConsistencyLevel().getDB());

        if (null == slice || 0 == slice.size())
            return EntryList.EMPTY_LIST;

        int sliceSize = slice.size();
        if (1 < sliceSize)
            throw new PermanentBackendException("Received " + sliceSize + " rows for single key");

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
                Iterables.filter(cf.getSortedColumns(), new FilterDeletedColumns(nowMillis)),
                entryGetter,
                query.getSliceEnd(),
                query.getLimit());

    }

    private class FilterDeletedColumns implements Predicate<Cell> {

        private final long tsMillis;
        private final int tsSeconds;

        private FilterDeletedColumns(long tsMillis) {
            this.tsMillis = tsMillis;
            this.tsSeconds = (int)(this.tsMillis / 1000L);
        }

        @Override
        public boolean apply(Cell input) {
            if (!input.isLive(tsMillis))
                return false;

            // Don't do this.  getTimeToLive() is a duration divorced from any particular clock.
            // For instance, if TTL=10 seconds, getTimeToLive() will have value 10 (not 10 + epoch seconds), and
            // this will always return false.
            //if (input instanceof ExpiringCell)
            //    return tsSeconds < ((ExpiringCell)input).getTimeToLive();

            return true;
        }
    }

    @Override
    public Map<StaticBuffer,EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions,
                       List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer, KCVMutation> mutations = ImmutableMap.of(key, new
                KCVMutation(additions, deletions));
        mutateMany(mutations, txh);
    }


    public void mutateMany(Map<StaticBuffer, KCVMutation> mutations,
                           StoreTransaction txh) throws BackendException {
        storeManager.mutateMany(ImmutableMap.of(columnFamily, mutations), txh);
    }

    private static List<Row> read(ReadCommand cmd, org.apache.cassandra.db.ConsistencyLevel clvl) throws BackendException {
        ArrayList<ReadCommand> cmdHolder = new ArrayList<ReadCommand>(1);
        cmdHolder.add(cmd);
        return read(cmdHolder, clvl);
    }

    private static List<Row> read(List<ReadCommand> cmds, org.apache.cassandra.db.ConsistencyLevel clvl) throws BackendException {
        try {
            return StorageProxy.read(cmds, clvl);
        } catch (UnavailableException e) {
            throw new TemporaryBackendException(e);
        } catch (RequestTimeoutException e) {
            throw new PermanentBackendException(e);
        } catch (IsBootstrappingException e) {
            throw new TemporaryBackendException(e);
        } catch (InvalidRequestException e) {
            throw new PermanentBackendException(e);
        }
    }

    private static class CassandraEmbeddedGetter implements StaticArrayEntry.GetColVal<Cell,ByteBuffer> {

        private final EntryMetaData[] schema;
        private final TimestampProvider times;

        private CassandraEmbeddedGetter(EntryMetaData[] schema, TimestampProvider times) {
            this.schema = schema;
            this.times = times;
        }

        @Override
        public ByteBuffer getColumn(Cell element) {
            return org.apache.cassandra.utils.ByteBufferUtil.clone(element.name().toByteBuffer());
        }

        @Override
        public ByteBuffer getValue(Cell element) {
            return org.apache.cassandra.utils.ByteBufferUtil.clone(element.value());
        }

        @Override
        public EntryMetaData[] getMetaSchema(Cell element) {
            return schema;
        }

        @Override
        public Object getMetaData(Cell element, EntryMetaData meta) {
            switch (meta) {
                case TIMESTAMP:
                    return element.timestamp();
                case TTL:
                    return ((element instanceof ExpiringCell)
                                    ? ((ExpiringCell) element).getTimeToLive()
                                    : 0);
                default:
                    throw new UnsupportedOperationException("Unsupported meta data: " + meta);
            }
        }
    }

    private class RowIterator implements KeyIterator {
        private final Token maximumToken;
        private final SliceQuery sliceQuery;
        private final StoreTransaction txh;

        /**
         * This RowIterator will use this timestamp for its entire lifetime,
         * even if the iterator runs more than one distinct slice query while
         * paging. <b>This field must be in units of milliseconds since
         * the UNIX Epoch</b>.
         * <p>
         * This timestamp is passed to three methods/constructors:
         * <ul>
         *  <li>{@link org.apache.cassandra.db.Column#isMarkedForDelete(long now)}</li>
         *  <li>{@link org.apache.cassandra.db.ColumnFamily#hasOnlyTombstones(long)}</li>
         *  <li>
         *   the {@link RangeSliceCommand} constructor via the last argument
         *   to {@link CassandraEmbeddedKeyColumnValueStore#getKeySlice(Token, Token, SliceQuery, int, long)}
         *  </li>
         * </ul>
         * The second list entry just calls the first and almost doesn't deserve
         * a mention at present, but maybe the implementation will change in the future.
         * <p>
         * When this value needs to be compared to TTL seconds expressed in seconds,
         * Cassandra internals do the conversion.
         * Consider {@link ExpiringColumn#isMarkedForDelete(long)}, which is implemented,
         * as of 2.0.6, by the following one-liner:
         * <p>
         * {@code return (int) (now / 1000) >= getLocalDeletionTime()}
         * <p>
         * The {@code now / 1000} does the conversion from milliseconds to seconds
         * (the units of getLocalDeletionTime()).
         */
        private final long nowMillis;

        private Iterator<Row> keys;
        private ByteBuffer lastSeenKey = null;
        private Row currentRow;
        private int pageSize;

        private boolean isClosed;

        public RowIterator(KeyRangeQuery keyRangeQuery, int pageSize, StoreTransaction txh) throws BackendException {
            this(StorageService.getPartitioner().getToken(keyRangeQuery.getKeyStart().asByteBuffer()),
                    StorageService.getPartitioner().getToken(keyRangeQuery.getKeyEnd().asByteBuffer()),
                    keyRangeQuery,
                    pageSize,
                    txh);
        }

        public RowIterator(Token minimum, Token maximum, SliceQuery sliceQuery, int pageSize, StoreTransaction txh) throws BackendException {
            this.pageSize = pageSize;
            this.sliceQuery = sliceQuery;
            this.maximumToken = maximum;
            this.txh = txh;
            this.nowMillis = times.getTime().toEpochMilli();
            this.keys = getRowsIterator(getKeySlice(minimum, maximum, sliceQuery, pageSize, nowMillis));
        }

        @Override
        public boolean hasNext() {
            try {
                return hasNextInternal();
            } catch (BackendException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public StaticBuffer next() {
            ensureOpen();

            if (!hasNext())
                throw new NoSuchElementException();

            currentRow = keys.next();
            ByteBuffer currentKey = currentRow.key.getKey().duplicate();

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
                        Iterables.filter(currentRow.cf.getSortedColumns(), new FilterDeletedColumns(nowMillis)),
                        entryGetter,
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

        private final boolean hasNextInternal() throws BackendException {
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

                List<Row> newKeys = getKeySlice(StorageService.getPartitioner().getToken(lastSeenKey), maximumToken, sliceQuery, pageSize, nowMillis);

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
                    // The hasOnlyTombstones(x) call below ultimately calls Column.isMarkedForDelete(x)
                    return !(row == null || row.cf == null || row.cf.isMarkedForDelete() || row.cf.hasOnlyTombstones(nowMillis));
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
                    return row != null && !row.key.getKey().equals(exceptKey);
                }
            });
        }
    }

    private static Token getMinimumToken() throws PermanentBackendException {
        IPartitioner partitioner = StorageService.getPartitioner();

        if (partitioner instanceof RandomPartitioner) {
            return ((RandomPartitioner) partitioner).getMinimumToken();
        } else if (partitioner instanceof Murmur3Partitioner) {
            return ((Murmur3Partitioner) partitioner).getMinimumToken();
        } else if (partitioner instanceof ByteOrderedPartitioner) {
            //TODO: This makes the assumption that its an EdgeStore (i.e. 8 byte keys)
            return new BytesToken(org.janusgraph.diskstorage.util.ByteBufferUtil.zeroByteBuffer(8));
        } else {
            throw new PermanentBackendException("Unsupported partitioner: " + partitioner);
        }
    }

    private static Token getMaximumToken() throws PermanentBackendException {
        IPartitioner partitioner = StorageService.getPartitioner();

        if (partitioner instanceof RandomPartitioner) {
            return new BigIntegerToken(RandomPartitioner.MAXIMUM);
        } else if (partitioner instanceof Murmur3Partitioner) {
            return new LongToken(Murmur3Partitioner.MAXIMUM);
        } else if (partitioner instanceof ByteOrderedPartitioner) {
            //TODO: This makes the assumption that its an EdgeStore (i.e. 8 byte keys)
            return new BytesToken(org.janusgraph.diskstorage.util.ByteBufferUtil.oneByteBuffer(8));
        } else {
            throw new PermanentBackendException("Unsupported partitioner: " + partitioner);
        }
    }
}
