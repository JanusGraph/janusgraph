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

package org.janusgraph.diskstorage.cassandra.thrift;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.cassandra.thrift.thriftpool.CTConnection;
import org.janusgraph.diskstorage.cassandra.thrift.thriftpool.CTConnectionPool;
import org.janusgraph.diskstorage.cassandra.utils.CassandraHelper;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.commons.lang.ArrayUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.janusgraph.diskstorage.cassandra.CassandraTransaction.getTx;

/**
 * A JanusGraph {@code KeyColumnValueStore} backed by Cassandra.
 * This uses the Cassandra Thrift API.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 * @see CassandraThriftStoreManager
 */
public class CassandraThriftKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger logger =
            LoggerFactory.getLogger(CassandraThriftKeyColumnValueStore.class);

    private static final Pattern BROKEN_BYTE_TOKEN_PATTERN = Pattern.compile("^Token\\(bytes\\[(.+)\\]\\)$");

    // Cassandra access
    private final CassandraThriftStoreManager storeManager;
    private final String keyspace;
    private final String columnFamily;
    private final CTConnectionPool pool;
    private final ThriftGetter entryGetter;

    public CassandraThriftKeyColumnValueStore(String keyspace, String columnFamily, CassandraThriftStoreManager storeManager,
                                              CTConnectionPool pool) {
        this.storeManager = storeManager;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.pool = pool;
        this.entryGetter = new ThriftGetter(storeManager.getMetaDataSchema(columnFamily));
    }

    /**
     * Call Cassandra's Thrift get_slice() method.
     * <p/>
     * When columnEnd equals columnStart and either startInclusive
     * or endInclusive is false (or both are false), then this
     * method returns an empty list without making any Thrift calls.
     * <p/>
     * If columnEnd = columnStart + 1, and both startInclusive and
     * startExclusive are false, then the arguments effectively form
     * an empty interval.  In this case, as in the one previous,
     * an empty list is returned.  However, it may not necessarily
     * be handled efficiently; a Thrift call might still be made
     * before returning the empty list.
     *
     * @throws org.janusgraph.diskstorage.BackendException
     *          when columnEnd < columnStart
     */
    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer, EntryList> result = getNamesSlice(query.getKey(), query, txh);
        return Iterables.getOnlyElement(result.values(), EntryList.EMPTY_LIST);
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
                                                      SliceQuery query,
                                                      StoreTransaction txh) throws BackendException {
        ColumnParent parent = new ColumnParent(columnFamily);
        /*
         * Cassandra cannot handle columnStart = columnEnd.
		 * Cassandra's Thrift getSlice() throws InvalidRequestException
		 * if columnStart = columnEnd.
		 */
        if (query.getSliceStart().compareTo(query.getSliceEnd()) >= 0) {
            // Check for invalid arguments where columnEnd < columnStart
            if (query.getSliceEnd().compareTo(query.getSliceStart())<0) {
                throw new PermanentBackendException("columnStart=" + query.getSliceStart() +
                        " is greater than columnEnd=" + query.getSliceEnd() + ". " +
                        "columnStart must be less than or equal to columnEnd");
            }
            if (0 != query.getSliceStart().length() && 0 != query.getSliceEnd().length()) {
                logger.debug("Return empty list due to columnEnd==columnStart and neither empty");
                return KCVSUtil.emptyResults(keys);
            }
        }

        assert query.getSliceStart().compareTo(query.getSliceEnd()) < 0;
        ConsistencyLevel consistency = getTx(txh).getReadConsistencyLevel().getThrift();
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange();
        range.setCount(query.getLimit() + (query.hasLimit()?1:0)); //Add one for potentially removed last column
        range.setStart(query.getSliceStart().asByteBuffer());
        range.setFinish(query.getSliceEnd().asByteBuffer());
        predicate.setSlice_range(range);

        CTConnection conn = null;
        try {
            conn = pool.borrowObject(keyspace);
            Cassandra.Client client = conn.getClient();
            Map<ByteBuffer, List<ColumnOrSuperColumn>> rows = client.multiget_slice(CassandraHelper.convert(keys),
                    parent,
                    predicate,
                    consistency);

			/*
			 * The final size of the "result" List may be at most rows.size().
			 * However, "result" could also be up to two elements smaller than
			 * rows.size(), depending on startInclusive and endInclusive
			 */
            Map<StaticBuffer, EntryList> results = new HashMap<StaticBuffer, EntryList>();

            for (ByteBuffer key : rows.keySet()) {
                results.put(StaticArrayBuffer.of(key),
                        CassandraHelper.makeEntryList(rows.get(key), entryGetter, query.getSliceEnd(), query.getLimit()));
            }

            return results;
        } catch (Exception e) {
            throw convertException(e);
        } finally {
            pool.returnObjectUnsafe(keyspace, conn);
        }
    }

    private static class ThriftGetter implements StaticArrayEntry.GetColVal<ColumnOrSuperColumn,ByteBuffer> {

        private final EntryMetaData[] schema;

        private ThriftGetter(EntryMetaData[] schema) {
            this.schema = schema;
        }

        @Override
        public ByteBuffer getColumn(ColumnOrSuperColumn element) {
            return element.getColumn().bufferForName();
        }

        @Override
        public ByteBuffer getValue(ColumnOrSuperColumn element) {
            return element.getColumn().bufferForValue();
        }

        @Override
        public EntryMetaData[] getMetaSchema(ColumnOrSuperColumn element) {
            return schema;
        }

        @Override
        public Object getMetaData(ColumnOrSuperColumn element, EntryMetaData meta) {
            switch(meta) {
                case TIMESTAMP:
                    return element.getColumn().getTimestamp();
                case TTL:
                    return element.getColumn().getTtl();
                default:
                    throw new UnsupportedOperationException("Unsupported meta data: " + meta);
            }
        }
    }

    @Override
    public void close() {
        // Do nothing
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue,
                            StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(@Nullable SliceQuery sliceQuery, StoreTransaction txh) throws BackendException {
        final IPartitioner partitioner = storeManager.getCassandraPartitioner();

        if (!(partitioner instanceof RandomPartitioner) && !(partitioner instanceof Murmur3Partitioner))
            throw new PermanentBackendException("This operation is only allowed when random partitioner (md5 or murmur3) is used.");

        try {
            return new AllTokensIterator(partitioner, sliceQuery, storeManager.getPageSize());
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery keyRangeQuery, StoreTransaction txh) throws BackendException {
        final IPartitioner partitioner = storeManager.getCassandraPartitioner();

        // see rant about the reason of this limitation in Astyanax implementation of this method.
        if (!(partitioner instanceof AbstractByteOrderedPartitioner))
            throw new PermanentBackendException("This operation is only allowed when byte-ordered partitioner is used.");

        try {
            return new KeyRangeIterator(partitioner, keyRangeQuery, storeManager.getPageSize(),
                    keyRangeQuery.getKeyStart().asByteBuffer(),
                    keyRangeQuery.getKeyEnd().asByteBuffer());
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    @Override
    public String getName() {
        return columnFamily;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer, KCVMutation> mutations = ImmutableMap.of(key, new KCVMutation(additions, deletions));
        mutateMany(mutations, txh);
    }

    public void mutateMany(Map<StaticBuffer, KCVMutation> mutations, StoreTransaction txh) throws BackendException {
        storeManager.mutateMany(ImmutableMap.of(columnFamily, mutations), txh);
    }

    static BackendException convertException(Throwable e) {
        if (e instanceof TException) {
            return new PermanentBackendException(e);
        } else if (e instanceof TimedOutException) {
            return new TemporaryBackendException(e);
        } else if (e instanceof UnavailableException) {
            return new TemporaryBackendException(e);
        } else if (e instanceof InvalidRequestException) {
            return new PermanentBackendException(e);
        } else {
            return new PermanentBackendException(e);
        }
    }

    @Override
    public String toString() {
        return "CassandraThriftKeyColumnValueStore[ks="
                + keyspace + ", cf=" + columnFamily + "]";
    }


    private List<KeySlice> getKeySlice(ByteBuffer startKey,
                                       ByteBuffer endKey,
                                       SliceQuery columnSlice,
                                       int count) throws BackendException {
        return getRangeSlices(new org.apache.cassandra.thrift.KeyRange().setStart_key(startKey).setEnd_key(endKey).setCount(count), columnSlice);
    }

    private <T extends Token> List<KeySlice> getTokenSlice(T startToken, T endToken,
            SliceQuery sliceQuery, int count) throws BackendException {

        String st = sanitizeBrokenByteToken(startToken);
        String et = sanitizeBrokenByteToken(endToken);

        org.apache.cassandra.thrift.KeyRange kr = new org.apache.cassandra.thrift.KeyRange().setStart_token(st).setEnd_token(et).setCount(count);

        return getRangeSlices(kr, sliceQuery);
    }

    private String sanitizeBrokenByteToken(Token tok) {
        /*
         * Background: https://issues.apache.org/jira/browse/CASSANDRA-5566
         *
         * This check is useful for compatibility with Cassandra server versions
         * 1.2.4 and earlier.
         */
        String st = tok.toString();
        if (!(tok instanceof BytesToken))
            return st;

        // Do a cheap 1-character startsWith before unleashing the regex
        if (st.startsWith("T")) {
            Matcher m = BROKEN_BYTE_TOKEN_PATTERN.matcher(st);
            if (!m.matches()) {
                logger.warn("Unknown token string format: \"{}\"", st);
            } else {
                String old = st;
                st = m.group(1);
                logger.debug("Rewrote token string: \"{}\" -> \"{}\"", old, st);
            }
        }
        return st;
    }

    private List<KeySlice> getRangeSlices(org.apache.cassandra.thrift.KeyRange keyRange, @Nullable SliceQuery sliceQuery) throws BackendException {
        SliceRange sliceRange = new SliceRange();

        if (sliceQuery == null) {
            sliceRange.setStart(ArrayUtils.EMPTY_BYTE_ARRAY)
                    .setFinish(ArrayUtils.EMPTY_BYTE_ARRAY)
                    .setCount(5);
        } else {
            sliceRange.setStart(sliceQuery.getSliceStart().asByteBuffer())
                    .setFinish(sliceQuery.getSliceEnd().asByteBuffer())
                    .setCount((sliceQuery.hasLimit()) ? sliceQuery.getLimit() : Integer.MAX_VALUE);
        }


        CTConnection connection = null;
        try {
            connection = pool.borrowObject(keyspace);

            List<KeySlice> slices =
                    connection.getClient().get_range_slices(new ColumnParent(columnFamily),
                            new SlicePredicate()
                                    .setSlice_range(sliceRange),
                            keyRange,
                            ConsistencyLevel.QUORUM);

            for (KeySlice s : slices) {
                logger.debug("Key {}", ByteBufferUtil.toString(s.key, "-"));
            }

            /* Note: we need to fetch columns for each row as well to remove "range ghosts" */
            List<KeySlice> result = new ArrayList<>(slices.size());
            KeyIterationPredicate pred = new KeyIterationPredicate();
            for (KeySlice ks : slices)
                if (pred.apply(ks))
                    result.add(ks);
            return result;
        } catch (Exception e) {
            throw convertException(e);
        } finally {
            if (connection != null)
                pool.returnObjectUnsafe(keyspace, connection);
        }
    }

    private static class KeyIterationPredicate implements Predicate<KeySlice> {

        @Override
        public boolean apply(@Nullable KeySlice row) {
            return (row != null) && row.getColumns().size() > 0;
        }
    }

    /**
     * Slices rows and columns using tokens. Recall that the partitioner turns
     * keys into tokens. For instance, under RandomPartitioner, tokens are the
     * MD5 hashes of keys.
     */
    public class AbstractBufferedRowIter implements KeyIterator {

        private final int pageSize;
        private final SliceQuery columnSlice;

        private boolean isClosed;
        private boolean seenEnd;
        protected Iterator<KeySlice> ksIter;
        private KeySlice mostRecentRow;

        private final IPartitioner partitioner;
        private Token nextStartToken;
        private final Token endToken;
        private ByteBuffer nextStartKey;

        private boolean omitEndToken;

        public AbstractBufferedRowIter(IPartitioner partitioner,
                SliceQuery columnSlice, int pageSize, Token startToken, Token endToken, boolean omitEndToken) {
            this.pageSize = pageSize;
            this.partitioner = partitioner;
            this.nextStartToken = startToken;
            this.endToken = endToken;
            this.columnSlice = columnSlice;

            this.seenEnd = false;
            this.isClosed = false;
            this.ksIter = Iterators.emptyIterator();
            this.mostRecentRow = null;
            this.omitEndToken = omitEndToken;
        }

        @Override
        public boolean hasNext() {
            ensureOpen();

            if (!ksIter.hasNext() && !seenEnd) {
                try {
                    ksIter = rebuffer().iterator();
                } catch (BackendException e) {
                    throw new RuntimeException(e);
                }
            }

            return ksIter.hasNext();
        }

        @Override
        public StaticBuffer next() {
            ensureOpen();

            if (!hasNext())
                throw new NoSuchElementException();

            mostRecentRow = ksIter.next();

            Preconditions.checkNotNull(mostRecentRow);
            return StaticArrayBuffer.of(mostRecentRow.bufferForKey());
        }

        @Override
        public void close() {
            closeIterator();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            return new RecordIterator<Entry>() {
                final Iterator<Entry> columns =
                        CassandraHelper.makeEntryIterator(mostRecentRow.getColumns(),
                                entryGetter, columnSlice.getSliceEnd(),
                                columnSlice.getLimit());

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
                    closeIterator();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        private void ensureOpen() {
            if (isClosed)
                throw new IllegalStateException("Iterator has been closed.");
        }

        private void closeIterator() {
            if (!isClosed) {
                isClosed = true;
            }
        }

        private List<KeySlice> rebuffer() throws BackendException {

            Preconditions.checkArgument(!seenEnd);

            return checkFreshSlices(getNextKeySlices());
        }

        protected List<KeySlice> checkFreshSlices(List<KeySlice> ks) {

            if (0 == ks.size()) {
                seenEnd = true;
                return Collections.emptyList();
            }

            nextStartKey = ks.get(ks.size() - 1).bufferForKey();
            nextStartToken = partitioner.getToken(nextStartKey);

            if (nextStartToken.equals(endToken)) {
                seenEnd = true;
                if (omitEndToken)
                    ks.remove(ks.size() - 1);
            }

            return ks;
        }

        protected final List<KeySlice> getNextKeySlices() throws BackendException {
            return getTokenSlice(nextStartToken, endToken, columnSlice, pageSize);
        }
    }

    private final class AllTokensIterator extends AbstractBufferedRowIter {
        public AllTokensIterator(IPartitioner partitioner, SliceQuery columnSlice, int pageSize) {
            super(partitioner, columnSlice, pageSize, partitioner.getMinimumToken(), partitioner.getMinimumToken(), false);
        }
    }

    private final class KeyRangeIterator extends AbstractBufferedRowIter {
        public KeyRangeIterator(IPartitioner partitioner, SliceQuery columnSlice,
                int pageSize, ByteBuffer startKey, ByteBuffer endKey) throws BackendException {
            super(partitioner, columnSlice, pageSize, partitioner.getToken(startKey), partitioner.getToken(endKey), true);

            Preconditions.checkArgument(partitioner instanceof AbstractByteOrderedPartitioner);

            // Get first slice with key range instead of token range. Token
            // ranges are start-exclusive, key ranges are start-inclusive. Both
            // are end-inclusive. If we don't make the call below, then we will
            // erroneously miss startKey.
            List<KeySlice> ks = getKeySlice(startKey, endKey, columnSlice, pageSize);

            this.ksIter = checkFreshSlices(ks).iterator();
        }
    }
}
