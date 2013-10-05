package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.cassandra.dht.AbstractByteOrderedPartitioner;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnectionPool;
import com.thinkaurelius.titan.diskstorage.cassandra.utils.CassandraHelper;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ByteBufferEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRangeQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;

/**
 * A Titan {@code KeyColumnValueStore} backed by Cassandra.
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

    public CassandraThriftKeyColumnValueStore(String keyspace, String columnFamily, CassandraThriftStoreManager storeManager,
                                              CTConnectionPool pool) {
        this.storeManager = storeManager;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.pool = pool;
    }

    /**
     * Call Cassandra's Thrift get_slice() method.
     * <p/>
     * When columnEnd equals columnStart, and both startInclusive
     * and endInclusive are true, then this method calls
     * {@link #get(java.nio.ByteBuffer, java.nio.ByteBuffer, com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction)}
     * instead of calling Thrift's getSlice() method and returns
     * a one-element list containing the result.
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
     * @throws com.thinkaurelius.titan.diskstorage.StorageException
     *          when columnEnd < columnStart
     */
    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        ByteBuffer key = query.getKey().asByteBuffer();
        List<Entry> slice = getNamesSlice(Arrays.asList(query.getKey()), query, txh).get(key);
        return (slice == null) ? Collections.<Entry>emptyList() : slice;
    }

    @Override
    public List<List<Entry>> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
        return CassandraHelper.order(getNamesSlice(keys, query, txh), keys);
    }

    public Map<ByteBuffer, List<Entry>> getNamesSlice(List<StaticBuffer> keys,
                                                      SliceQuery query,
                                                      StoreTransaction txh) throws StorageException {
        Preconditions.checkArgument(query.getLimit() >= 0);
        if (0 == query.getLimit())
            return Collections.emptyMap();

        ColumnParent parent = new ColumnParent(columnFamily);
        /*
         * Cassandra cannot handle columnStart = columnEnd.
		 * Cassandra's Thrift getSlice() throws InvalidRequestException
		 * if columnStart = columnEnd.
		 */
        if (ByteBufferUtil.compare(query.getSliceStart(), query.getSliceEnd()) >= 0) {
            // Check for invalid arguments where columnEnd < columnStart
            if (ByteBufferUtil.isSmallerThan(query.getSliceEnd(), query.getSliceStart())) {
                throw new PermanentStorageException("columnStart=" + query.getSliceStart() +
                        " is greater than columnEnd=" + query.getSliceEnd() + ". " +
                        "columnStart must be less than or equal to columnEnd");
            }
            if (0 != query.getSliceStart().length() && 0 != query.getSliceEnd().length()) {
                logger.debug("Return empty list due to columnEnd==columnStart and neither empty");
                return Collections.emptyMap();
            }
        }

        // true: columnStart < columnEnd
        ConsistencyLevel consistency = getTx(txh).getReadConsistencyLevel().getThriftConsistency();
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange();
        range.setCount(query.getLimit());
        range.setStart(query.getSliceStart().asByteBuffer());
        range.setFinish(query.getSliceEnd().asByteBuffer());
        predicate.setSlice_range(range);

        CTConnection conn = null;
        try {
            conn = pool.borrowObject(keyspace);
            Cassandra.Client client = conn.getClient();

            List<ByteBuffer> requestKeys = new ArrayList<ByteBuffer>(keys.size());
            {
                for (StaticBuffer key : keys) {
                    requestKeys.add(key.asByteBuffer());
                }
            }

            Map<ByteBuffer, List<ColumnOrSuperColumn>> rows = client.multiget_slice(requestKeys,
                    parent,
                    predicate,
                    consistency);

			/*
			 * The final size of the "result" List may be at most rows.size().
			 * However, "result" could also be up to two elements smaller than
			 * rows.size(), depending on startInclusive and endInclusive
			 */
            Map<ByteBuffer, List<Entry>> results = new HashMap<ByteBuffer, List<Entry>>();

            ByteBuffer sliceEndBB = query.getSliceEnd().asByteBuffer();

            for (ByteBuffer key : rows.keySet()) {
                results.put(key, excludeLastColumn(rows.get(key), sliceEndBB));
            }

            return results;
        } catch (Exception e) {
            throw convertException(e);
        } finally {
            pool.returnObjectUnsafe(keyspace, conn);
        }
    }

    private static List<Entry> excludeLastColumn(List<ColumnOrSuperColumn> row, ByteBuffer lastColumn) {
        List<Entry> entries = new ArrayList<Entry>();

        for (ColumnOrSuperColumn r : row) {
            Column c = r.getColumn();

            // Skip column if it is equal to columnEnd because columnEnd is exclusive
            if (lastColumn.equals(c.bufferForName()))
                break;

            entries.add(new ByteBufferEntry(c.bufferForName(), c.bufferForValue()));
        }

        return entries;
    }

    @Override
    public void close() {
        // Do nothing
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        ColumnParent parent = new ColumnParent(columnFamily);
        ConsistencyLevel consistency = getTx(txh).getReadConsistencyLevel().getThriftConsistency();
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange();
        range.setCount(1);
        byte[] empty = new byte[0];
        range.setStart(empty);
        range.setFinish(empty);
        predicate.setSlice_range(range);

        CTConnection conn = null;
        try {
            conn = pool.borrowObject(keyspace);
            Cassandra.Client client = conn.getClient();
            List<?> result = client.get_slice(key.asByteBuffer(), parent, predicate, consistency);
            return 0 < result.size();
        } catch (Exception e) {
            throw convertException(e);
        } finally {
            pool.returnObjectUnsafe(keyspace, conn);
        }
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue,
                            StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(@Nullable SliceQuery sliceQuery, StoreTransaction txh) throws StorageException {
        final IPartitioner<? extends Token<?>> partitioner = storeManager.getCassandraPartitioner();

        if (!(partitioner instanceof RandomPartitioner) && !(partitioner instanceof Murmur3Partitioner))
            throw new PermanentStorageException("This operation is only allowed when random partitioner (md5 or murmur3) is used.");

        try {
            return new AllTokensIterator<Token<?>>(pool.borrowObject(keyspace), partitioner, sliceQuery, storeManager.getPageSize());
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery keyRangeQuery, StoreTransaction txh) throws StorageException {
        final IPartitioner<? extends Token<?>> partitioner = storeManager.getCassandraPartitioner();

        // see rant about the reason of this limitation in Astyanax implementation of this method.
        if (!(partitioner instanceof AbstractByteOrderedPartitioner))
            throw new PermanentStorageException("This operation is only allowed when byte-ordered partitioner is used.");

        try {
            SliceQuery columnSlice = new SliceQuery(
                    keyRangeQuery.getSliceStart(), keyRangeQuery.getSliceEnd());
            
            return new KeyRangeIterator<Token<?>>(
                    pool.borrowObject(keyspace),
                    partitioner, columnSlice, storeManager.getPageSize(),
                    keyRangeQuery.getKeyStart().asByteBuffer(),
                    keyRangeQuery.getKeyEnd().asByteBuffer());
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }


    @Override
    public String getName() {
        return columnFamily;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws StorageException {
        Map<StaticBuffer, KCVMutation> mutations = ImmutableMap.of(key, new KCVMutation(additions, deletions));
        mutateMany(mutations, txh);
    }

    public void mutateMany(Map<StaticBuffer, KCVMutation> mutations, StoreTransaction txh) throws StorageException {
        storeManager.mutateMany(ImmutableMap.of(columnFamily, mutations), txh);
    }

    static StorageException convertException(Throwable e) {
        if (e instanceof TException) {
            return new PermanentStorageException(e);
        } else if (e instanceof TimedOutException) {
            return new TemporaryStorageException(e);
        } else if (e instanceof UnavailableException) {
            return new TemporaryStorageException(e);
        } else if (e instanceof InvalidRequestException) {
            return new PermanentStorageException(e);
        } else {
            return new PermanentStorageException(e);
        }
    }

    @Override
    public String toString() {
        return "CassandraThriftKeyColumnValueStore[ks="
                + keyspace + ", cf=" + columnFamily + "]";
    }


    private List<KeySlice> getKeySlice(Cassandra.Client client,
                                           ByteBuffer startKey,
                                           ByteBuffer endKey,
                                           SliceQuery columnSlice,
                                           int count) throws StorageException {
        return getRangeSlices(client, new KeyRange().setStart_key(startKey).setEnd_key(endKey).setCount(count), columnSlice);
    }

    private <T extends Token<?>> List<KeySlice> getTokenSlice(
            Cassandra.Client client, T startToken, T endToken,
            SliceQuery sliceQuery, int count) throws StorageException {
        
        String st = sanitizeBrokenByteToken(startToken);
        String et = sanitizeBrokenByteToken(endToken);
        
        KeyRange kr = new KeyRange().setStart_token(st).setEnd_token(et).setCount(count);

        return getRangeSlices(client, kr, sliceQuery);
    }
    
    private String sanitizeBrokenByteToken(Token<?> tok) {
        /*
         * Background: https://issues.apache.org/jira/browse/CASSANDRA-5566
         * 
         * This hack can go away when we upgrade to or past 1.2.5. But as I
         * write this comment, we're still stuck on 1.2.2 because Astyanax
         * hasn't upgraded and tries to call an undefined thrift constructor
         * when I try running against Cassandra 1.2.10. I haven't tried 1.2.5.
         * However, I think it's not worth breaking from Astyanax's supported
         * Cassandra version unless we can break all the way to the latest
         * Cassandra version, and 1.2.5 is not the latest anyway.
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

    private List<KeySlice> getRangeSlices(Cassandra.Client client,
                                           KeyRange keyRange,
                                           @Nullable SliceQuery sliceQuery) throws StorageException {
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


        try {
            List<KeySlice> slices =
                    client.get_range_slices(new ColumnParent(columnFamily),
                            new SlicePredicate()
                                    .setSlice_range(sliceRange),
                            keyRange,
                            ConsistencyLevel.QUORUM);
            
            for (KeySlice s : slices) {
                logger.debug("Key {}", ByteBufferUtil.toString(s.key, "-"));
            }
            
            /* Note: we need to fetch columns for each row as well to remove "range ghosts" */
            List<KeySlice> result = new ArrayList<KeySlice>(slices.size());
            KeyIterationPredicate pred = new KeyIterationPredicate();
            for (KeySlice ks : slices)
                if (pred.apply(ks))
                    result.add(ks);
            return result;
        } catch (Exception e) {
            throw convertException(e);
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
    public class AbstractBufferedRowIter<T extends Token<?>> implements KeyIterator {
        
        private final int pageSize;
        private final CTConnection connection;
        private final SliceQuery columnSlice;

        private boolean isClosed;
        private boolean seenEnd;
        protected Iterator<KeySlice> ksIter;
        private KeySlice mostRecentRow;
        
        private final IPartitioner<? extends T> partitioner;
        private T nextStartToken;
        private final T endToken;
        private ByteBuffer nextStartKey;
        
        private boolean omitEndToken;
        
        public AbstractBufferedRowIter(CTConnection connection, IPartitioner<? extends T> partitioner,
                SliceQuery columnSlice, int pageSize, T startToken, T endToken, boolean omitEndToken) {
            this.pageSize = pageSize;
            this.partitioner = partitioner;
            this.nextStartToken = startToken;
            this.endToken = endToken;
            this.connection = connection;
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
                } catch (StorageException e) {
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
            
            return new StaticByteBuffer(mostRecentRow.bufferForKey());
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
                final Iterator<Entry> columns = excludeLastColumn(mostRecentRow.getColumns(),
                        columnSlice.getSliceEnd().asByteBuffer()).iterator();

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
                pool.returnObjectUnsafe(keyspace, connection);
            }
        }

        private List<KeySlice> rebuffer() throws StorageException {
            
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
        
        protected final List<KeySlice> getNextKeySlices() throws StorageException {
            return getTokenSlice(connection.getClient(), nextStartToken, endToken, columnSlice, pageSize);
        }
    }
    
    private final class AllTokensIterator<T extends Token<?>> extends AbstractBufferedRowIter<T> {

        public AllTokensIterator(CTConnection connection,
                IPartitioner<? extends T> partitioner, SliceQuery columnSlice,
                int pageSize) {
            super(connection, partitioner, columnSlice, pageSize, partitioner.getMinimumToken(), partitioner.getMinimumToken(), false);
        }        
    }
        
    private final class KeyRangeIterator<T extends Token<?>> extends AbstractBufferedRowIter<T> {
        
        public KeyRangeIterator(CTConnection connection,
                IPartitioner<? extends T> partitioner, SliceQuery columnSlice,
                int pageSize, ByteBuffer startKey, ByteBuffer endKey) throws StorageException {
            super(connection, partitioner, columnSlice, pageSize, partitioner.getToken(startKey), partitioner.getToken(endKey), true);
            
            Preconditions.checkArgument(partitioner instanceof AbstractByteOrderedPartitioner);

            // Get first slice with key range instead of token range. Token
            // ranges are start-exclusive, key ranges are start-inclusive. Both
            // are end-inclusive. If we don't make the call below, then we will
            // erroneously miss startKey.
            List<KeySlice> ks = getKeySlice(connection.getClient(), startKey, endKey, columnSlice, pageSize);
            
            this.ksIter = checkFreshSlices(ks).iterator();
        }
    }
}
