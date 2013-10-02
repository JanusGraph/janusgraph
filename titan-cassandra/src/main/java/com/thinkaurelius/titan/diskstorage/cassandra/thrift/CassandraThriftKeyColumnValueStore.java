package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.thinkaurelius.titan.diskstorage.cassandra.utils.CassandraHelper;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;

import org.apache.cassandra.dht.*;
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

import com.esotericsoftware.minlog.Log;
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
    
//    private static final ByteBuffer EIGHT_BYTE_WIDE_MAX_VALUE;
    
    private static final Pattern BROKEN_BYTE_TOKEN_PATTERN = Pattern.compile("^Token\\(bytes\\[(.+)\\]\\)$");
    
//    static {
//        EIGHT_BYTE_WIDE_MAX_VALUE = ByteBuffer.allocate(8);
//        while (EIGHT_BYTE_WIDE_MAX_VALUE.hasRemaining())
//            EIGHT_BYTE_WIDE_MAX_VALUE.put((byte)-1);
//        EIGHT_BYTE_WIDE_MAX_VALUE.flip();
//    }

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
        final IPartitioner<?> partitioner = storeManager.getCassandraPartitioner();

        if (!(partitioner instanceof RandomPartitioner) && !(partitioner instanceof Murmur3Partitioner))
            throw new PermanentStorageException("This operation is only allowed when random partitioner (md5 or murmur3) is used.");

        try {
            return new RowIterator(pool.borrowObject(keyspace),
                    partitioner,
                    ByteBuffer.wrap(ArrayUtils.EMPTY_BYTE_ARRAY),
                    ByteBuffer.wrap(ArrayUtils.EMPTY_BYTE_ARRAY),
                    sliceQuery,
                    storeManager.getPageSize(),
                    true);
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery keyRangeQuery, StoreTransaction txh) throws StorageException {
        final IPartitioner<?> partitioner = storeManager.getCassandraPartitioner();

        // see rant about the reason of this limitation in Astyanax implementation of this method.
        if (!(partitioner instanceof AbstractByteOrderedPartitioner))
            throw new PermanentStorageException("This operation is only allowed when byte-ordered partitioner is used.");

        try {
            return new RowIterator(pool.borrowObject(keyspace),
                    partitioner,
                    keyRangeQuery.getKeyStart().asByteBuffer(),
                    keyRangeQuery.getKeyEnd().asByteBuffer(),
                    keyRangeQuery,
                    storeManager.getPageSize(),
                    false);
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


    private Iterator<KeySlice> getKeySlice(Cassandra.Client client,
                                           ByteBuffer startKey,
                                           ByteBuffer endKey,
                                           SliceQuery sliceQuery,
                                           int pageSize,
                                           ByteBuffer excludeFinalKey) throws StorageException {
        return getKeySlice(client, new KeyRange().setStart_key(startKey).setEnd_key(endKey).setCount(pageSize), sliceQuery, excludeFinalKey);
    }

    private Iterator<KeySlice> getKeySlice(Cassandra.Client client, Token startToken, Token endToken, SliceQuery sliceQuery, int pageSize, ByteBuffer excludeFinalKey) throws StorageException {
        String st = sanitizeBrokenByteToken(startToken.toString());
        String et = sanitizeBrokenByteToken(endToken.toString());
        
        return getKeySlice(client,
                new KeyRange().setStart_token(st)
                        .setEnd_token(et)
                        .setCount(pageSize),
                sliceQuery, excludeFinalKey);
    }
    
    private String sanitizeBrokenByteToken(String tok) {
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
        
        // Do a cheap 1-character startsWith before unleashing the regex
        if (tok.startsWith("T")) {
            Matcher m = BROKEN_BYTE_TOKEN_PATTERN.matcher(tok);
            if (!m.matches()) {
                logger.warn("Unknown token string format: \"{}\"", tok);
            } else {
                String old = tok;
                tok = m.group(1);
                logger.warn("Rewrote token string: \"{}\" -> \"{}\"", old, tok);
            }
        }
        return tok;
    }

    private Iterator<KeySlice> getKeySlice(Cassandra.Client client,
                                           KeyRange keyRange,
                                           @Nullable SliceQuery sliceQuery,
                                           ByteBuffer excludeFinalKey) throws StorageException {
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
            
            if (null != excludeFinalKey && 0 < slices.size() && slices.get(slices.size() - 1).key.equals(excludeFinalKey)) {
                slices.remove(slices.size() - 1);
                logger.error("Deleted final slice in get_range_slices with key {}", ByteBufferUtil.bytesToHex(excludeFinalKey));
            }
            
            for (KeySlice s : slices) {
                logger.debug("Key {}", ByteBufferUtil.toString(s.key, "-"));
            }
            
            /* Note: we need to fetch columns for each row as well to remove "range ghosts" */
            return Iterators.filter(slices.iterator(), new KeyIterationPredicate());
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

    private class RowIterator implements KeyIterator {
        private final CTConnection connection;
        private final Cassandra.Client client;
        private final IPartitioner<?> partitioner;
        private final Token maximumToken;
        private final SliceQuery sliceQuery;
        private final ByteBuffer excludeFinalKey;

        private Iterator<KeySlice> keys;
        private ByteBuffer lastSeenKey = null;
        private KeySlice currentRow;
        private int pageSize;

        private boolean isClosed;

        public RowIterator(CTConnection connection,
                           IPartitioner<?> partitioner,
                           ByteBuffer startKey,
                           ByteBuffer endKey,
                           SliceQuery sliceQuery,
                           int pageSize,
                           boolean includeEndKey) throws StorageException {
            this.connection = connection;
            this.client = connection.getClient();
            this.partitioner = partitioner;
            this.pageSize = pageSize;
            this.sliceQuery = sliceQuery;
            this.excludeFinalKey = includeEndKey ? null : endKey;
            this.keys = getKeySlice(client, startKey, endKey, sliceQuery, pageSize, excludeFinalKey);

            if (endKey.remaining() == 0) {
                if (partitioner instanceof RandomPartitioner) {
                    this.maximumToken = new BigIntegerToken(RandomPartitioner.MAXIMUM);
                } else if (partitioner instanceof Murmur3Partitioner) {
                    this.maximumToken = new LongToken(Murmur3Partitioner.MAXIMUM);
                } else if (partitioner instanceof AbstractByteOrderedPartitioner) {
//                    this.maximumToken = partitioner.getToken(EIGHT_BYTE_WIDE_MAX_VALUE);
                    this.maximumToken = partitioner.getToken(ByteBufferUtil.emptyBuffer().asByteBuffer());
                } else {
                    throw new PermanentStorageException("Unknown partitioner " + partitioner);
                }
            } else {
                this.maximumToken = partitioner.getToken(endKey);
            }
        }

        @Override
        public boolean hasNext() throws StorageException {
            ensureOpen();

            boolean hasNext = keys.hasNext();

            if (!hasNext && lastSeenKey != null) {
                Token nextStartToken = partitioner.getToken(lastSeenKey);
                if (maximumToken.equals(nextStartToken)) {
                    // Cassandra's API doc says that submitting equal tokens returns effectively includes all keys on the ring
                    // That is not what we want
                    return false;
                }
                keys = getKeySlice(client, nextStartToken, maximumToken, sliceQuery, pageSize, excludeFinalKey);
                hasNext = keys.hasNext();
            }

            return hasNext;
        }

        @Override
        public StaticBuffer next() throws StorageException {
            ensureOpen();

            if (!hasNext())
                throw new NoSuchElementException();

            currentRow = keys.next();

            try {
                return new StaticByteBuffer(currentRow.bufferForKey());
            } finally {
                lastSeenKey = currentRow.bufferForKey();
            }
        }

        @Override
        public void close() throws StorageException {
            closeIterator();
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            return new RecordIterator<Entry>() {
                final Iterator<Entry> columns = excludeLastColumn(currentRow.getColumns(),
                        sliceQuery.getSliceEnd().asByteBuffer()).iterator();

                @Override
                public boolean hasNext() throws StorageException {
                    ensureOpen();
                    return columns.hasNext();
                }

                @Override
                public Entry next() throws StorageException {
                    ensureOpen();
                    return columns.next();
                }

                @Override
                public void close() throws StorageException {
                    closeIterator();
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
    }
}
