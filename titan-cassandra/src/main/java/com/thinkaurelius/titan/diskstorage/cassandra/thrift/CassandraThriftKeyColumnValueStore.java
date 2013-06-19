package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;

import java.nio.ByteBuffer;
import java.util.*;

import javax.annotation.Nullable;

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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnectionPool;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.util.stats.MetricManager;

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
    
    // Cassandra access
    private final CassandraThriftStoreManager storeManager;
    private final String keyspace;
    private final String columnFamily;
    private final CTConnectionPool pool;
    
    // Metrics setup
    private final Timer   getKeySliceTimer;
    private final Counter getKeySliceCounter;

    public CassandraThriftKeyColumnValueStore(String keyspace, String columnFamily, CassandraThriftStoreManager storeManager,
                                              CTConnectionPool pool) {
        this.storeManager = storeManager;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.pool = pool;
        
        // Metrics setup
        MetricRegistry metrics = MetricManager.INSTANCE.getRegistry();
        Class<?> myClass = CassandraThriftKeyColumnValueStore.class;
        getKeySliceTimer =
                metrics.timer(MetricRegistry.name(myClass, "getKeySlice", "time"));
        getKeySliceCounter =
                metrics.counter(MetricRegistry.name(myClass, "getKeySlice", "keyslices"));
        
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
        Preconditions.checkArgument(query.getLimit() >= 0);
        if (0 == query.getLimit())
            return Collections.emptyMap();

        ColumnParent parent = new ColumnParent(columnFamily);
        /*
		 * Cassandra cannot handle columnStart = columnEnd.
		 * Cassandra's Thrift getSlice() throws InvalidRequestException
		 * if columnStart = columnEnd.
		 */
        if (ByteBufferUtil.compare(query.getSliceStart(), query.getSliceEnd())>=0) {
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
                List<Entry> entries = new ArrayList<Entry>();

                for (ColumnOrSuperColumn r : rows.get(key)) {
                    Column c = r.getColumn();

                    // Skip column if it is equal to columnEnd because columnEnd is exclusive
                    if (sliceEndBB.equals(c.bufferForName())) {
                        continue;
                    }

                    entries.add(new ByteBufferEntry(c.bufferForName(), c.bufferForValue()));
                }

                results.put(key.duplicate(), entries);
            }

            return results;
        } catch (Exception e) {
            throw convertException(e);
        } finally {
            pool.returnObjectUnsafe(keyspace, conn);
        }
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
    public RecordIterator<StaticBuffer> getKeys(final StoreTransaction txh) throws StorageException {
        return getKeys(null, txh);
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
                                   storeManager.getPageSize());
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery keyRangeQuery, StoreTransaction txh) throws StorageException {
        final IPartitioner<?> partitioner = storeManager.getCassandraPartitioner();

        // see rant about the reason of this limitation in Astyanax implementation of this method.
        if (!(partitioner instanceof OrderPreservingPartitioner))
            throw new PermanentStorageException("This operation is only allowed when byte-ordered partitioner is used.");

        try {
            return new RowIterator(pool.borrowObject(keyspace),
                                   partitioner,
                                   keyRangeQuery.getKeyStart().asByteBuffer(),
                                   keyRangeQuery.getKeyEnd().asByteBuffer(),
                                   keyRangeQuery,
                                   storeManager.getPageSize());
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
                                           int pageSize) throws StorageException {
        return getKeySlice(client, new KeyRange().setStart_key(startKey).setEnd_key(endKey).setCount(pageSize), sliceQuery);
    }

    private Iterator<KeySlice> getKeySlice(Cassandra.Client client, Token startToken, Token endToken, SliceQuery sliceQuery, int pageSize) throws StorageException {
        return getKeySlice(client,
                           new KeyRange().setStart_token(startToken.token.toString())
                                         .setEnd_token(endToken.token.toString())
                                         .setCount(pageSize),
                           sliceQuery);
    }

    private Iterator<KeySlice> getKeySlice(Cassandra.Client client,
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


        Timer.Context timerContext = getKeySliceTimer.time();

         try {
            List<KeySlice> slices =
                    client.get_range_slices(new ColumnParent(columnFamily),
                            new SlicePredicate()
                                    .setSlice_range(new SliceRange()
                                            .setStart(ArrayUtils.EMPTY_BYTE_ARRAY)
                                            .setFinish(ArrayUtils.EMPTY_BYTE_ARRAY)
                                            .setCount(5)),
                            keyRange,
                            ConsistencyLevel.QUORUM);

            getKeySliceCounter.inc(slices.size());

            /* Note: we need to fetch columns for each row as well to remove "range ghosts" */
            return Iterators.filter(slices.iterator(), new KeyIterationPredicate());
        } catch (Exception e) {
            throw convertException(e);
        } finally {
            timerContext.stop();
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
                           int pageSize) throws StorageException {
            this.connection = connection;
            this.client = connection.getClient();
            this.partitioner = partitioner;
            this.keys = getKeySlice(client, startKey, endKey, sliceQuery, pageSize);
            this.pageSize = pageSize;
            this.sliceQuery = sliceQuery;

            if (endKey.remaining() == 0) {
                this.maximumToken = (partitioner instanceof RandomPartitioner)
                                     ? new BigIntegerToken(RandomPartitioner.MAXIMUM)
                                     : new LongToken(Murmur3Partitioner.MAXIMUM);
            } else {
                this.maximumToken = partitioner.getToken(endKey.duplicate());
            }
        }

        @Override
        public boolean hasNext() throws StorageException {
            ensureOpen();

            boolean hasNext = keys.hasNext();

            if (!hasNext && lastSeenKey != null) {
                keys = getKeySlice(client, partitioner.getToken(lastSeenKey), maximumToken, sliceQuery, pageSize);
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
                final Iterator<ColumnOrSuperColumn> columns = currentRow.getColumnsIterator();

                @Override
                public boolean hasNext() throws StorageException {
                    ensureOpen();

                    return columns.hasNext();
                }

                @Override
                public Entry next() throws StorageException {
                    ensureOpen();

                    Column column = columns.next().getColumn();
                    return new ByteBufferEntry(column.bufferForName(), column.bufferForValue());
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
