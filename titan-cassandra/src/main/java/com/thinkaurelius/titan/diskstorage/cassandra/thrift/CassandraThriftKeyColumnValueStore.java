package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.UncheckedGenericKeyedObjectPool;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;

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

import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;

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

    private final CassandraThriftStoreManager storeManager;
    private final String keyspace;
    private final String columnFamily;
    private final UncheckedGenericKeyedObjectPool<String, CTConnection> pool;

    public CassandraThriftKeyColumnValueStore(String keyspace, String columnFamily, CassandraThriftStoreManager storeManager,
                                              UncheckedGenericKeyedObjectPool<String, CTConnection> pool) {
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
        Preconditions.checkArgument(query.getLimit() >= 0);
        if (0 == query.getLimit()) return ImmutableList.<Entry>of();

        ColumnParent parent = new ColumnParent(columnFamily);        /*
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
                return ImmutableList.<Entry>of();
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
            conn = pool.genericBorrowObject(keyspace);
            Cassandra.Client client = conn.getClient();
            List<ColumnOrSuperColumn> rows = client.get_slice(query.getKey().asByteBuffer(), parent, predicate, consistency);
			/*
			 * The final size of the "result" List may be at most rows.size().
			 * However, "result" could also be up to two elements smaller than
			 * rows.size(), depending on startInclusive and endInclusive
			 */
            List<Entry> result = new ArrayList<Entry>(rows.size());
            
            ByteBuffer sliceEndBB = query.getSliceEnd().asByteBuffer();
            
            for (ColumnOrSuperColumn r : rows) {
                Column c = r.getColumn();

                // Skip column if it is equal to columnEnd because columnEnd is exclusive
                if (sliceEndBB.equals(c.bufferForName())) {
                    continue;
                }

                result.add(new ByteBufferEntry(c.bufferForName(), c.bufferForValue()));
            }
            return result;
        } catch (Exception e) {
            throw convertException(e);
        } finally {
            if (null != conn)
                pool.genericReturnObject(keyspace, conn);
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
            conn = pool.genericBorrowObject(keyspace);
            Cassandra.Client client = conn.getClient();
            List<?> result = client.get_slice(key.asByteBuffer(), parent, predicate, consistency);
            return 0 < result.size();
        } catch (Exception e) {
            throw convertException(e);
        } finally {
            if (null != conn)
                pool.genericReturnObject(keyspace, conn);
        }
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue,
                            StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordIterator<StaticBuffer> getKeys(StoreTransaction txh) throws StorageException {
        CTConnection conn = null;

        final IPartitioner<?> partitioner = storeManager.getCassandraPartitioner();

        if (!(partitioner instanceof RandomPartitioner) && !(partitioner instanceof Murmur3Partitioner))
            throw new PermanentStorageException("This operation is only allowed when random partitioner (md5 or murmur3) is used.");

        final Token maximumToken = (partitioner instanceof RandomPartitioner)
                                    ? new BigIntegerToken(RandomPartitioner.MAXIMUM)
                                    : new LongToken(Murmur3Partitioner.MAXIMUM);
        try {
            conn = pool.genericBorrowObject(keyspace);
            final Cassandra.Client client = conn.getClient();

            return new RecordIterator<StaticBuffer>() {
                Iterator<KeySlice> keys = getKeySlice(client,
                                                      ArrayUtils.EMPTY_BYTE_ARRAY,
                                                      ArrayUtils.EMPTY_BYTE_ARRAY,
                        storeManager.getPageSize());

                private ByteBuffer lastSeenKey = null;

                @Override
                public boolean hasNext() throws StorageException {
                    boolean hasNext = keys.hasNext();

                    if (!hasNext && lastSeenKey != null) {
                        keys = getKeySlice(client, partitioner.getToken(lastSeenKey), maximumToken, storeManager.getPageSize());
                        hasNext = keys.hasNext();
                    }

                    return hasNext;
                }

                @Override
                public StaticBuffer next() throws StorageException {
                    if (!hasNext())
                        throw new NoSuchElementException();

                    KeySlice slice = keys.next();

                    try {
                        return new StaticByteBuffer(slice.bufferForKey());
                    } finally {
                        lastSeenKey = slice.bufferForKey();
                    }
                }

                @Override
                public void close() throws StorageException {
                    // nothing to clean-up here
                }
            };
        } catch (Exception e) {
            throw convertException(e);
        } finally {
            if (conn != null)
                pool.genericReturnObject(keyspace, conn);
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

    static final StorageException convertException(Throwable e) {
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


    private Iterator<KeySlice> getKeySlice(Cassandra.Client client, byte[] startKey, byte[] endKey, int pageSize) throws StorageException {
        return getKeySlice(client, new KeyRange().setStart_key(startKey).setEnd_key(endKey).setCount(pageSize));
    }

    private Iterator<KeySlice> getKeySlice(Cassandra.Client client, Token startToken, Token endToken, int pageSize) throws StorageException {
        return getKeySlice(client, new KeyRange().setStart_token(startToken.token.toString()).setEnd_token(endToken.token.toString()).setCount(pageSize));
    }

    private Iterator<KeySlice> getKeySlice(Cassandra.Client client, KeyRange keyRange) throws StorageException {
        try {
            /* Note: we need to fetch columns for each row as well to remove "range ghosts" */
            return Iterators.filter(client.get_range_slices(new ColumnParent(columnFamily),
                                                            new SlicePredicate()
                                                                    .setSlice_range(new SliceRange()
                                                                                         .setStart(ArrayUtils.EMPTY_BYTE_ARRAY)
                                                                                         .setFinish(ArrayUtils.EMPTY_BYTE_ARRAY)
                                                                                         .setCount(5)),
                                                            keyRange,
                                                            ConsistencyLevel.QUORUM).iterator(), new KeyIterationPredicate());
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    private static class KeyIterationPredicate implements Predicate<KeySlice> {
        @Override
        public boolean apply(@Nullable KeySlice row) {
            return (row == null) ? false : row.getColumns().size() > 0;
        }
    }
}
