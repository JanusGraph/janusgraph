package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.UncheckedGenericKeyedObjectPool;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Mutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
                                ByteBuffer columnEnd, int limit, StoreTransaction txh) throws StorageException {
        Preconditions.checkArgument(limit >= 0);
        if (0 == limit) return ImmutableList.<Entry>of();

        ColumnParent parent = new ColumnParent(columnFamily);        /*
		 * Cassandra cannot handle columnStart = columnEnd.
		 * Cassandra's Thrift getSlice() throws InvalidRequestException
		 * if columnStart = columnEnd.
		 */
        if (!ByteBufferUtil.isSmallerThan(columnStart, columnEnd)) {
            // Check for invalid arguments where columnEnd < columnStart
            if (ByteBufferUtil.isSmallerThan(columnEnd, columnStart)) {
                throw new PermanentStorageException("columnStart=" + columnStart +
                        " is greater than columnEnd=" + columnEnd + ". " +
                        "columnStart must be less than or equal to columnEnd");
            }
            if (0 != columnStart.remaining() && 0 != columnEnd.remaining()) {
                logger.debug("Return empty list due to columnEnd==columnStart and neither empty");
                return ImmutableList.<Entry>of();
            }
        }

        // true: columnStart < columnEnd
        ConsistencyLevel consistency = getTx(txh).getReadConsistencyLevel().getThriftConsistency();
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange();
        range.setCount(limit);
        range.setStart(columnStart);
        range.setFinish(columnEnd);
        predicate.setSlice_range(range);


        CTConnection conn = null;
        try {
            conn = pool.genericBorrowObject(keyspace);
            Cassandra.Client client = conn.getClient();
            List<ColumnOrSuperColumn> rows = client.get_slice(key, parent, predicate, consistency);
			/*
			 * The final size of the "result" List may be at most rows.size().
			 * However, "result" could also be up to two elements smaller than
			 * rows.size(), depending on startInclusive and endInclusive
			 */
            List<Entry> result = new ArrayList<Entry>(rows.size());
            for (ColumnOrSuperColumn r : rows) {
                Column c = r.getColumn();

                // Skip column if it is equal to columnEnd because columnEnd is exclusive
                if (columnEnd.equals(c.bufferForName())) continue;

                result.add(new Entry(c.bufferForName(), c.bufferForValue()));
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
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
                                ByteBuffer columnEnd, StoreTransaction txh) throws StorageException {
        return getSlice(key, columnStart, columnEnd, Integer.MAX_VALUE, txh);
    }

    @Override
    public void close() {
        // Do nothing
    }

    @Override
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {
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
            List<?> result = client.get_slice(key, parent, predicate, consistency);
            return 0 < result.size();
        } catch (Exception e) {
            throw convertException(e);
        } finally {
            if (null != conn)
                pool.genericReturnObject(keyspace, conn);
        }
    }


    @Override
    public ByteBuffer get(ByteBuffer key, ByteBuffer column,
                          StoreTransaction txh) throws StorageException {
        ColumnPath path = new ColumnPath(columnFamily);
        path.setColumn(column);
        CTConnection conn = null;
        try {
            conn = pool.genericBorrowObject(keyspace);
            Cassandra.Client client = conn.getClient();
            ColumnOrSuperColumn result =
                    client.get(key, path, getTx(txh).getReadConsistencyLevel().getThriftConsistency());
            return result.getColumn().bufferForValue();
        } catch (NotFoundException e) {
            return null;
        } catch (Exception e) {
            throw convertException(e);
        } finally {
            if (null != conn)
                pool.genericReturnObject(keyspace, conn);
        }
    }


    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column,
                                     StoreTransaction txh) throws StorageException {
        ColumnParent parent = new ColumnParent(columnFamily);
        ConsistencyLevel consistency = getTx(txh).getReadConsistencyLevel().getThriftConsistency();
        SlicePredicate predicate = new SlicePredicate();
        predicate.setColumn_names(Arrays.asList(column.duplicate()));
        CTConnection conn = null;
        try {
            conn = pool.genericBorrowObject(keyspace);
            Cassandra.Client client = conn.getClient();
            List<?> result = client.get_slice(key, parent, predicate, consistency);
            return 0 < result.size();
        } catch (Exception ex) {
            throw convertException(ex);
        } finally {
            if (null != conn)
                pool.genericReturnObject(keyspace, conn);
        }
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue,
                            StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }


    @Override
    public String getName() {
        return columnFamily;
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransaction txh) throws StorageException {
        Map<ByteBuffer, Mutation> mutations = ImmutableMap.of(key, new Mutation(additions, deletions));
        mutateMany(mutations, txh);
    }

    public void mutateMany(Map<ByteBuffer, Mutation> mutations, StoreTransaction txh) throws StorageException {
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

}
