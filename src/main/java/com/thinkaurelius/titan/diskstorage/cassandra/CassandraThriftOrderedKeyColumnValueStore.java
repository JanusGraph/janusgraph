package com.thinkaurelius.titan.diskstorage.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.GraphStorageException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.LockConfig;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.UncheckedGenericKeyedObjectPool;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.SimpleLockConfig;
import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.writeaggregation.MultiWriteKeyColumnValueStore;

/**
 * A Titan {@code OrderedKeyColumnValueStore} backed by Cassandra.
 * This uses the Cassandra Thrift API.
 * 
 * @see CassandraThriftStorageManager
 * @author Dan LaRocque <dalaro@hopcount.org>
 * 
 */
public class CassandraThriftOrderedKeyColumnValueStore
	implements OrderedKeyColumnValueStore, MultiWriteKeyColumnValueStore {
	
	private final String keyspace;
	private final String columnFamily;
	private final UncheckedGenericKeyedObjectPool<String, CTConnection> pool;
	
	private final ConsistencyLevel readConsistencyLevel;
	private final ConsistencyLevel writeConsistencyLevel;
	
	private final LockConfig internals;
	
	private static final Logger logger =
		LoggerFactory.getLogger(CassandraThriftOrderedKeyColumnValueStore.class);
	
	public CassandraThriftOrderedKeyColumnValueStore(
			String keyspace,
			String columnFamily,
            UncheckedGenericKeyedObjectPool<String, CTConnection> pool,
            ConsistencyLevel readConsistencyLevel,
            ConsistencyLevel writeConsistencyLevel,
            CassandraThriftOrderedKeyColumnValueStore lockStore,
            LocalLockMediator llm,
            byte[] rid,
            int lockRetryCount,
            long lockWaitMS,
            long lockExpireMS) throws RuntimeException {
		this.keyspace = keyspace;
		this.columnFamily = columnFamily;
		this.pool = pool;
		this.readConsistencyLevel = readConsistencyLevel;
		this.writeConsistencyLevel = writeConsistencyLevel;
		
		if (null != llm && null != lockStore) {
			this.internals = new SimpleLockConfig(this, lockStore, llm,
					rid, lockRetryCount, lockWaitMS, lockExpireMS);
		} else {
			this.internals = null;
		}
	}

	/**
	 * Call Cassandra's Thrift get_slice() method.
	 * 
	 * When columnEnd equals columnStart, and both startInclusive
	 * and endInclusive are true, then this method calls
	 * {@link #get(java.nio.ByteBuffer, java.nio.ByteBuffer, TransactionHandle)}
	 * instead of calling Thrift's getSlice() method and returns
	 * a one-element list containing the result.
	 * 
	 * When columnEnd equals columnStart and either startInclusive
	 * or endInclusive is false (or both are false), then this
	 * method returns an empty list without making any Thrift calls.
	 * 
	 * If columnEnd = columnStart + 1, and both startInclusive and
	 * startExclusive are false, then the arguments effectively form
	 * an empty interval.  In this case, as in the one previous,
	 * an empty list is returned.  However, it may not necessarily
	 * be handled efficiently; a Thrift call might still be made
	 * before returning the empty list.
	 * 
	 * @throws GraphStorageException when columnEnd < columnStart
	 * 
	 */
	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, int limit, TransactionHandle txh) throws GraphStorageException {
		// Sanity check the limit argument
		if (0 > limit) {
			logger.warn("Setting negative limit ({}) to 0", limit);
			limit = 0;
		}
		
		if (0 == limit)
			return ImmutableList.<Entry>of();
		
		ColumnParent parent = new ColumnParent(columnFamily);
		/* 
		 * Cassandra cannot handle columnStart = columnEnd.
		 * Cassandra's Thrift getSlice() throws InvalidRequestException
		 * if columnStart = columnEnd.
		 */
		if (!ByteBufferUtil.isSmallerThan(columnStart, columnEnd)) {
			// Check for invalid arguments where columnEnd < columnStart
			if (ByteBufferUtil.isSmallerThan(columnEnd, columnStart)) {
				throw new GraphStorageException("columnStart=" + columnStart + 
						" is greater than columnEnd=" + columnEnd + ". " +
						"columnStart must be less than or equal to columnEnd");
			}
                        if (0 != columnStart.remaining() && 0 != columnEnd.remaining()) {
                                logger.debug("Return empty list due to columnEnd==columnStart and neither empty");
                                return ImmutableList.<Entry>of();
                        }
		}
		
		// true: columnStart < columnEnd
		ConsistencyLevel consistency = getConsistencyLevel(txh, Operation.READ);
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
		} catch (TException e) {
			throw new GraphStorageException(e);
		} catch (TimedOutException e) {
			throw new GraphStorageException(e);
		} catch (UnavailableException e) {
			throw new GraphStorageException(e);
		} catch (InvalidRequestException e) {
			throw new GraphStorageException(e);
		} finally {
			if (null != conn)
				pool.genericReturnObject(keyspace, conn);
		}
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, TransactionHandle txh) {
		return getSlice(key, columnStart, columnEnd, Integer.MAX_VALUE, txh);
	}

	@Override
	public void close() {
		// Do nothing
	}

	@Override
	public boolean containsKey(ByteBuffer key, TransactionHandle txh) {
		ColumnParent parent = new ColumnParent(columnFamily);
		ConsistencyLevel consistency = getConsistencyLevel(txh, Operation.READ);
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
		} catch (TException e) {
			throw new GraphStorageException(e);
		} catch (TimedOutException e) {
			throw new GraphStorageException(e);
		} catch (UnavailableException e) {
			throw new GraphStorageException(e);
		} catch (InvalidRequestException e) {
			throw new GraphStorageException(e);
		} finally {
			if (null != conn)
				pool.genericReturnObject(keyspace, conn);
		}
	}

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, TransactionHandle txh) {

    	Map<ByteBuffer, com.thinkaurelius.titan.diskstorage.writeaggregation.Mutation> mutations =
    			new HashMap<ByteBuffer, com.thinkaurelius.titan.diskstorage.writeaggregation.Mutation>(1);
    	
    	com.thinkaurelius.titan.diskstorage.writeaggregation.Mutation m = new
    			com.thinkaurelius.titan.diskstorage.writeaggregation.Mutation(additions, deletions);
    	
    	mutations.put(key, m);
    	
    	mutateMany(mutations, txh);
    }

	@Override
	public ByteBuffer get(ByteBuffer key, ByteBuffer column,
			TransactionHandle txh) {
		ColumnPath path = new ColumnPath(columnFamily);
		path.setColumn(column);
		CTConnection conn = null;
		try {
			conn = pool.genericBorrowObject(keyspace);
			Cassandra.Client client = conn.getClient();
			ColumnOrSuperColumn result =
				client.get(key, path, getConsistencyLevel(txh, Operation.READ));
			return result.getColumn().bufferForValue();
		} catch (TException e) {
			throw new GraphStorageException(e);
		} catch (TimedOutException e) {
			throw new GraphStorageException(e);
		} catch (UnavailableException e) {
			throw new GraphStorageException(e);
		} catch (InvalidRequestException e) {
			e.printStackTrace();
			throw new GraphStorageException(e);
		} catch (NotFoundException e) {
			return null;
		} finally {
			if (null != conn)
				pool.genericReturnObject(keyspace, conn);
		}
	}
	
	@Override
	public boolean isLocalKey(ByteBuffer key) {
        return true;
	}
	
	private ConsistencyLevel getConsistencyLevel(TransactionHandle txh, Operation op) {
		
		if (null == txh) {
			return ConsistencyLevel.QUORUM;
		}
		
		if (op.equals(Operation.WRITE)) {
			return writeConsistencyLevel;
		} else {
			return readConsistencyLevel;
		}
	}

	@Override
	public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column,
			TransactionHandle txh) {
		ColumnParent parent = new ColumnParent(columnFamily);
		ConsistencyLevel consistency = getConsistencyLevel(txh, Operation.READ);
		SlicePredicate predicate = new SlicePredicate();
		predicate.setColumn_names(Arrays.asList(column.duplicate()));
		CTConnection conn = null;
		try {
			conn = pool.genericBorrowObject(keyspace);
			Cassandra.Client client = conn.getClient();
			List<?> result = client.get_slice(key, parent, predicate, consistency);
			return 0 < result.size();
		} catch (TException e) {
			throw new GraphStorageException(e);
		} catch (TimedOutException e) {
			throw new GraphStorageException(e);
		} catch (UnavailableException e) {
			throw new GraphStorageException(e);
		} catch (InvalidRequestException e) {
			throw new GraphStorageException(e);
		} finally {
			if (null != conn)
				pool.genericReturnObject(keyspace, conn);
		}
	}

	@Override
	public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue,
			TransactionHandle txh) {
		
		CassandraTransaction ctxh = (CassandraTransaction)txh;
		if (ctxh.isMutationStarted()) {
			throw new GraphStorageException("Attempted to obtain a lock after one or more mutations");
		}
		
		ctxh.writeBlindLockClaim(internals, key, column, expectedValue);
	}

    @Override
    public void mutateMany(Map<ByteBuffer, com.thinkaurelius.titan.diskstorage.writeaggregation.Mutation> mutations, TransactionHandle txh) {
		
    	if (null == mutations)
    		return;

    	// null txh means a CassandraTransaction is calling this method
    	if (null != txh) {
    		// non-null txh -> make sure locks are valid
    		CassandraTransaction ctxh = (CassandraTransaction)txh;
    		if (! ctxh.isMutationStarted()) {
    			// This is the first mutate call in the transaction
    			ctxh.mutationStarted();
    			// Verify all blind lock claims now
    			ctxh.verifyAllLockClaims(); // throws GSE and unlocks everything on any lock failure
    		}
    	}
    	
		long deletionTimestamp = TimestampProvider.getApproxNSSinceEpoch(false);
		long additionTimestamp = TimestampProvider.getApproxNSSinceEpoch(true);
		
		ConsistencyLevel consistency = getConsistencyLevel(txh, Operation.WRITE);
		
		// Generate Thrift-compatible batch_mutate() datastructure
		// key -> cf -> cassmutation
		Map<ByteBuffer, Map<String, List<Mutation>>> batch =
			new HashMap<ByteBuffer, Map<String, List<Mutation>>>(mutations.size());

		for (Map.Entry<ByteBuffer, com.thinkaurelius.titan.diskstorage.writeaggregation.Mutation> e : mutations.entrySet()) {

			Map<String, List<Mutation>> cfs = new HashMap<String, List<Mutation>>(1);
			List<Mutation> muts = new ArrayList<Mutation>(mutations.size());
			
			ByteBuffer key = e.getKey();

			if (null != e.getValue().getDeletions()) {
				for (ByteBuffer buf : e.getValue().getDeletions()) {
					Deletion d = new Deletion();
					SlicePredicate sp = new SlicePredicate();
					sp.addToColumn_names(buf);
					d.setPredicate(sp);
					d.setTimestamp(deletionTimestamp);
					Mutation m = new Mutation();
					m.setDeletion(d);
					muts.add(m);
				}
			}
			
			if (null != e.getValue().getAdditions()) {
				for (Entry ent : e.getValue().getAdditions()) {
					ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
					Column column = new Column(ent.getColumn());
					column.setValue(ent.getValue());
					column.setTimestamp(additionTimestamp);
					cosc.setColumn(column);
					Mutation m = new Mutation();
					m.setColumn_or_supercolumn(cosc);
					muts.add(m);
				}
			}
			
			cfs.put(columnFamily, muts);
			batch.put(key, cfs);
			
		}
		

		CTConnection conn = null;
		try {
			conn = pool.genericBorrowObject(keyspace);
			Cassandra.Client client = conn.getClient();

			client.batch_mutate(batch, consistency);
		} catch (TException ex) {
			throw new GraphStorageException(ex);
		} catch (TimedOutException ex) {
			throw new GraphStorageException(ex);
		} catch (UnavailableException ex) {
			throw new GraphStorageException(ex);
		} catch (InvalidRequestException ex) {
			throw new GraphStorageException(ex);
		} finally {
			if (null != conn)
				pool.genericReturnObject(keyspace, conn);
		}
		
    }

	@Override
	public String toString() {
		return "CassandraThriftOrderedKeyColumnValueStore[ks="
				+ keyspace + ", cf=" + columnFamily + "]";
	}
    
    private static enum Operation { READ, WRITE; }
}
