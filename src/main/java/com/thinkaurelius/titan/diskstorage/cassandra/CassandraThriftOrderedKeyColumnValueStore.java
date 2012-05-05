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
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.UncheckedGenericKeyedObjectPool;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.writeaggregation.MultiWriteKeyColumnValueStore;
import com.thinkaurelius.titan.exceptions.GraphStorageException;

public class CassandraThriftOrderedKeyColumnValueStore
	implements OrderedKeyColumnValueStore, MultiWriteKeyColumnValueStore {
	
	private final String keyspace;
	private final String columnFamily;
	private final UncheckedGenericKeyedObjectPool<String, CTConnection> pool;

	// This is the value of System.nanoTime() at startup
	private static final long t0NanoTime;
	
	/* This is the value of System.currentTimeMillis() at
	 * startup times a million (i.e. CTM in ns)
	 */
	private static final long t0NanosSinceEpoch;
	
	private static final Logger logger =
		LoggerFactory.getLogger(CassandraThriftOrderedKeyColumnValueStore.class);
	
	public CassandraThriftOrderedKeyColumnValueStore(String keyspace, String columnFamily,
                                UncheckedGenericKeyedObjectPool<String, CTConnection> pool) throws RuntimeException {
		this.keyspace = keyspace;
		this.columnFamily = columnFamily;
		this.pool = pool;
	}
	
	// Initialize the t0 variables
	static {
		
		/*
		 * This is a crude attempt to establish a correspondence
		 * between System.currentTimeMillis() and System.nanoTime().
		 * 
		 * It's susceptible to errors up to -999 us due to the
		 * limited accuracy of System.currentTimeMillis()
		 * versus that of System.nanoTime(), with an average
		 * error of about -0.5 ms.
		 * 
		 * In addition, it's susceptible to arbitrarily large
		 * error if the scheduler decides to sleep this thread
		 * in between the following time calls.
		 * 
		 * One mitigation for both errors could be to wrap
		 * this logic in a loop and combine the timing information
		 * from multiple passes into the final t0 values.
		 */
		final long t0ms = System.currentTimeMillis();
		final long t0ns = System.nanoTime();
		
		t0NanosSinceEpoch = t0ms * 1000L * 1000L;
		t0NanoTime = t0ns;
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
			ByteBuffer columnEnd, boolean startInclusive, boolean endInclusive,
			int limit, TransactionHandle txh) throws GraphStorageException {
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
		if (! ByteBufferUtil.isSmallerThan(columnStart, columnEnd)) {
			// Check for invalid arguments where columnEnd < columnStart
			if (ByteBufferUtil.isSmallerThan(columnEnd, columnStart)) {
				throw new GraphStorageException("columnStart=" + columnStart + 
						" is greater than columnEnd=" + columnEnd + ". " +
						"columnStart must be less than or equal to columnEnd");
			}
			/* Must be the case that columnStart equals columnEnd;
			 * check inclusivity and refer to get() if appropriate.
			 */
			if (startInclusive && endInclusive) {
				ByteBuffer name = columnStart.duplicate();
				ByteBuffer value = get(key, columnStart.duplicate(), txh);
				List<Entry> result = new ArrayList<Entry>(1);
				result.add(new Entry(name, value));
				return result;
			} else {
//				logger.debug(
//						"Parameters columnStart=columnEnd={}, " +
//						"startInclusive={}, endInclusive={} " + 
//						"collectively form an empty interval; " +
//						"returning an empty result list.", 
//						new Object[]{columnStart.duplicate(), startInclusive,
//								endInclusive});
				return ImmutableList.<Entry>of();
			}
		}
		
		// true: columnStart < columnEnd
		ConsistencyLevel consistency = getConsistencyLevel();
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
				// Skip columnStart if !startInclusive
				if (!startInclusive && ByteBufferUtil.isSmallerOrEqualThan(c.bufferForName(), columnStart))
					continue;
				// Skip columnEnd if !endInclusive
				if (!endInclusive && ByteBufferUtil.isSmallerOrEqualThan(columnEnd, c.bufferForName()))
					continue;
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
			ByteBuffer columnEnd, boolean startInclusive, boolean endInclusive,
			TransactionHandle txh) {
		return getSlice(key, columnStart, columnEnd, 
				startInclusive, endInclusive, Integer.MAX_VALUE, txh); 
	}

	@Override
	public void close() {
		// Do nothing
	}

	@Override
	public boolean containsKey(ByteBuffer key, TransactionHandle txh) {
		ColumnParent parent = new ColumnParent(columnFamily);
		ConsistencyLevel consistency = getConsistencyLevel();
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
				client.get(key, path, getConsistencyLevel());
			return result.getColumn().bufferForValue();
		} catch (TException e) {
			throw new GraphStorageException(e);
		} catch (TimedOutException e) {
			throw new GraphStorageException(e);
		} catch (UnavailableException e) {
			throw new GraphStorageException(e);
		} catch (InvalidRequestException e) {
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
	
	private static ConsistencyLevel getConsistencyLevel() {
		return ConsistencyLevel.ALL;
	}

	/**
	 * This returns the approximate number of nanoseconds
	 * elapsed since the UNIX Epoch.  The least significant
	 * bit is overridden to 1 or 0 depending on whether
	 * setLSB is true or false (respectively).
	 * <p>
	 * This timestamp rolls over about every 2^63 ns, or
	 * just over 292 years.  The first rollover starting
	 * from the UNIX Epoch would be sometime in 2262.
	 * 
	 * @param setLSB should the smallest bit in the
	 * 	             returned value be one?
	 * @return a timestamp as described above
	 */
	private long getNewTimestamp(final boolean setLSB) {
		final long nanosSinceEpoch = System.nanoTime() - t0NanoTime + t0NanosSinceEpoch;
		final long ts = ((nanosSinceEpoch) & 0xFFFFFFFFFFFFFFFEL) + (setLSB ? 1L : 0L);
		return ts;
	}

	@Override
	public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column,
			TransactionHandle txh) {
		ColumnParent parent = new ColumnParent(columnFamily);
		ConsistencyLevel consistency = getConsistencyLevel();
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
		// TODO Auto-generated method stub
		
	}

    @Override
    public void mutateMany(Map<ByteBuffer, com.thinkaurelius.titan.diskstorage.writeaggregation.Mutation> mutations, TransactionHandle txh) {
		
    	if (null == mutations)
    		return;
		
		long deletionTimestamp = getNewTimestamp(false);
		long additionTimestamp = getNewTimestamp(true);
		
		ConsistencyLevel consistency = getConsistencyLevel();
		
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
}
