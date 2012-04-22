package com.thinkaurelius.titan.diskstorage.cassandra;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.UncheckedGenericKeyedObjectPool;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.writeaggregation.MultiWriteKeyColumnValueStore;
import com.thinkaurelius.titan.exceptions.GraphStorageException;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class CassandraThriftOrderedKeyColumnValueStore
	implements OrderedKeyColumnValueStore, MultiWriteKeyColumnValueStore {
	
	private final String keyspace;
	private final String columnFamily;
	private final UncheckedGenericKeyedObjectPool<String, CTConnection> pool;
	
	private static final Logger logger =
		LoggerFactory.getLogger(CassandraThriftOrderedKeyColumnValueStore.class);
	
	public CassandraThriftOrderedKeyColumnValueStore(String keyspace, String columnFamily,
                                UncheckedGenericKeyedObjectPool<String, CTConnection> pool) throws RuntimeException {
		this.keyspace = keyspace;
		this.columnFamily = columnFamily;
		this.pool = pool;
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
	public void delete(ByteBuffer key, List<ByteBuffer> columns,
			TransactionHandle txh) {
		ColumnPath path = new ColumnPath(columnFamily);
		long timestamp = getNewTimestamp();
		ConsistencyLevel consistency = getConsistencyLevel();
		
		CTConnection conn = null;
		try {
			conn = pool.genericBorrowObject(keyspace);
			Cassandra.Client client = conn.getClient();
			for (ByteBuffer col : columns) {
				path.setColumn(col);
				client.remove(key, path, timestamp, consistency);
			}
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
	public void insert(ByteBuffer key, List<Entry> entries,
			TransactionHandle txh) {
		ColumnParent parent = new ColumnParent(columnFamily);
		long timestamp = getNewTimestamp();
		ConsistencyLevel consistency = getConsistencyLevel();
		CTConnection conn = null;
		try {
			conn = pool.genericBorrowObject(keyspace);
			Cassandra.Client client = conn.getClient();
			for (Entry e : entries) {
				Column column = new Column();
				column.setName(e.getColumn().duplicate());
				column.setValue(e.getValue().duplicate());
				column.setTimestamp(timestamp);
				client.insert(key.duplicate(), parent, column, consistency);
			}
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
	public boolean isLocalKey(ByteBuffer key) {
        return true;
	}
	
	private static ConsistencyLevel getConsistencyLevel() {
		return ConsistencyLevel.ALL;
	}
	
	private static long getNewTimestamp() {
		return System.currentTimeMillis();
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
	public void acquireLock(ByteBuffer key, ByteBuffer column,
			TransactionHandle txh) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void insertMany(Map<ByteBuffer, List<Entry>> insertions,
			TransactionHandle txh) {
		long timestamp = getNewTimestamp();
		
		// Generate Thrift-compatible batch_mutate() datastructure
		Map<ByteBuffer, Map<String, List<Mutation>>> batch =
			new HashMap<ByteBuffer, Map<String, List<Mutation>>>(insertions.size());
		
		for (Map.Entry<ByteBuffer, List<Entry>> ins : insertions.entrySet()) {
			ByteBuffer key = ins.getKey();
			
			List<Mutation> mutationsForCurrentKeyAndCF =
				new ArrayList<Mutation>(ins.getValue().size());
			for (Entry ent : ins.getValue()) {
				Mutation m = new Mutation();
				
				Column thriftCol = new Column();
				thriftCol.setName(ent.getColumn());
				thriftCol.setValue(ent.getValue());
				thriftCol.setTimestamp(timestamp);
				ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
				cosc.setColumn(thriftCol);
				m.setColumn_or_supercolumn(cosc);

				mutationsForCurrentKeyAndCF.add(m);
			}
			
			batch.put(key, ImmutableMap.of(columnFamily, mutationsForCurrentKeyAndCF));
		}

		batchMutate(batch);
	}

	@Override
	public void deleteMany(Map<ByteBuffer, List<ByteBuffer>> deletions,
			TransactionHandle txh) {
		long timestamp = getNewTimestamp();
		
		// Generate Thrift-compatible batch_mutate() datastructure
		Map<ByteBuffer, Map<String, List<Mutation>>> batch =
			new HashMap<ByteBuffer, Map<String, List<Mutation>>>(deletions.size());
		
		for (Map.Entry<ByteBuffer, List<ByteBuffer>> ins : deletions.entrySet()) {
			ByteBuffer key = ins.getKey();
			
			List<Mutation> mutationsForCurrentKeyAndCF =
				new ArrayList<Mutation>(ins.getValue().size());
			for (ByteBuffer column : ins.getValue()) {
				Mutation m = new Mutation();
				
				SlicePredicate p = new SlicePredicate();
				p.setColumn_names(ImmutableList.of(column));
				Deletion d = new Deletion();
				d.setTimestamp(timestamp);
				d.setPredicate(p);
				m.setDeletion(d);

				mutationsForCurrentKeyAndCF.add(m);
			}
			
			batch.put(key, ImmutableMap.of(columnFamily, mutationsForCurrentKeyAndCF));
		}

		batchMutate(batch);
	}
	
	private void batchMutate(Map<ByteBuffer, Map<String, List<Mutation>>> batch) {

		ConsistencyLevel consistency = getConsistencyLevel();
		CTConnection conn = null;
		try {
			conn = pool.genericBorrowObject(keyspace);
			Cassandra.Client client = conn.getClient();

			client.batch_mutate(batch, consistency);
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

}
