package com.thinkaurelius.titan.diskstorage.cassandra;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.UncheckedGenericKeyedObjectPool;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
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
	private final CassandraThriftNodeIDMapper mapper;
	private final UncheckedGenericKeyedObjectPool<String, CTConnection> pool;
	
	private static final Logger logger =
		LoggerFactory.getLogger(CassandraThriftOrderedKeyColumnValueStore.class);
	
	public CassandraThriftOrderedKeyColumnValueStore(String keyspace, String columnFamily, UncheckedGenericKeyedObjectPool<String, CTConnection> pool, CassandraThriftNodeIDMapper mapper) throws RuntimeException {
		this.keyspace = keyspace;
		this.columnFamily = columnFamily;
		this.pool = pool;
		this.mapper = mapper;
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
		long l = key.duplicate().getLong(); // redundancy inside isNodeLocal()
		return mapper.isNodeLocal(l);
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
	public void acquireLock(ByteBuffer key, ByteBuffer column, LockType type,
			TransactionHandle txh) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Entry> getLimitedSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, boolean startInclusive, boolean endInclusive,
			int limit, TransactionHandle txh) {
		List<Entry> tentativeResults = 
			getSlice(key, columnStart, columnEnd,
					 startInclusive, endInclusive, limit + 1, txh);
		if (limit < tentativeResults.size()) {
			return null;
		}
		return tentativeResults;
	}
	
	/**
	 * Call Cassandra's get_range_slices() method.
	 * 
	 * When keyStart equals keyEnd, and both startKeyInc and endKeyInc
	 * are true, then this method calls {@link #getKeySlice(java.nio.ByteBuffer,
	 * java.nio.ByteBuffer, boolean, boolean, java.nio.ByteBuffer, java.nio.ByteBuffer, boolean,
	 * boolean, int, int, TransactionHandle)} and returns a one-element
	 * map containing an entry from keyStart/keyEnd to the result of
	 * {@code getKeySlice(...)}.
	 * 
	 * When keyStart equals keyEnd, and either startKeyInc or endKeyInc
	 * are false (or both are false), then this method returns an empty
	 * map.
	 * 
	 * When keyEnd equals keyStart + 1, and both startKeyInc and endKeyInc
	 * are false, then, as in the previous case, an empty map is returned.
	 * However, this case might not be handled efficiently.  This method
	 * might still make Thrift calls before returning the empty map.
	 * 
	 * The {@link #getSlice(java.nio.ByteBuffer, java.nio.ByteBuffer, java.nio.ByteBuffer, boolean,
	 * boolean, int, TransactionHandle)} method describes how the 
	 * startColumnIncl and endColumnIncl arguments are interpreted with
	 * respect to the columnStart and columnEnd methods.  However,
	 * in any case where {@code getSlice(...)} would return an empty list,
	 * this method instead omits the affected key from the mapping.
	 * Equivalently, the values of the map returned by this method are
	 * always lists with one or more elements.
	 * 
	 * @throws GraphStorageException when keyEnd < keyStart or
	 *         columnEnd < columnStart
	 */
	@Override
	public Map<ByteBuffer, List<Entry>> getKeySlice(ByteBuffer keyStart,
			ByteBuffer keyEnd, boolean startKeyInc, boolean endKeyInc,
			ByteBuffer columnStart, ByteBuffer columnEnd,
			boolean startColumnIncl, boolean endColumnIncl, int keyLimit,
			int columnLimit, TransactionHandle txh)
			throws GraphStorageException {
		Map<ByteBuffer, List<Entry>> result =
			new HashMap<ByteBuffer, List<Entry>>();
		// Check column/key start/end
		if (ByteBufferUtil.isSmallerThan(keyEnd, keyStart)) {
			throw new GraphStorageException("keyStart=" + keyStart + 
					" is greater than keyEnd=" + keyEnd + ". " +
					"keyStart must be less than or equal to keyEnd");
		}
		if (ByteBufferUtil.isSmallerThan(columnEnd, columnStart)) {
			throw new GraphStorageException("columnStart=" + columnStart + 
					" is greater than columnEnd=" + columnEnd + ". " +
					"columnStart must be less than or equal to columnEnd");
		}
		
		// Sanity-check keyLimit and columnLimit
		if (0 > keyLimit) {
			logger.warn("Setting negative keyLimit ({}) to 0", keyLimit);
			keyLimit = 0;
		}
		if (0 > columnLimit) {
			logger.warn("Setting negative columnLimit ({}) to 0", columnLimit);
			columnLimit = 0;
		}
		
		// keyLimit=0 or columnLimit=0 is a trivial but acceptable case
		if (0 == keyLimit || 0 == columnLimit)
			return result;
		
		// Check for special case: keyStart = keyEnd
		if (ByteBufferUtil.isSmallerOrEqualThan(keyEnd, keyStart)) {
			if (startKeyInc && endKeyInc) {
				ByteBuffer key = keyStart.duplicate();
				List<Entry> entries = getSlice(key, columnStart, columnEnd, 
						startColumnIncl, endColumnIncl, columnLimit, txh);
				if (0 < entries.size())
					result.put(key, entries);
				return result;
			} else {
//				logger.debug(
//						"Parameters keyStart=keyEnd={}, " +
//						"startKeyInc={}, endKeyInc={} " + 
//						"collectively form an empty interval; " +
//						"returning an empty result map.", 
//						new Object[]{keyStart.duplicate(), startKeyInc,
//								endKeyInc});
				return ImmutableMap.<ByteBuffer, List<Entry>>of();
			}
		}
		
		
		CTConnection conn = null;
		try {
			conn = pool.genericBorrowObject(keyspace);
			Cassandra.Client client = conn.getClient();
			
			ConsistencyLevel consistency = getConsistencyLevel();
			ColumnParent columnParent = new ColumnParent(columnFamily);
			SliceRange sliceRange = new SliceRange( // slice range affects columns
					columnStart.duplicate(),
					columnEnd.duplicate(),
					false, // reversed=false
					columnLimit);
			SlicePredicate slicePredicate = new SlicePredicate();
			slicePredicate.setSlice_range(sliceRange);
			KeyRange keyRange = new KeyRange();// key range affects keys
			keyRange.start_key = keyStart.duplicate();  // inclusive
			keyRange.end_key = keyEnd.duplicate();  // inclusive
			keyRange.count = keyLimit;
			
			// Thrift call
			List<KeySlice> slices = client.get_range_slices(columnParent, slicePredicate, keyRange, consistency);
			
			// Transfer Thrift.KeySlice list into result map
			for (KeySlice s : slices) {
				byte[] rawKey = s.getKey();
				ByteBuffer key = ByteBuffer.wrap(rawKey);
				// Check key against inclusive/exclusive constraint
				if (!startKeyInc && ByteBufferUtil.isSmallerOrEqualThan(key, keyStart))
					continue;
				if (!endKeyInc && !ByteBufferUtil.isSmallerThan(key, keyEnd))
					continue;
				
				assert !result.containsKey(key);
				List<Entry> l = new ArrayList<Entry>(s.getColumnsSize());
				for (ColumnOrSuperColumn csc : s.getColumns()) {
					Column c = csc.getColumn();
					// Check column against inclusive/exclusive constraint
					if (!startColumnIncl && ByteBufferUtil.isSmallerOrEqualThan(c.bufferForName(), columnStart))
						continue;
					if (!endColumnIncl && !ByteBufferUtil.isSmallerThan(c.bufferForName(), columnEnd))
						continue;
					
					l.add(new Entry(c.bufferForName(), c.bufferForValue()));
				}
				if (0 < l.size())
					result.put(key, l);
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
	public Map<ByteBuffer, List<Entry>> getLimitedKeySlice(ByteBuffer keyStart,
			ByteBuffer keyEnd, boolean startKeyInc, boolean endKeyInc,
			ByteBuffer columnStart, ByteBuffer columnEnd,
			boolean startColumnIncl, boolean endColumnIncl, int keyLimit,
			int columnLimit, TransactionHandle txh) {
		Map<ByteBuffer, List<Entry>> tentativeResult = getKeySlice(keyStart,
				keyEnd, startKeyInc, endKeyInc, columnStart, columnEnd,
				startColumnIncl, endColumnIncl, keyLimit + 1, columnLimit + 1,
				txh);
		// check whether keyLimit was exceeded
		if (keyLimit < tentativeResult.size())
			return null;
		// check whether columnLimit was exceeded
		for (List<Entry> l : tentativeResult.values())
			if (columnLimit < l.size())
				return null;
		return tentativeResult;
	}

	@Override
	public Map<ByteBuffer, List<Entry>> getKeySlice(ByteBuffer keyStart,
			ByteBuffer keyEnd, boolean startKeyInc, boolean endKeyInc,
			ByteBuffer columnStart, ByteBuffer columnEnd,
			boolean startColumnIncl, boolean endColumnIncl, TransactionHandle txh) {
		/*
		 * Setting keyLimit and columnLimit both to Integer.MAX_VALUE
		 * caused my testing Cassandra instance to run out of memory
		 * when answering the resulting get_range_slices query, even if
		 * the actual results returned by the query are tiny.  Cassandra
		 * is probably allocating some or all of the result storage space
		 * eagerly.  We should either eliminate this unlimited getKeySlice
		 * method or make the hard-coded Integer.MAX_VALUE-derived constants
		 * into configurable values.
		 */
		return getKeySlice(keyStart, keyEnd, startKeyInc, endKeyInc,
				columnStart, columnEnd, startColumnIncl, endColumnIncl, 
				Integer.MAX_VALUE / 1024, Integer.MAX_VALUE / 1024, txh);
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
