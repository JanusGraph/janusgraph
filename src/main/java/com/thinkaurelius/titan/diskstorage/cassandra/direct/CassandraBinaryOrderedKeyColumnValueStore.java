package com.thinkaurelius.titan.diskstorage.cassandra.direct;

import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.exceptions.GraphStorageException;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class CassandraBinaryOrderedKeyColumnValueStore
	implements OrderedKeyColumnValueStore, MultiWriteKeyColumnValueStore {

	private static final Logger log =
		LoggerFactory.getLogger(
				CassandraBinaryOrderedKeyColumnValueStore.class);

	private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

	private final String keyspace;
	private final String columnFamily;
//	private final AtomicLong ts = new AtomicLong();

	public CassandraBinaryOrderedKeyColumnValueStore(String keyspace, String columnFamily) {
		this.keyspace = keyspace;
		this.columnFamily = columnFamily;

		assert null != this.keyspace;
		assert null != this.columnFamily;

		ensureColumnFamilyExists(columnFamily);
	}

	private ConsistencyLevel getConsistencyLevel() {
		return ConsistencyLevel.ONE;
	}

	private long getCurrentTimestamp() {
		return System.currentTimeMillis();
	}

	@Override
	public void insert(ByteBuffer key, List<Entry> entries,
			TransactionHandle txh) throws GraphStorageException {

		RowMutation inserts = new RowMutation(keyspace, key.duplicate()); // TODO maybe: remove the key.duplicate() call

        long timestamp = getCurrentTimestamp();

		for (Entry e : entries) {
			QueryPath path = new QueryPath(columnFamily, null,
					e.getColumn().duplicate());
			inserts.add(path, e.getValue().duplicate(), timestamp);
		}

		mutate(inserts);
	}

	@Override
	public void insertMany(Map<ByteBuffer, List<Entry>> insertions,
			TransactionHandle txh) {

		final long timestamp = getCurrentTimestamp();

		List<RowMutation> commands = new ArrayList<RowMutation>(insertions.size());

		for (Map.Entry<ByteBuffer, List<Entry>> ins : insertions.entrySet()) {
			ByteBuffer key = ins.getKey();
			List<Entry> entries = ins.getValue();

			RowMutation cmd = new RowMutation(keyspace, key);
			for (Entry entry : entries) {
				ByteBuffer column = entry.getColumn();
				ByteBuffer value = entry.getValue();
				cmd.add(new QueryPath(columnFamily, null, column), value, timestamp);
			}

			commands.add(cmd);
		}

		mutate(commands);
	}

	@Override
	public void delete(ByteBuffer key, List<ByteBuffer> columns,
			TransactionHandle txh) throws GraphStorageException {

		RowMutation deletes = new RowMutation(keyspace, key.duplicate());

		long timestamp = getCurrentTimestamp();

		for (ByteBuffer col : columns) {
			QueryPath path = new QueryPath(columnFamily, null,
					col.duplicate());
			deletes.delete(path, timestamp);
		}

		mutate(deletes);
	}

	@Override
	public void deleteMany(Map<ByteBuffer, List<ByteBuffer>> deletions,
			TransactionHandle txh) {
		final long timestamp = getCurrentTimestamp();

		List<RowMutation> commands = new ArrayList<RowMutation>(deletions.size());

		for (Map.Entry<ByteBuffer, List<ByteBuffer>> del : deletions.entrySet()) {
			ByteBuffer key = del.getKey();
			List<ByteBuffer> cols = del.getValue();

			RowMutation cmd = new RowMutation(keyspace, key);
			for (ByteBuffer column : cols) {
				cmd.delete(new QueryPath(columnFamily, null, column), timestamp);
			}

			commands.add(cmd);
		}

		mutate(commands);
	}

	@Override
	public ByteBuffer get(ByteBuffer key, ByteBuffer column,
			TransactionHandle txh) throws GraphStorageException {

		QueryPath slicePath = new QueryPath(columnFamily);

		SliceByNamesReadCommand namesCmd =
			new SliceByNamesReadCommand(keyspace, key.duplicate(), slicePath,
					Arrays.asList(column.duplicate()));

		List<Row> rows = read(namesCmd);
		if (null == rows || 0 == rows.size())
			return null;

		if (1 < rows.size())
			throw new GraphStorageException("Received " + rows.size()
					+ " rows from a single-key-column cassandra read");

		assert 1 == rows.size();

		Row r = Iterators.getOnlyElement(rows.iterator());
		ColumnFamily cf = r.cf;
		if (null == cf)
			return null;

		if (cf.isMarkedForDelete())
			return null;

		IColumn c = cf.getColumn(column.duplicate());
		if (null == c)
			return null;

		if (c.isMarkedForDelete())
			return null;

		return org.apache.cassandra.utils.ByteBufferUtil.clone(c.value());
	}

	@Override
	public boolean containsKey(ByteBuffer key, TransactionHandle txh) {

		QueryPath slicePath = new QueryPath(columnFamily);
        ReadCommand sliceCmd = new SliceFromReadCommand(
                keyspace,     // Keyspace name
                key.duplicate(),          // Row key
                slicePath,       // ColumnFamily
                EMPTY_BYTE_BUFFER, // Start column name (empty means begin at first result)
                EMPTY_BYTE_BUFFER, // End column name (empty means max out the count)
                false,           // Reverse results? (false=no)
                1);           // Max count of Columns to return

        List<Row> rows = read(sliceCmd);

        if (null == rows || 0 == rows.size())
        	return false;

        // Find at least one live column
        for (Row r : rows) {
        	if (null == r || null == r.cf)
        		continue;

        	if (r.cf.isMarkedForDelete())
        		continue;

        	Map<ByteBuffer, IColumn> cmap = r.cf.getColumnsMap();

        	if (null == cmap)
        		continue;

        	for (IColumn ic : cmap.values())
        		if (!ic.isMarkedForDelete())
        			return true;
        }

        return false;
	}

	@Override
	public boolean isLocalKey(ByteBuffer key) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void close() throws GraphStorageException {
		// Noop
	}

	@Override
	public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column,
			TransactionHandle txh) {
		ByteBuffer bb = get(key, column, txh);
		return null != bb;
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
		List<Entry> tentativeResults = getSlice(key, columnStart, columnEnd,
				startInclusive, endInclusive, limit + 1, txh);
		if (limit < tentativeResults.size())
			return null;
		return tentativeResults;
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, boolean startInclusive, boolean endInclusive,
			int limit, TransactionHandle txh) {

//		if (!startInclusive && limit < Integer.MAX_VALUE)
//			limit++;

		QueryPath slicePath = new QueryPath(columnFamily);
        ReadCommand sliceCmd = new SliceFromReadCommand(
                keyspace,        // Keyspace name
                key.duplicate(), // Row key
                slicePath,       // ColumnFamily
                columnStart.duplicate(),     // Start column name (empty means begin at first result)
                columnEnd.duplicate(),       // End column name (empty means max out the count)
                false,           // Reverse results? (false=no)
                limit);          // Max count of Columns to return

        List<Row> slice = read(sliceCmd);

        int sliceSize = slice.size();

        if (null == slice || 0 == sliceSize)
        	return new ArrayList<Entry>(0);

        if (1 < sliceSize)
        	throw new GraphStorageException("Received " + sliceSize + " rows for single key!");

        Row r = Iterators.getOnlyElement(slice.iterator());
        ColumnFamily cf = r.cf;

        if (null == cf) {
        	log.warn("null ColumnFamily (\"{}\")", columnFamily);
        	return new ArrayList<Entry>(0);
        }

        if (cf.isMarkedForDelete())
        	return new ArrayList<Entry>(0);

        return cfToEntries(cf, columnStart, startInclusive,
        		columnEnd, endInclusive);
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, boolean startInclusive, boolean endInclusive,
			TransactionHandle txh) {
		return getSlice(key, columnStart, columnEnd, startInclusive,
				endInclusive, 1024 * 1024, txh);
	}

	@Override
	public Map<ByteBuffer, List<Entry>> getKeySlice(ByteBuffer keyStart,
			ByteBuffer keyEnd, boolean startKeyInc, boolean endKeyInc,
			ByteBuffer columnStart, ByteBuffer columnEnd,
			boolean startColumnIncl, boolean endColumnIncl, int keyLimit,
			int columnLimit, TransactionHandle txh) {

//		if (!startKeyInc && keyLimit < Integer.MAX_VALUE)
//			keyLimit++;
//		
//		if (!startColumnIncl && columnLimit < Integer.MAX_VALUE)
//			columnLimit++;

		IPartitioner<?> p = StorageService.getPartitioner();
		ColumnParent parent = new ColumnParent(columnFamily);
		SlicePredicate predicate = new SlicePredicate();
		predicate.column_names = null;
		predicate.slice_range = new SliceRange(columnStart.duplicate(), columnEnd.duplicate(), false, columnLimit);
		Bounds bounds = new Bounds(p.getToken(keyStart.duplicate()), p.getToken(keyEnd.duplicate()));

		RangeSliceCommand cmd = new RangeSliceCommand(keyspace, parent, predicate, bounds, keyLimit);

		List<Row> rows = readSlice(cmd);

		if (null == rows || 0 == rows.size())
			return new HashMap<ByteBuffer, List<Entry>>();

		Map<ByteBuffer, List<Entry>> result = new HashMap<ByteBuffer, List<Entry>>();
		for (Row r : rows) {
			if (r.cf.isMarkedForDelete())
				continue;

			ByteBuffer key = org.apache.cassandra.utils.ByteBufferUtil.clone(r.key.key);

			if (!startKeyInc
					&& ByteBufferUtil.isSmallerOrEqualThan(key, keyStart))
				continue;

			if (!endKeyInc
					&& ByteBufferUtil.isSmallerOrEqualThan(keyEnd, key))
				continue;

			List<Entry> cols = cfToEntries(r.cf, columnStart, startColumnIncl,
					columnEnd, endColumnIncl);

			if (0 < cols.size()) {
				result.put(key, cols);
			}
		}

		return result;
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
			boolean startColumnIncl, boolean endColumnIncl,
			TransactionHandle txh) {
		return getKeySlice(keyStart, keyEnd, startKeyInc, endKeyInc,
				columnStart, columnEnd, startColumnIncl, endColumnIncl,
				Integer.MAX_VALUE / 1024, Integer.MAX_VALUE / 1024, txh);
	}


	private void ensureColumnFamilyExists(String cfName) {
		CassandraServer cs = new CassandraServer();

		// check whether cfName exists
		try {
			KsDef myKeyspace = cs.describe_keyspace(keyspace);
			for (CfDef cf : myKeyspace.getCf_defs()) {
				if (cf.getName().equals(cfName))
					return; // CF already exists
			}
			// Fall through to create CF below
		} catch (NotFoundException e) {
			throw new GraphStorageException(e);
		} catch (InvalidRequestException e) {
			throw new GraphStorageException(e);
		}

		CfDef addCf = new CfDef();
		addCf.setName(cfName);
		addCf.setKeyspace(keyspace);
		addCf.setComparator_type("org.apache.cassandra.db.marshal.BytesType");
		try {
			cs.set_keyspace(keyspace);
			cs.system_add_column_family(addCf);
		} catch (InvalidRequestException e) {
			throw new GraphStorageException(e);
		} catch (TException e) {
			throw new GraphStorageException(e);
		}
	}

	private List<Row> read(ReadCommand cmd) {
		ArrayList<ReadCommand> cmdHolder = new ArrayList<ReadCommand>(1);
		cmdHolder.add(cmd);
		return read(cmdHolder);
	}

	private List<Row> read(List<ReadCommand> cmds) {
		try {
			return StorageProxy.read(cmds, getConsistencyLevel());
		} catch (IOException e) {
			throw new GraphStorageException(e);
		} catch (UnavailableException e) {
			throw new GraphStorageException(e);
		} catch (TimeoutException e) {
			throw new GraphStorageException(e);
		} catch (InvalidRequestException e) {
			throw new GraphStorageException(e);
		}
	}

	private List<Row> readSlice(RangeSliceCommand cmd) {
		try {
			return StorageProxy.getRangeSlice(cmd, getConsistencyLevel());
		} catch (IOException e) {
			throw new GraphStorageException(e);
		} catch (UnavailableException e) {
			throw new GraphStorageException(e);
		} catch (TimeoutException e) {
			throw new GraphStorageException(e);
		}
	}

	private void mutate(RowMutation cmd) {
		mutate(Arrays.asList(cmd));
	}

	private void mutate(List<RowMutation> cmds) {
        try {
            StorageProxy.mutate(cmds, getConsistencyLevel());
        } catch (UnavailableException ex) {
            throw new GraphStorageException(ex);
        } catch (TimeoutException ex) {
            throw new GraphStorageException(ex);
        }
	}

	private List<Entry> cfToEntries(ColumnFamily cf, ByteBuffer columnStart,
			boolean startInclusive, ByteBuffer columnEnd, boolean endInclusive) {

		assert ! cf.isMarkedForDelete();

		// Deduct deleted columns from result size estimate
		int resultSize = 0;
		for (ByteBuffer col : cf.getColumnNames()) {
			IColumn icol = cf.getColumn(col);
			if (null == icol)
				throw new GraphStorageException("Unexpected null IColumn");

			if (icol.isMarkedForDelete())
				continue;

			resultSize++;
		}

		/*
		 * Our resultSize estimate could be up to two entries too big,
		 * depending on startInclusive and endInclusive.  That's
		 * probably OK.
		 */
		List<Entry> result = new ArrayList<Entry>(resultSize);

		for (ByteBuffer col : cf.getColumnNames()) {
			IColumn icol = cf.getColumn(col);
			if (null == icol)
				throw new GraphStorageException("Unexpected null IColumn");

			ByteBuffer name = org.apache.cassandra.utils.ByteBufferUtil.clone(icol.name());
			ByteBuffer value = org.apache.cassandra.utils.ByteBufferUtil.clone(icol.value());

			if (icol.isMarkedForDelete())
				continue;

			if (!startInclusive
					&& ByteBufferUtil.isSmallerOrEqualThan(name, columnStart))
				continue;

			if (!endInclusive
					&& ByteBufferUtil.isSmallerOrEqualThan(columnEnd, name))
				continue;

			result.add(new Entry(name, value));
		}
		return result;
	}
}
