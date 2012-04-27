package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.exceptions.GraphStorageException;

/*
 * This is a naive and slow implementation.  Here are some areas that might need work:
 *
 * - batching (consider HTable#batch, HTable#setAutoFlush(false),  Scan#setBatch(int))
 * - scan caching (Scan#setCache)
 * - tuning HTable#setWriteBufferSize (?)
 * - writing a server-side filter to replace ColumnCountGetFilter, which drops
 *   all columns on the row where it reaches its limit.  This requires getSlice,
 *   currently, to impose its limit on the client side.  That obviously won't
 *   scale.
 * - connection pooling (see HTablePool)
 * - thread safety(i.e. making each method use a stack-stored HTable from the pool)
 * - RowMutations for combining Puts+Deletes (need a newer HBase than 0.92 for this)
 * - (maybe) fiddle with HTable#setRegionCachePrefetch and/or #prewarmRegionCache
 * 
 * Features that definitely aren't implemented:
 * - transactions
 * - locks
 * 
 * There may be other problem areas.  These are just the ones of which I'm aware.
 */
public class HBaseOrderedKeyColumnValueStore implements
		OrderedKeyColumnValueStore {
	
	private static final Logger log = LoggerFactory.getLogger(HBaseOrderedKeyColumnValueStore.class);
	
	private final HTable table;
//	private final String family;
	
	// This is cf.getBytes()
	private final byte[] famBytes;
	
	HBaseOrderedKeyColumnValueStore(HTable table, String family) {
		this.table = table;
//		this.family = family;
		this.famBytes = family.getBytes();
	}

	@Override
	public void close() throws GraphStorageException {
		// Do nothing
	}

	@Override
	public ByteBuffer get(ByteBuffer key, ByteBuffer column,
			TransactionHandle txh) {
		
		byte[] keyBytes = toArray(key);
		byte[] colBytes = toArray(column);
		
		Get g = new Get(keyBytes);
		g.addColumn(famBytes, colBytes);
		
		try {
			Result r = table.get(g);
			if (0 == r.size()) {
				return null;
			} else if (1 == r.size()) {
				return ByteBuffer.wrap(r.getValue(famBytes, colBytes));
			} else {
				log.warn("Found {} results for key {}, column {}, family {} (expected 0 or 1 results)", 
						new Object[] { r.size(),
								       new String(Hex.encodeHex(keyBytes)),
							           new String(Hex.encodeHex(colBytes)),
							           new String(Hex.encodeHex(famBytes)) }
				);
				return null;
			}
		} catch (IOException e) {
			throw new GraphStorageException(e);
		}
	}

	@Override
	public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column,
			TransactionHandle txh) {
		return null != get(key, column, txh);
	}

	@Override
	public boolean isLocalKey(ByteBuffer key) {
		// TODO Auto-generated method stub
		return false;
	}

//	@Override
//	public void insert(ByteBuffer key, List<Entry> entries,
//			TransactionHandle txh) {
//		
//		byte[] keyBytes = toArray(key);
//		
//		Put p = new Put(keyBytes);
//		
//		for (Entry e : entries) {
//			byte[] colBytes = toArray(e.getColumn());
//			byte[] valBytes = toArray(e.getValue());
//			
//			p.add(famBytes, colBytes, valBytes);
//		}
//		
//		try {
//			table.put(p);
//		} catch (IOException e) {
//			throw new GraphStorageException(e);
//		}
//	}
//
//	@Override
//	public void delete(ByteBuffer key, List<ByteBuffer> columns,
//			TransactionHandle txh) {
//		
//		byte[] keyBytes = toArray(key);
//		
//		Delete d = new Delete(keyBytes);
//		
//		for (ByteBuffer c : columns) {
//			d.deleteColumn(famBytes, toArray(c));
//		}
//		
//		try {
//			table.delete(d);
//			table.flushCommits();
//		} catch (IOException e) {
//			throw new GraphStorageException(e);
//		}
//	}

//	@Override
//	public void acquireLock(ByteBuffer key, ByteBuffer column, LockType type,
//			TransactionHandle txh) {
//		// TODO Auto-generated method stub
//
//	}

	@Override
	public boolean containsKey(ByteBuffer key, TransactionHandle txh) {
		
		byte[] keyBytes = toArray(key);
		
		Get g = new Get(keyBytes);
		g.addFamily(famBytes);
		
		try {
			return table.exists(g);
		} catch (IOException e) {
			throw new GraphStorageException(e);
		}
	}

//	@Override
//	public List<Entry> getLimitedSlice(ByteBuffer key, ByteBuffer columnStart,
//			ByteBuffer columnEnd, boolean startInclusive, boolean endInclusive,
//			int limit, TransactionHandle txh) {
//		List<Entry> tentativeResults = getSlice(key, columnStart, columnEnd,
//				startInclusive, endInclusive, limit + 1, txh);
//		if (limit < tentativeResults.size()) {
//			return null;
//		}
//		return tentativeResults;
//	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, boolean startInclusive, boolean endInclusive,
			int limit, TransactionHandle txh) {

		byte[] colStartBytes = toArray(columnStart);
		byte[] colEndBytes = toArray(columnEnd);
		
		// Once ColumnCountGetFilter reaches its configured column limit,
		// it drops the entire row on which the limit was reached.  This
		// makes it useless for slicing columns under a single key.
		
//		Filter colRangeFilter = new ColumnRangeFilter(colStartBytes, startInclusive, colEndBytes, endInclusive);
//		Filter limitFilter = new ColumnCountGetFilter(limit);
//		
//		FilterList bothFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL,
//			limitFilter, colRangeFilter);
//		
//		return getHelper(key, bothFilters);
		
		
		// Here, I'm falling back to retrieving the whole row and
		// cutting it down to the limit on the client.  This is obviously
		// not going to scale.  The long-term solution is probably to
		// reimplement ColumnCountGetFilter in such a way that it won't
		// drop the final row.
		Filter colRangeFilter = new ColumnRangeFilter(colStartBytes, startInclusive, colEndBytes, endInclusive);
		List<Entry> ents = getHelper(key, colRangeFilter);
		
		if (ents.size() <= limit)
			return ents;
		
		List<Entry> result = new ArrayList<Entry>(limit);
		for (int i = 0; i < limit; i++) {
			result.add(ents.get(i));
		}
		
		return result;
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, boolean startInclusive, boolean endInclusive,
			TransactionHandle txh) {

		byte[] colStartBytes = toArray(columnStart);
		byte[] colEndBytes = toArray(columnEnd);
		
		Filter colRangeFilter = new ColumnRangeFilter(colStartBytes, startInclusive, colEndBytes, endInclusive);

		return getHelper(key, colRangeFilter);
	}

	private List<Entry> getHelper(ByteBuffer key,
			Filter getFilter) {
		
		byte[] keyBytes = toArray(key);
		
		Get g = new Get(keyBytes);
		g.addFamily(famBytes);
		g.setFilter(getFilter);
		
		List<Entry> ret = null;
		
		try {
			Result r = table.get(g);
			
			int resultCount = r.size();
			
			ret = new ArrayList<Entry>(resultCount);
			
			Map<byte[], byte[]> fmap = r.getFamilyMap(famBytes);
			
			if (null != fmap) {
				for (Map.Entry<byte[], byte[]> ent : fmap.entrySet()) {
					ret.add(new Entry(ByteBuffer.wrap(ent.getKey()), ByteBuffer.wrap(ent.getValue())));
				}
			}
			
			return ret;
		} catch (IOException e) {
			throw new GraphStorageException(e);
		}
	}
	
//	@Override
//	public Map<ByteBuffer, List<Entry>> getKeySlice(ByteBuffer keyStart,
//			ByteBuffer keyEnd, boolean startKeyInc, boolean endKeyInc,
//			ByteBuffer columnStart, ByteBuffer columnEnd,
//			boolean startColumnIncl, boolean endColumnIncl, int keyLimit,
//			int columnLimit, TransactionHandle txh) {
//		
//		byte[] colStartBytes = toArray(columnStart);
//		byte[] colEndBytes = toArray(columnEnd);
//		
//		Filter colRangeFilter = new ColumnRangeFilter(colStartBytes, startColumnIncl, colEndBytes, endColumnIncl);
//		
//		return scanHelper(keyStart, keyEnd, startKeyInc, endKeyInc, keyLimit, columnLimit, colRangeFilter);
//	}

//	@Override
//	public Map<ByteBuffer, List<Entry>> getLimitedKeySlice(ByteBuffer keyStart,
//			ByteBuffer keyEnd, boolean startKeyInc, boolean endKeyInc,
//			ByteBuffer columnStart, ByteBuffer columnEnd,
//			boolean startColumnIncl, boolean endColumnIncl, int keyLimit,
//			int columnLimit, TransactionHandle txh) {
//		Map<ByteBuffer, List<Entry>> tentativeResult = getKeySlice(keyStart,
//				keyEnd, startKeyInc, endKeyInc, columnStart, columnEnd,
//				startColumnIncl, endColumnIncl, keyLimit + 1, columnLimit + 1,
//				txh);
//		// check whether keyLimit was exceeded
//		if (keyLimit < tentativeResult.size())
//			return null;
//		// check whether columnLimit was exceeded
//		for (List<Entry> l : tentativeResult.values())
//			if (columnLimit < l.size())
//				return null;
//		return tentativeResult;
//	}

//	@Override
//	public Map<ByteBuffer, List<Entry>> getKeySlice(ByteBuffer keyStart,
//			ByteBuffer keyEnd, boolean startKeyInc, boolean endKeyInc,
//			ByteBuffer columnStart, ByteBuffer columnEnd,
//			boolean startColumnIncl, boolean endColumnIncl,
//			TransactionHandle txh) {
//		// TODO remove the Integer.MAX_VALUE-derived constants
//		return getKeySlice(keyStart, keyEnd, startKeyInc, endKeyInc,
//				columnStart, columnEnd, startColumnIncl, endColumnIncl, 
//				Integer.MAX_VALUE / 1024, Integer.MAX_VALUE / 1024, txh);
//	}
	
//	private Map<ByteBuffer, List<Entry>> scanHelper(ByteBuffer keyStart,
//			ByteBuffer keyEnd, boolean startKeyInc, boolean endKeyInc, int keyLimit, int columnLimit, Filter columnFilter) {
//		
//		// Special case: keyStart equals keyEnd
//		if (keyStart.equals(keyEnd)) {
//			if (startKeyInc && endKeyInc) {
//				Map<ByteBuffer, List<Entry>> singleton =
//						new HashMap<ByteBuffer, List<Entry>>(1);
//				singleton.put(keyStart, getHelper(keyStart, columnFilter));
//				return singleton;
//			} else {
//				return ImmutableMap.<ByteBuffer, List<Entry>>of();
//			}
//		}
//		
//		byte[] keyStartBytes = toArray(keyStart);
//		byte[] keyEndBytes = toArray(keyEnd);
//		
//		// Scans are hardcoded to interpret the start as inclusive and end as exclusive
//		// Later in this method, we'll do a Get for keyEndBytes if endColumnIncl is true
//		Scan s = new Scan(keyStartBytes, keyEndBytes);
//		s.addFamily(famBytes);
//		s.setFilter(columnFilter);
//		// TODO set configurable scanner batch size here
//		
//		try {
//			ResultScanner rs = table.getScanner(s);
//			int rowsScanned = 0;
//				
//			Map<ByteBuffer, List<Entry>> ret = new HashMap<ByteBuffer, List<Entry>>();
//			
//			for (Result r : rs) {
//				
//				byte[] row = r.getRow();
//				ByteBuffer rowBB = ByteBuffer.wrap(row);
//				
//				// Skip this row if !startKeyInc
//				if (!startKeyInc && Arrays.equals(row, keyStartBytes)) {
//					continue;
//				}
//				
//				Map<byte[], byte[]> fmap = r.getFamilyMap(famBytes);
//				
//				List<Entry> rowEntries = new ArrayList<Entry>(fmap.size());
//				
//				int columnsScanned = 0;
//				if (null != fmap) {
//					for (Map.Entry<byte[], byte[]> ent : fmap.entrySet()) {
//						ByteBuffer colBB = ByteBuffer.wrap(ent.getKey());
//						ByteBuffer valBB = ByteBuffer.wrap(ent.getValue());
//						rowEntries.add(new Entry(colBB, valBB));
//					
//						if (++columnsScanned == columnLimit)
//							break;
//					}
//				}
//				
//				if (!rowEntries.isEmpty())
//					ret.put(rowBB, rowEntries);
//
//				if (++rowsScanned == keyLimit)
//					return ret;
//			}
//			
//			assert ! (ret.containsKey(keyEnd));
//
//			// If endKeyIncl is true, then issue a Get for that final row
//			if (endKeyInc) {
//				Filter limitFilter = new ColumnCountGetFilter(columnLimit);
//				FilterList bothFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL,
//						limitFilter, columnFilter);
//			
//				List<Entry> lastRowEntries = getHelper(keyEnd, bothFilters);
//				
//				if (!lastRowEntries.isEmpty())
//					ret.put(keyEnd.duplicate(), lastRowEntries);
//			}
//			
//			return ret;
//			
//		} catch (IOException e) {
//			throw new GraphStorageException(e);
//		}
//	}
	
	/*
	 * This method exists because HBase's API generally deals in
	 * whole byte[] arrays.  That is, data are always assumed to
	 * begin at index zero and run through the native length of
	 * the array.  These assumptions are reflected, for example,
	 * in the classes hbase.client.Get and hbase.client.Scan.
	 * These assumptions about arrays are not generally true when
	 * dealing with ByteBuffers.
	 * <p>
	 * This method first checks whether the array backing the
	 * ByteBuffer argument indeed satisfies the assumptions described
	 * above.  If so, then this method returns the backing array.
	 * In other words, this case returns {@code b.array()}.
	 * <p>
	 * If the ByteBuffer argument does not satisfy the array
	 * assumptions described above, then a new native array of length
	 * {@code b.limit()} is created.  The ByteBuffer's contents
	 * are copied into the new native array without modifying the
	 * state of {@code b} (using {@code b.duplicate()}).  The new
	 * native array is then returned.
	 *  
	 */
	private static byte[] toArray(ByteBuffer b) {
		if (0 == b.arrayOffset() && b.limit() == b.array().length)
			return b.array();
		
		byte[] result = new byte[b.limit()];
		b.duplicate().get(result);
		return result;
	}

	@Override
	public void mutate(ByteBuffer key, List<Entry> additions,
			List<ByteBuffer> deletions, TransactionHandle txh) {
		
		byte[] keyBytes = toArray(key);
		
//		boolean oldAutoFlush = table.isAutoFlush();
//		table.setAutoFlush(false);
		
//		RowMutations rms = new RowMutations(keyBytes);
		
		// TODO use RowMutations (requires 0.94.x-ish HBase)
		
		// Deletes
		if (null != deletions && 0 != deletions.size()) {
			Delete d = new Delete(keyBytes);
			
			for (ByteBuffer del : deletions) {
				d.deleteColumn(famBytes, toArray(del.duplicate()));
			}
			
			try {
				table.delete(d);
			} catch (IOException e) {
				throw new GraphStorageException(e);
			}
		}
		
		// Inserts
		if (null != additions && 0 != additions.size()) {
			Put p = new Put(keyBytes);
			
			for (Entry e : additions) {
				byte[] colBytes = toArray(e.getColumn().duplicate());
				byte[] valBytes = toArray(e.getValue().duplicate());
				
				p.add(famBytes, colBytes, valBytes);
			}
			
			try {
				table.put(p);
			} catch (IOException e) {
				throw new GraphStorageException(e);
			}
		}
		
		try {
			table.flushCommits();
		} catch (IOException e) {
			throw new GraphStorageException(e);
		}
//		table.setAutoFlush(oldAutoFlush);
	}

	@Override
	public void acquireLock(ByteBuffer key, ByteBuffer column,
			ByteBuffer expectedValue, TransactionHandle txh) {
		// TODO Auto-generated method stub
		
	}

}
