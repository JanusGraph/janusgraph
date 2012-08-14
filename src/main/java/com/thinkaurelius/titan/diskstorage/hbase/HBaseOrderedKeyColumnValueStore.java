package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.core.GraphStorageException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.LockConfig;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.util.SimpleLockConfig;
import com.thinkaurelius.titan.diskstorage.writeaggregation.MultiWriteKeyColumnValueStore;

/**
 * Experimental HBase store.
 * 
 * This is not ready for production.  It's pretty slow.
 * 
 * Here are some areas that might need work:
 *
 * - batching? (consider HTable#batch, HTable#setAutoFlush(false)
 * - tuning HTable#setWriteBufferSize (?)
 * - writing a server-side filter to replace ColumnCountGetFilter, which drops
 *   all columns on the row where it reaches its limit.  This requires getSlice,
 *   currently, to impose its limit on the client side.  That obviously won't
 *   scale.
 * - RowMutations for combining Puts+Deletes (need a newer HBase than 0.92 for this)
 * - (maybe) fiddle with HTable#setRegionCachePrefetch and/or #prewarmRegionCache
 * 
 * There may be other problem areas.  These are just the ones of which I'm aware.
 */
public class HBaseOrderedKeyColumnValueStore implements
		OrderedKeyColumnValueStore, MultiWriteKeyColumnValueStore {
	
	private static final Logger log = LoggerFactory.getLogger(HBaseOrderedKeyColumnValueStore.class);
	
	private final String tableName;
//	private final String columnFamily;
	private final HTablePool pool;
	private final LockConfig internals;
//	private final Configuration config;
	
	// This is cf.getBytes()
	private final byte[] famBytes;
	
	HBaseOrderedKeyColumnValueStore(Configuration config, String tableName,
			String columnFamily, OrderedKeyColumnValueStore lockStore,
			LocalLockMediator llm, byte[] rid, int lockRetryCount,
			long lockWaitMS, long lockExpireMS) {
//		this.config = config;
		this.tableName = tableName;
//		this.columnFamily = columnFamily;
		// TODO The number 32 of max pooled instances per table should be a config option
		this.pool = new HTablePool(config, 32);
		this.famBytes = columnFamily.getBytes();
		
		if (null != llm && null != lockStore) {
			this.internals = new SimpleLockConfig(this, lockStore, llm,
					rid, lockRetryCount, lockWaitMS, lockExpireMS);
		} else {
			this.internals = null;
		}
	}

	@Override
	public void close() throws GraphStorageException {
		try {
			pool.close();
		} catch (IOException e) {
			throw new GraphStorageException(e);
		}
	}

	@Override
	public ByteBuffer get(ByteBuffer key, ByteBuffer column,
			TransactionHandle txh) {
		
		byte[] keyBytes = toArray(key);
		byte[] colBytes = toArray(column);
		
		Get g = new Get(keyBytes);
		g.addColumn(famBytes, colBytes);
		try {
			g.setMaxVersions(1);
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
		
		try {
			HTableInterface table = null;
			Result r = null;
			
			try {
				table = pool.getTable(tableName);
				r = table.get(g);
			} finally {
				if (null != table)
					table.close();
			}
			
			if (null == r) {
				return null;
		    } else if (1 == r.size()) {
				return ByteBuffer.wrap(r.getValue(famBytes, colBytes));
			} else if (0 == r.size()) {
				return null;
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

	@Override
	public boolean containsKey(ByteBuffer key, TransactionHandle txh) {
		
		byte[] keyBytes = toArray(key);
		
		Get g = new Get(keyBytes);
		g.addFamily(famBytes);
		
		try {
			HTableInterface table = null;
			try {
				table = pool.getTable(tableName);
				return table.exists(g);
			} finally {
				if (null != table)
					table.close();
			}
		} catch (IOException e) {
			throw new GraphStorageException(e);
		}
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, int limit, TransactionHandle txh) {

		byte[] colStartBytes = columnEnd.hasRemaining() ? toArray(columnStart) : null;
		byte[] colEndBytes = columnEnd.hasRemaining() ? toArray(columnEnd) : null;
		
		Filter colRangeFilter = new ColumnRangeFilter(colStartBytes, true, colEndBytes, false);
		Filter limitFilter = new ColumnPaginationFilter(limit, 0);
		
		FilterList bothFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL, colRangeFilter,
			limitFilter);
		
		return getHelper(key, bothFilters);
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, TransactionHandle txh) {

		byte[] colStartBytes = columnEnd.hasRemaining() ? toArray(columnStart) : null;
		byte[] colEndBytes = columnEnd.hasRemaining() ? toArray(columnEnd) : null;
		
		Filter colRangeFilter = new ColumnRangeFilter(colStartBytes, true, colEndBytes, false);

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
			HTableInterface table = null;
			Result r = null;
			
			try {
				table = pool.getTable(tableName);
				r = table.get(g);
			} finally {
				if (null != table)
					table.close();
			}
			
			if (null == r)
				return new ArrayList<Entry>(0);
			
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
		
    	// null txh means a LockingTransaction is calling this method
    	if (null != txh) {
    		// non-null txh -> make sure locks are valid
    		HBaseTransaction lt = (HBaseTransaction)txh;
    		if (! lt.isMutationStarted()) {
    			// This is the first mutate call in the transaction
    			lt.mutationStarted();
    			// Verify all blind lock claims now
    			lt.verifyAllLockClaims(); // throws GSE and unlocks everything on any lock failure
    		}
    	}
		
		byte[] keyBytes = toArray(key);
		
		// TODO use RowMutations (requires 0.94.x-ish HBase)
		// error handling through the legacy batch() method sucks
        //RowMutations rms = new RowMutations(keyBytes);
		int totalsize = 0;
		
		if (null != additions)
			totalsize += additions.size();
		if (null != deletions)
			totalsize += deletions.size();
		
		List<Row> batchOps = new ArrayList<Row>(totalsize);
		
		// Deletes
		if (null != deletions && 0 != deletions.size()) {
			Delete d = new Delete(keyBytes);
			
			for (ByteBuffer del : deletions) {
				d.deleteColumn(famBytes, toArray(del.duplicate()));
			}
			
			batchOps.add(d);
		}
		
		// Inserts
		if (null != additions && 0 != additions.size()) {
			Put p = new Put(keyBytes);
			
			for (Entry e : additions) {
				byte[] colBytes = toArray(e.getColumn().duplicate());
				byte[] valBytes = toArray(e.getValue().duplicate());
				
				p.add(famBytes, colBytes, valBytes);
			}
			
			batchOps.add(p);
		}
		
		try {
			HTableInterface table = null;
			try {
				table = pool.getTable(tableName);
				table.batch(batchOps);
				table.flushCommits();
			} finally {
				if (null != table)
					table.close();
			}
		} catch (IOException e) {
			throw new GraphStorageException(e);
		} catch (InterruptedException e) {
			throw new GraphStorageException(e);
		}
	}

	@Override
	public void mutateMany(
			Map<ByteBuffer, com.thinkaurelius.titan.diskstorage.writeaggregation.Mutation> mutations,
			TransactionHandle txh) {
		
    	// null txh means a LockingTransaction is calling this method
    	if (null != txh) {
    		// non-null txh -> make sure locks are valid
    		HBaseTransaction lt = (HBaseTransaction)txh;
    		if (! lt.isMutationStarted()) {
    			// This is the first mutate call in the transaction
    			lt.mutationStarted();
    			// Verify all blind lock claims now
    			lt.verifyAllLockClaims(); // throws GSE and unlocks everything on any lock failure
    		}
    	}
		
		Map<byte[], Put> puts = new HashMap<byte[], Put>();
		Map<byte[], Delete> dels = new HashMap<byte[], Delete>();

		List<Row> batchOps = new LinkedList<Row>();

		final long delTS = System.currentTimeMillis();
		final long putTS = delTS + 1;
		
		for (ByteBuffer keyBB : mutations.keySet()) {
			byte[] keyBytes = toArray(keyBB);
			
			com.thinkaurelius.titan.diskstorage.writeaggregation.Mutation m = mutations.get(keyBB);
			
			if (m.hasDeletions()) {
				Delete d = dels.get(keyBytes);
				
				if (null == d) {
					d = new Delete(keyBytes, delTS, null);
					dels.put(keyBytes, d);
					batchOps.add(d);
				}
				
				for (ByteBuffer b : m.getDeletions()) {
					d.deleteColumns(famBytes, toArray(b), delTS);
				}
			}
			

			if (m.hasAdditions()) {
				Put p = puts.get(keyBytes);
				
				if (null == p) {
					p = new Put(keyBytes, putTS);
					puts.put(keyBytes, p);
					batchOps.add(p);
				}
				
				for (Entry e : m.getAdditions()) {
					byte[] colBytes = toArray(e.getColumn());
					byte[] valBytes = toArray(e.getValue());
					p.add(famBytes, colBytes, putTS, valBytes);
				}
			}
		}
		
		try {
			HTableInterface table = null;
			try {
				table = pool.getTable(tableName);
				table.batch(batchOps);
				table.flushCommits();
			} finally {
				if (null != table)
					table.close();
			}
		} catch (IOException e) {
			throw new GraphStorageException(e);
		} catch (InterruptedException e) {
			throw new GraphStorageException(e);
		}
		
		long now = System.currentTimeMillis(); 
		while (now <= putTS) {
			try {
				Thread.sleep(1L);
				now = System.currentTimeMillis();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void acquireLock(ByteBuffer key, ByteBuffer column,
			ByteBuffer expectedValue, TransactionHandle txh) {
		HBaseTransaction lt = (HBaseTransaction)txh;
		if (lt.isMutationStarted()) {
			throw new GraphStorageException("Attempted to obtain a lock after one or more mutations");
		}
		
		lt.writeBlindLockClaim(internals, key, column, expectedValue);
	}

}
