package com.thinkaurelius.titan.diskstorage.astyanax;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.thinkaurelius.titan.core.GraphStorageException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.LockConfig;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.util.SimpleLockConfig;
import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.writeaggregation.MultiWriteKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.writeaggregation.Mutation;

public class AstyanaxOrderedKeyColumnValueStore implements
		OrderedKeyColumnValueStore, MultiWriteKeyColumnValueStore {

	private final String cfName;
	private final Keyspace keyspace;
	private final ColumnFamily<ByteBuffer, ByteBuffer> cf;
	private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);
	private final ConsistencyLevel readLevel;
	private final ConsistencyLevel writeLevel;
	private final RetryPolicy retryPolicy;
	
	private final LockConfig lockConfig;

	AstyanaxOrderedKeyColumnValueStore(Keyspace keyspace, String cfName,
			SimpleLockConfig.Builder lcBuilder, ConsistencyLevel readLevel,
			ConsistencyLevel writeLevel, RetryPolicy retryPolicy) {
		this.keyspace = keyspace;
		this.cfName = cfName;
		this.readLevel = readLevel;
		this.writeLevel = writeLevel;
		this.retryPolicy = retryPolicy;

		cf = new ColumnFamily<ByteBuffer, ByteBuffer>(
				this.cfName,
				ByteBufferSerializer.get(),
				ByteBufferSerializer.get());
		
		if (null != lcBuilder) {
			this.lockConfig = lcBuilder.dataStore(this).build();
		} else {
			this.lockConfig = null;
		}
	}
	
	@Override
	public void close() throws GraphStorageException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ByteBuffer get(ByteBuffer key, ByteBuffer column,
			TransactionHandle txh) {
		try {
			OperationResult<Column<ByteBuffer>> result = 
				keyspace.prepareQuery(cf)
					.setConsistencyLevel(readLevel)
					.withRetryPolicy(retryPolicy.duplicate())
					.getKey(key).getColumn(column).execute();
			return result.getResult().getByteBufferValue();
		} catch (NotFoundException e) {
			return null;
		} catch (ConnectionException e) {
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
	public void mutate(ByteBuffer key, List<Entry> additions,
			List<ByteBuffer> deletions, TransactionHandle txh) {
    	Map<ByteBuffer, com.thinkaurelius.titan.diskstorage.writeaggregation.Mutation> mutations =
    			new HashMap<ByteBuffer, com.thinkaurelius.titan.diskstorage.writeaggregation.Mutation>(1);
    	
    	com.thinkaurelius.titan.diskstorage.writeaggregation.Mutation m = new
    			com.thinkaurelius.titan.diskstorage.writeaggregation.Mutation(additions, deletions);
    	
    	mutations.put(key, m);
    	
    	mutateMany(mutations, txh);
	}

	@Override
	public void acquireLock(ByteBuffer key, ByteBuffer column,
			ByteBuffer expectedValue, TransactionHandle txh) {
		AstyanaxTransaction ctxh = (AstyanaxTransaction)txh;
		if (ctxh.isMutationStarted()) {
			throw new GraphStorageException("Attempted to obtain a lock after one or more mutations");
		}
		
		ctxh.writeBlindLockClaim(lockConfig, key, column, expectedValue);
	}

	@Override
	public boolean containsKey(ByteBuffer key, TransactionHandle txh) {
		try {
			// See getSlice() below for a warning suppression justification
			@SuppressWarnings("rawtypes")
			RowQuery rq = (RowQuery)keyspace.prepareQuery(cf)
								.withRetryPolicy(retryPolicy.duplicate())
								.setConsistencyLevel(readLevel)
								.getKey(key);
			@SuppressWarnings("unchecked")
			OperationResult<ColumnList<ByteBuffer>> r = rq.withColumnRange(EMPTY, EMPTY, false, 1).execute();
			return 0 < r.getResult().size();
		} catch (ConnectionException e) {
			throw new GraphStorageException(e);
		}
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, int limit, TransactionHandle txh) {
		
		/*
		 * The following hideous cast dance avoids a type-erasure error in the
		 * RowQuery<K, V> type that emerges when K=V=ByteBuffer. Specifically,
		 * these two methods erase to the same signature after generic reduction
		 * during compilation:
		 * 
		 * RowQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean
		 * reversed, int count) RowQuery<K, C> withColumnRange(ByteBuffer
		 * startColumn, ByteBuffer endColumn, boolean reversed, int count)
		 * 
		 * 
		 * The compiler substitutes ByteBuffer=C for both startColumn and
		 * endColumn, compares it to its identical twin with that type
		 * hard-coded, and dies.
		 * 
		 * Here's the compiler error I received when attempting to compile this
		 * code without the following casts. I used Oracle JDK 6 Linux x86_64.
		 * 
		 * AstyanaxOrderedKeyColumnValueStore.java:[108,4] reference to
		 * withColumnRange is ambiguous, both method
		 * withColumnRange(C,C,boolean,int) in
		 * com.netflix.astyanax.query.RowQuery<java.nio.ByteBuffer,java.nio.ByteBuffer>
		 * and method
		 * withColumnRange(java.nio.ByteBuffer,java.nio.ByteBuffer,boolean,int)
		 * in
		 * com.netflix.astyanax.query.RowQuery<java.nio.ByteBuffer,java.nio.ByteBuffer>
		 * match
		 * 
		 */
		@SuppressWarnings("rawtypes")
		RowQuery rq = (RowQuery)keyspace.prepareQuery(cf)
						.setConsistencyLevel(readLevel)
						.withRetryPolicy(retryPolicy.duplicate())
						.getKey(key);
//		RowQuery<ByteBuffer, ByteBuffer> rq = keyspace.prepareQuery(cf).getKey(key);
		rq.withColumnRange(columnStart, columnEnd, false, limit + 1);
		
		OperationResult<ColumnList<ByteBuffer>> r;
		try {
			@SuppressWarnings("unchecked")
			OperationResult<ColumnList<ByteBuffer>> tmp = (OperationResult<ColumnList<ByteBuffer>>)rq.execute();
			r = tmp;
		} catch (ConnectionException e) {
			throw new GraphStorageException(e);
		}
		
		List<Entry> result = new ArrayList<Entry>(r.getResult().size());
		
		int i = 0;
		
		for (Column<ByteBuffer> c : r.getResult()) {
			ByteBuffer colName = c.getName();
			
			if (colName.equals(columnEnd)) {
				break;
			}
			
			result.add(new Entry(colName, c.getByteBufferValue()));
			
			if (++i == limit) {
				break;
			}
		}
		
		return result;
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, TransactionHandle txh) {
		return getSlice(key, columnStart, columnEnd, Integer.MAX_VALUE - 1, txh);
	}

	@Override
	public void mutateMany(Map<ByteBuffer, Mutation> mutations,
			TransactionHandle txh) {
    	// null txh means a Transaction is calling this method
    	if (null != txh) {
    		// non-null txh -> make sure locks are valid
    		AstyanaxTransaction atxh = (AstyanaxTransaction)txh;
    		if (! atxh.isMutationStarted()) {
    			// This is the first mutate call in the transaction
    			atxh.mutationStarted();
    			// Verify all blind lock claims now
    			atxh.verifyAllLockClaims(); // throws GSE and unlocks everything on any lock failure
    		}
    	}
		
		MutationBatch m = keyspace.prepareMutationBatch()
							.setConsistencyLevel(writeLevel)
							.withRetryPolicy(retryPolicy.duplicate());
		
		final long delTS = TimestampProvider.getApproxNSSinceEpoch(false);
		final long addTS = TimestampProvider.getApproxNSSinceEpoch(true);
		
		for (Map.Entry<ByteBuffer, Mutation> ent : mutations.entrySet()) {
			// The CLMs for additions and deletions are separated because
			// Astyanax's operation timestamp cannot be set on a per-delete
			// or per-addition basis.
			ColumnListMutation<ByteBuffer> dels = m.withRow(cf, ent.getKey());
			dels.setTimestamp(delTS);
			ColumnListMutation<ByteBuffer> adds = m.withRow(cf, ent.getKey());
			adds.setTimestamp(addTS);
			 
			Mutation titanMutation = ent.getValue();
			
			if (titanMutation.hasDeletions()) {
				for (ByteBuffer b : titanMutation.getDeletions()) {
					dels.deleteColumn(b);
				}
			}
			
			if (titanMutation.hasAdditions()) {
				for (Entry e : titanMutation.getAdditions()) {
					adds.putColumn(e.getColumn(), e.getValue(), null);
				}
			}
		}
		
		try {
			m.execute();
		} catch (ConnectionException e) {
			throw new GraphStorageException(e);
		}

	}

}
