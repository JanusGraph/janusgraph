package com.thinkaurelius.titan.diskstorage.cassandra;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
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
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.UncheckedGenericKeyedObjectPool;
import com.thinkaurelius.titan.exceptions.GraphStorageException;

/**
 * This class is not safe for concurrent use by multiple threads.
 *
 */
public class CassandraTransaction implements TransactionHandle {

	/*
	 * This variable starts false.  It remains false during the
	 * locking stage of a transaction.  It is set to true at the
	 * beginning of the first mutate/mutateMany call in a transaction
	 * (before performing any writes to the backing store). 
	 */
	private boolean isMutationStarted;
	
	private long lastLockApplicationTimeMS;
	
	private final LinkedHashSet<LockClaim> lockClaims = new LinkedHashSet<LockClaim>();
	
	private final UncheckedGenericKeyedObjectPool<String, CTConnection> pool;
	private final String keyspace;
	
	private final int lockExpirationMS = 300 * 1000;
	private final int lockWaitMS = 500;
	private final int lockRetryCount = 3;
	
	private final byte[] rid;
	
	CassandraTransaction(String keyspace, UncheckedGenericKeyedObjectPool<String, CTConnection> pool) {
		this.keyspace = keyspace;
		this.pool = pool;
		
		byte[] addrBytes;
		try {
			addrBytes = InetAddress.getLocalHost().getAddress();
		} catch (UnknownHostException e) {
			throw new GraphStorageException(e);
		}
		byte[] procNameBytes = ManagementFactory.getRuntimeMXBean().getName().getBytes();
		
		this.rid = new byte[addrBytes.length + procNameBytes.length];
		System.arraycopy(addrBytes, 0, rid, 0, addrBytes.length);
		System.arraycopy(procNameBytes, 0, rid, addrBytes.length, procNameBytes.length);
	}
	
	
	@Override
	public void abort() {
		if (0 < lockClaims.size())
			unlockAll();
	}

	@Override
	public void commit() {
		if (0 < lockClaims.size())
			unlockAll();
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}
	
	boolean isMutationStarted() {
		return isMutationStarted;
	}
	
	void mutationStarted() {
		isMutationStarted = true;
	}
	
	/*
	 * This method first checks the local lock mediator to see whether
	 * another transaction in this process holds the lock described
	 * by this method's arguments.  If the local lock mediator determines
	 * that no other transaction holds the lock and assigns it to us,
	 * then we write a claim to the backing key-value store.  We do not
	 * yet check to see whether our locking claim is the most senior
	 * one present in the backing key-value store.  We also don't check
	 * whether the expectedValue matches the actual value stored at the
	 * key-column coordinates supplied.  These checks happen in
	 * #verifyAllLockClaims().
	 * <p>
	 * Once a lock is acquired in the local lock mediator and a claim
	 * written to the backing key-value store, this method appends a
	 * LockClaim object to the lockClaims field and then returns.
	 * <p>
	 * If we can't get the lock from the local lock mediator, then we
	 * throw a GraphStorageException with a string message to that effect.
	 */
	void writeBlindLockClaim(String cf, ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue)
			throws GraphStorageException {

		LockClaim lc = new LockClaim(cf, key, column, expectedValue);
		
		// Check to see whether we already hold this lock
		if (lockClaims.contains(lc))
			return;
		
		Integer myId = hashCode();
		String lockCfName = cf + "_locks";
		
		// Check the local lock mediator
		LocalLockMediator llm = LocalLockMediator.get(cf);
		if (!llm.lock(key, column, myId)) {
			throw new GraphStorageException("Lock contention among local transactions");
		}
		
		/* Write lock to the backing store
		 *
		 * The key we write is a concatenation of the arguments key and column,
		 * prefixed by an int (4 bytes) representing the length of the argument key.
		 * 
		 * The column we write is a concatenation of our rid and the timestamp.
		 * 
		 * The value is undefined in the locking protocol; here, we write the
		 * transaction id to the value merely for debugging purposes.
		 */

		
		ByteBuffer lockKeyBuf = lc.getLockKey();

		ColumnParent cp = new ColumnParent(lockCfName);
		
		CTConnection conn = null;
		boolean ok = false;
		try {
			conn = pool.genericBorrowObject(keyspace);
			Cassandra.Client client = conn.getClient();
			
			for (int i = 0; i < lockRetryCount; i++) {

				long ts = CassandraThriftOrderedKeyColumnValueStore.getNewTimestamp(false);
				

				ByteBuffer lockColBuf = lc.getLockCol(ts);
				
				ByteBuffer myIdBuf = ByteBuffer.allocate(4);
				myIdBuf.putInt(myId).reset();
				
				Column lockCol = new Column();
				lockCol.setName(lockColBuf);
				lockCol.setValue(myIdBuf);
				lockCol.setTimestamp(ts);
				
				long before = System.currentTimeMillis();
				client.insert(lockKeyBuf, cp, lockCol, ConsistencyLevel.QUORUM);
				long after = System.currentTimeMillis();
				
				if (lockWaitMS < after - before) {
					// Too slow
					// Delete lock claim and loop again
					ColumnPath cpath = new ColumnPath();
					cpath.setColumn(lockColBuf);
					cpath.setColumnIsSet(true);
					cpath.setColumn_family(lockCfName);
					cpath.setColumn_familyIsSet(true);
					client.remove(lockKeyBuf, cpath, 
							CassandraThriftOrderedKeyColumnValueStore.getNewTimestamp(false),
							ConsistencyLevel.QUORUM);
				} else {
					ok = true;
					lastLockApplicationTimeMS = ts;
					break;
				}
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
			if (!ok)
				llm.unlock(key, column, myId);
		}
		
		lockClaims.add(lc);
	}

	/*
	 * For each object in the lockClaims list, this method checks (1)
	 * that the current transaction indeed holds the lock globally (that
	 * is, that the lock claim we wrote to the backing key-value store is
	 * indeed the most senior or earliest such claim) and (2) that the
	 * expectedValue originally supplied in the locking request matches
	 * the current value stored at the key-column coordinate of the lock.
	 * <p>
	 * If we are not most senior on any object in the lockClaims list,
	 * then we throw GraphStorageException to that effect.
	 * <p>
	 * If there is an expectedValue mismatch between any locking request
	 * and actual key-column value, then we throw GraphStorageException
	 * to that effect.
	 * <p>
	 * In other words, if this method returns without throwing an
	 * exception, then the transaction holds all locks it has previously
	 * requested and the expectedValue associated with each transaction
	 * matches reality.
	 * 
	 */
	void verifyAllLockClaims() throws GraphStorageException {

		// wait one full lockWaitMS since the last claim attempt, if needed
		if (0 == lastLockApplicationTimeMS)
			return; // no locks
		long now;
		while (true) {
			now = System.currentTimeMillis();
			
			final long delta = now - lastLockApplicationTimeMS;
		
			if (delta < lockWaitMS) {
				try {
					Thread.sleep(lockWaitMS - delta);
				} catch (InterruptedException e) {
					throw new GraphStorageException(e);
				}
			} else {
				break;
			}
		}
		
		// lock-column-family -> keys
		Map<String, List<ByteBuffer>> multigetSlices = 
				new HashMap<String, List<ByteBuffer>>();
		
		
		// Prepare a series of multiget_slice calls to verify lock seniority
		for (LockClaim lc : lockClaims) {
			
			ByteBuffer lockKeyBuf = lc.getLockKey();
			
			List<ByteBuffer> keys = multigetSlices.get(lc.cf);
			
			if (null == keys) {
				keys = new LinkedList<ByteBuffer>();
				multigetSlices.put(lc.cf, keys);
			}
			
			keys.add(lockKeyBuf);
		}

		SlicePredicate sp = new SlicePredicate();
		
		Map<String, Map<ByteBuffer, List<ColumnOrSuperColumn>>> cassResults =
				new HashMap<String, Map<ByteBuffer, List<ColumnOrSuperColumn>>>(multigetSlices.size());
		
		// Issue multiget_slice calls
		CTConnection conn = null;
		try {
			conn = pool.genericBorrowObject(keyspace);
			Cassandra.Client client = conn.getClient();

			for (Map.Entry<String, List<ByteBuffer>> ent : multigetSlices.entrySet()) {
				String lockCfName = ent.getKey() + "_locks";
				ColumnParent colParent = new ColumnParent();
				colParent.setColumn_family(lockCfName);
				colParent.setColumn_familyIsSet(true);
				Map<ByteBuffer, List<ColumnOrSuperColumn>> result =
						client.multiget_slice(ent.getValue(), colParent, sp, ConsistencyLevel.QUORUM);
				cassResults.put(ent.getKey(), result);
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
		
		// Check multiget_slice results for lock seniority
		for (LockClaim lc : lockClaims) {
			Map<ByteBuffer, List<ColumnOrSuperColumn>> slice =
					cassResults.get(lc.cf);
			
			if (null == slice)
				throw new GraphStorageException("Locking failed: no data for column family " + lc.cf);
			
			ByteBuffer lockKeyBuf = lc.getLockKey();
			
			List<ColumnOrSuperColumn> cols = slice.get(lockKeyBuf);
			
			if (null == cols || 0 == cols.size())
				throw new GraphStorageException("Locking failed: no data for lock key");
			
			Long mints = null;
			byte[] oldestRid = null;
			for (ColumnOrSuperColumn cosc : cols) {
				ByteBuffer bb = cosc.getColumn().bufferForName();
				long ts = bb.getLong();
				byte[] rid = new byte[bb.remaining()];
				bb.get(rid);

				// Ignore expired lock claims
				if (ts < now - lockExpirationMS) {
					continue;
				}
				
				if (null == mints || ts < mints) {
					mints = ts;
					oldestRid = rid;
				} else if (mints == ts) { // tie breaker
					ByteBuffer oldestRidBuf = ByteBuffer.wrap(oldestRid);
					ByteBuffer currentRid = ByteBuffer.wrap(rid);
					
					int i = currentRid.compareTo(oldestRidBuf);
					
					if (-1 == i) {
						oldestRid = rid;
					} else if (1 == i) {
						// Do nothing; rid takes priority
					} else {
						throw new GraphStorageException("Retrieved two copies of the same column from Cassandra");
					}
				}
			}
			
			if (null == oldestRid)
				throw new GraphStorageException("Error parsing lock reservations");
			
			// Finally, check whether oldestRid is our rid
			if (!oldestRid.equals(rid)) {
				throw new GraphStorageException("Lock already held");
			}
		}

		// Now check that expectedValue for each lock matches reality
		try {
			conn = pool.genericBorrowObject(keyspace);
			Cassandra.Client client = conn.getClient();
			for (LockClaim lc : lockClaims) {
				ColumnPath cpath = new ColumnPath();
				cpath.setColumn(lc.column);
				cpath.setColumnIsSet(true);
				cpath.setColumn_family(lc.cf);
				cpath.setColumn_familyIsSet(true);
				ColumnOrSuperColumn result = client.get(lc.key, cpath, ConsistencyLevel.QUORUM);
				Column cresult = result.getColumn();
				if (!lc.expectedValue.equals(cresult.bufferForValue())) {
					throw new GraphStorageException("Expected value mismatch");
				}	
			}
		} catch (TException e) {
			throw new GraphStorageException(e);
		} catch (TimedOutException e) {
			throw new GraphStorageException(e);
		} catch (UnavailableException e) {
			throw new GraphStorageException(e);
		} catch (InvalidRequestException e) {
			throw new GraphStorageException(e);
		} catch (NotFoundException e) {
			throw new GraphStorageException(e);
		} finally {
			if (null != conn)
				pool.genericReturnObject(keyspace, conn);
		}
	}
	
	private void unlockAll() {
		Integer myId = hashCode();

		final long ts =
				CassandraThriftOrderedKeyColumnValueStore.getNewTimestamp(true);
		
		// Release locks in the backing key-value store
		Map<ByteBuffer, Map<String, List<Mutation>>> batch = new
				HashMap<ByteBuffer, Map<String, List<Mutation>>>();
		
		for (LockClaim lc : lockClaims) {
			ByteBuffer lockKeyBuf = lc.getLockKey();
			ByteBuffer lockColBuf = lc.getLockCol(ts);
			
			Map<String, List<Mutation>> mutationsOnKey =
					batch.get(lockKeyBuf);
			
			if (null == mutationsOnKey) {
				mutationsOnKey = new HashMap<String, List<Mutation>>();
				batch.put(lc.key, mutationsOnKey);
			}
			
			List<Mutation> mutationsOnCf = mutationsOnKey.get(lc.cf + "_locks");
			
			if (null == mutationsOnCf) {
				mutationsOnCf = new LinkedList<Mutation>();
				mutationsOnKey.put(lc.cf, mutationsOnCf);
			}
			Deletion d = new Deletion();
			d.setTimestamp(ts);
			d.setTimestampIsSet(true);
			
			SlicePredicate sp = new SlicePredicate();
			List<ByteBuffer> l = new ArrayList<ByteBuffer>(1);
			l.add(lockColBuf);
			sp.setColumn_names(l);
			sp.setColumn_namesIsSet(true);
			d.setPredicate(sp);
			d.setPredicateIsSet(true);
			
			Mutation m = new Mutation();
			m.deletion = d;
			
			mutationsOnCf.add(m);
		}

		CTConnection conn = null;
		try {
			conn = pool.genericBorrowObject(keyspace);
			Cassandra.Client client = conn.getClient();
			client.batch_mutate(batch, ConsistencyLevel.QUORUM);
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
		
		// Release locks locally
		for (LockClaim lc : lockClaims) {
			LocalLockMediator llm = LocalLockMediator.get(lc.cf);
			llm.unlock(lc.key, lc.column, myId);
		}
	}
	
	private class LockClaim {
		private final String cf;
		private final ByteBuffer key;
		private final ByteBuffer column;
		private final ByteBuffer expectedValue;
		private ByteBuffer lockKey, lockCol;
		
		public LockClaim(String cf, ByteBuffer key, ByteBuffer column,
				ByteBuffer expectedValue) {
			this.cf = cf;
			this.key = key;
			this.column = column;
			this.expectedValue = expectedValue;
		}
		
		public ByteBuffer getLockKey() {
			if (null != lockKey) {
				return lockKey;
			}
			
			lockKey = ByteBuffer.allocate(key.remaining() + column.remaining() + 4);
			lockKey.putInt(key.remaining()).put(key.duplicate()).put(column.duplicate()).reset();
			
			return lockKey;
		}
		
//		public ByteBuffer getLockCol() {
//			if (null != lockCol) {
//				return lockCol;
//			}
//			
//			return getLockCol(CassandraThriftOrderedKeyColumnValueStore.getNewTimestamp(false));
//		}
		
		public ByteBuffer getLockCol(long ts) {
			
			lockCol = ByteBuffer.allocate(rid.length + 8);
			lockCol.putLong(ts).put(rid).reset();
			
			return lockCol;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((cf == null) ? 0 : cf.hashCode());
			result = prime * result + ((column == null) ? 0 : column.hashCode());
			result = prime * result + ((key == null) ? 0 : key.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			LockClaim other = (LockClaim) obj;
			return other.cf.equals(this.cf) &&
					other.key.equals(this.key) &&
					other.column.equals(this.column);
		}
	}
}
