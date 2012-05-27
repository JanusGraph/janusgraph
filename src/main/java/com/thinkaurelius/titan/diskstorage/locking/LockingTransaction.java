package com.thinkaurelius.titan.diskstorage.locking;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager;
import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;
import com.thinkaurelius.titan.exceptions.LockingFailureException;

/**
 * This class is not safe for concurrent use by multiple threads.
 *
 */
public abstract class LockingTransaction implements TransactionHandle {

	private final StorageManager sm;
	
	/*
	 * This variable starts false.  It remains false during the
	 * locking stage of a transaction.  It is set to true at the
	 * beginning of the first mutate/mutateMany call in a transaction
	 * (before performing any writes to the backing store). 
	 */
	private boolean isMutationStarted;
	
	private long lastLockApplicationTimeMS;
	
	private final LinkedHashSet<LockClaim> lockClaims =
			new LinkedHashSet<LockClaim>();
	
	private final long lockExpireMS;
	private final long lockWaitMS;
	private final int lockRetryCount;
	
	// These two should be final but are not to facilitate testing
	private byte[] rid;
	private LocalLockMediatorProvider mediators;
	
	private static final Logger log = LoggerFactory.getLogger(LockingTransaction.class);
	
	public LockingTransaction(StorageManager sm, byte[] rid, int lockRetryCount, long lockWaitMS, long lockExpireMS) {
		this.sm = sm;
		
		this.rid = rid;
		
		this.lockRetryCount = lockRetryCount;
		this.lockWaitMS = lockWaitMS;
		this.lockExpireMS = lockExpireMS;

		this.mediators = LocalLockMediators.INSTANCE;
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
	
	/**
	 * This method is only used in testing.
	 * Do not call it.
	 */
	public void setRid(byte rid[]) {
		this.rid = rid;
	}
	
	/**
	 * This method is only used in testing.
	 * Don not call it.
	 */
	public void setLocalLockMediatorProvider(LocalLockMediatorProvider p) {
		this.mediators = p;
	}
	
	public boolean isMutationStarted() {
		return isMutationStarted;
	}
	
	public void mutationStarted() {
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
	public void writeBlindLockClaim(String cf, ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue)
			throws LockingFailureException {

		LockClaim lc = new LockClaim(cf, key, column, expectedValue);
		
		// Check to see whether we already hold this lock
		if (lockClaims.contains(lc)) {
			log.trace("Skipping lock {}: already held", lc);
			return;
		}
		
		// Check the local lock mediator
		LocalLockMediator llm = mediators.get(cf);
		if (!llm.lock(lc.getKc(), this)) {
			throwLockFailure("Lock contention among local transactions");
		}
		
		/* Write lock to the backing store
		 *
		 * The key we write is a concatenation of the arguments key and column,
		 * prefixed by an int (4 bytes) representing the length of the argument key.
		 * 
		 * The column we write is a concatenation of our rid and the timestamp.
		 */
		String lockCfName = getLockColumnFamilyName(cf);
		OrderedKeyColumnValueStore store = sm.openDatabase(lockCfName);

		ByteBuffer lockKey = lc.getLockKey();
		
		ByteBuffer valBuf = ByteBuffer.allocate(4);
		valBuf.putInt(0).rewind();
		
		boolean ok = false;
		try {
			for (int i = 0; i < lockRetryCount; i++) {
				long ts = TimestampProvider.getApproxNSSinceEpoch(false);
				Entry addition = new Entry(lc.getLockCol(ts, rid), valBuf);
				
				long before = System.currentTimeMillis();
				store.mutate(lockKey, Arrays.asList(addition), null, null);
				long after = System.currentTimeMillis();
				
				if (lockWaitMS < after - before) {
					// Too slow
					// Delete lock claim and loop again
					ts = TimestampProvider.getApproxNSSinceEpoch(false);
					store.mutate(lockKey, null, Arrays.asList(lc.getLockCol(ts, rid)), null);
				} else {
					ok = true;
					lastLockApplicationTimeMS = before;
					lc.setTimestamp(ts);
                    log.trace("Wrote lock: {}", lc);
            		lockClaims.add(lc);
					return;
				}
			}

			throwLockFailure("Lock failed: exceeded max timeouts. " + lc);
		} finally {
			if (!ok)
				llm.unlock(lc.getKc(), this);
		}
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
	public void verifyAllLockClaims() throws LockingFailureException {

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
					throwLockFailure(e);
				}
			} else {
				break;
			}
		}
		
		// Check lock claim seniority
		for (LockClaim lc : lockClaims) {
			
			// Get the backing store
			String lockCfName = getLockColumnFamilyName(lc.getCf());
			OrderedKeyColumnValueStore store = sm.openDatabase(lockCfName);
			
			ByteBuffer lockKey = lc.getLockKey();
			ByteBuffer empty = ByteBuffer.allocate(0);
			
			List<Entry> entries = store.getSlice(lockKey, empty, empty, null);
			
			// Determine the timestamp and rid of the earliest still-valid lock claim
			Long earliestTS = null;
			byte[] earliestRid = null;

            log.trace("Retrieved {} total lock claim(s) when verifying {}", entries.size(), lc);
			
			for (Entry e : entries) {
				ByteBuffer bb = e.getColumn();
				long ts = bb.getLong();
				byte[] curRid = new byte[bb.remaining()];
				bb.get(curRid);
				
				// Ignore expired lock claims
				if (ts < now - lockExpireMS) {
                    log.warn("Discarded expired lock with timestamp {}", ts);
					continue;
				}
				
				if (null == earliestTS || ts < earliestTS) {
					// Appoint new winner
					earliestTS = ts;
					earliestRid = curRid;
				} else if (earliestTS == ts) {
					// Timestamp tie: break with column
					// (Column must be unique because it contains Rid)
					ByteBuffer earliestRidBuf = ByteBuffer.wrap(earliestRid);
					ByteBuffer curRidBuf = ByteBuffer.wrap(curRid);
					
					int i = curRidBuf.compareTo(earliestRidBuf);
					
					if (-1 == i) {
						earliestRid = curRid;
					} else if (1 == i) {
						// curRid comes after earliestRid -> don't change earliestRid
					} else {
						// This should never happen
						log.warn("Retrieved duplicate column from Cassandra during lock check!? lc={}", lc);
					}
				}
			}
			
			// Check: did our Rid win?
			if (! Arrays.equals(earliestRid, rid)) {
                log.trace("My rid={} lost to earlier rid={},ts={}",
                		new Object[] { Hex.encodeHexString(rid), Hex.encodeHexString(earliestRid), earliestTS });
				throwLockFailure("A remote transaction holds " + lc);
			}
			
			
			// Check expectedValue
			store = sm.openDatabase(lc.getCf());
			ByteBuffer bb = store.get(lc.getKey(), lc.getColumn(), null);
			if ((null == bb && null != lc.getExpectedValue()) ||
			    (null != bb && null == lc.getExpectedValue()) ||
			    (null != bb && null != lc.getExpectedValue() && !lc.getExpectedValue().equals(bb))) {
				throwLockFailure("Expected value mismatch on " + lc);
			}
		}
	}
	
	private void unlockAll() {
		
		for (LockClaim lc : lockClaims) {
			ByteBuffer lockKeyBuf = lc.getLockKey();
			ByteBuffer lockColBuf = lc.getLockCol(lc.getTimestamp(), rid);
			
			// Get the backing store
			String lockCfName = getLockColumnFamilyName(lc.getCf());
			OrderedKeyColumnValueStore store = sm.openDatabase(lockCfName);
			
			// Delete lock
			store.mutate(lockKeyBuf, null, Arrays.asList(lockColBuf), null);
                        log.trace("Wrote unlock: {}", lc);
			
			// Release local lock
			LocalLockMediator llm = mediators.get(lc.getCf());
			llm.unlock(lc.getKc(), this);
		}
	}
	
	private void throwLockFailure(String s) throws LockingFailureException {
		unlockAll();
		throw new LockingFailureException(s);
	}
	
	private void throwLockFailure(Throwable t) throws LockingFailureException {
		unlockAll();
		throw new LockingFailureException(t);
	}
	
	private String getLockColumnFamilyName(String baseColumnFamilyName) {
		return baseColumnFamilyName + "_locks";
	}
}
