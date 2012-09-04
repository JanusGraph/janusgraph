package com.thinkaurelius.titan.diskstorage.locking;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import com.thinkaurelius.titan.diskstorage.*;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;

/**
 * This class implements locking according to the Titan key-column-expectedvalue
 * protocol.
 * 
 * This class is not safe for concurrent use by multiple threads.
 * 
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public abstract class LockingTransactionHandle implements TransactionHandle {
	
	/**
	 * This variable starts false.  It remains false during the
	 * locking stage of a transaction.  It is set to true at the
	 * beginning of the first mutate/mutateMany call in a transaction
	 * (before performing any writes to the backing store). 
	 */
	private boolean isMutationStarted;
	
	/**
	 * This variable holds the last time we successfully wrote a
	 * lock via the {@link #writeBlindLockClaim(com.thinkaurelius.titan.diskstorage.LockConfig, java.nio.ByteBuffer, java.nio.ByteBuffer, java.nio.ByteBuffer)}
     * method.
	 */
	private final Map<LockConfig, Long> lastLockApplicationTimesMS =
			new HashMap<LockConfig, Long>();
	
	/**
	 * All locks currently claimed by this transaction.  Note that locks
	 * in this set may not necessarily be actually held; membership in the
	 * set only signifies that we've attempted to claim the lock via the
	 * {@link #writeBlindLockClaim(com.thinkaurelius.titan.diskstorage.LockConfig, java.nio.ByteBuffer, java.nio.ByteBuffer, java.nio.ByteBuffer)} method.
	 */
	private final LinkedHashSet<LockClaim> lockClaims =
			new LinkedHashSet<LockClaim>();
	
	private static final Logger log = LoggerFactory.getLogger(LockingTransactionHandle.class);
	
	private static final long MILLION = 1000000;
	
	public LockingTransactionHandle() {
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
	 * throw a StorageException with a string message to that effect.
	 */
	public void writeBlindLockClaim(
			LockConfig backer, ByteBuffer key,
			ByteBuffer column, ByteBuffer expectedValue)
			throws StorageException {

		LockClaim lc = new LockClaim(backer, key, column, expectedValue);
		
		// Check to see whether we already hold this lock
		if (lockClaims.contains(lc)) {
			log.trace("Skipping lock {}: already held", lc);
			return;
		}
		
		/* Check the local lock mediator.
		 * 
		 * The timestamp calculated here is only approximate.  If it turns out that we
		 * spend longer than the expiration period attempting to finish the rest of
		 * this method, then there's a window of time in which the LocalLockMediator
		 * may tell other threads that our key-column target is unlocked.  Lock conflict
		 * is still detected in such cases during verifyAllLockClaims() below (it's
		 * just slower than when LocalLockMediator gives the correct answer).
		 * 
		 * We'll also update the timestamp in the LocalLockMediator after we're done
		 * talking to the backend store.
		 * 
		 * We use TimestampProvider.getApproxNSSinceEpoch()/1000 instead of the
		 * superficially equivalent System.currentTimeMillis() to get consistent timestamp
		 * rollovers.
		 */
		long tempts = TimestampProvider.getApproxNSSinceEpoch(false) + 
				backer.getLockExpireMS() * MILLION;
		if (!backer.getLocalLockMediator().lock(lc.getKc(), this, tempts)) {
			throw new PermanentLockingException("Lock could not be acquired because it is held by a local transaction [" + lc + "]");
		}
		
		/* Write lock to the backing store
		 *
		 * The key we write is a concatenation of the arguments key and column,
		 * prefixed by an int (4 bytes) representing the length of the argument key.
		 * 
		 * The column we write is a concatenation of our rid and the timestamp.
		 */
		ByteBuffer lockKey = lc.getLockKey();
		
		ByteBuffer valBuf = ByteBuffer.allocate(4);
		valBuf.putInt(0).rewind();
		
		boolean ok = false;
		long tsNS = 0;
		try {
			for (int i = 0; i < backer.getLockRetryCount(); i++) {
				tsNS = TimestampProvider.getApproxNSSinceEpoch(false);
				Entry addition = new Entry(lc.getLockCol(tsNS, backer.getRid()), valBuf);
				
				long before = System.currentTimeMillis();
				backer.getLockStore().mutate(lockKey, Arrays.asList(addition), null, null);
				long after = System.currentTimeMillis();
				
				if (backer.getLockWaitMS() < after - before) {
					// Too slow
					// Delete lock claim and loop again
					backer.getLockStore().mutate(lockKey, null, Arrays.asList(lc.getLockCol(tsNS, backer.getRid())), null);
				} else {
					ok = true;
					lastLockApplicationTimesMS.put(backer, before);
					lc.setTimestamp(tsNS);
                    log.trace("Wrote lock: {}", lc);
            		lockClaims.add(lc);
					return;
				}
			}

			throw new TemporaryLockingException("Lock failed: exceeded max timeouts [" + lc + "]");
		} finally {
			if (ok) {
				// Update the timeout
				assert 0 != tsNS;
				boolean expireTimeUpdated = backer.getLocalLockMediator().lock(
						lc.getKc(), this, tsNS + MILLION * backer.getLockExpireMS());
				
				if (!expireTimeUpdated)
					log.warn("Failed to update expiration time of local lock {}; is titan.storage.lock-expiry-time too low?");
				
				/*
				 * No action is immediately necessary even if we failed to re-lock locally.
				 * 
				 * Any failure to re-lock locally will be detected later in verifyAllLockClaims().
				 */

			} else {
				backer.getLocalLockMediator().unlock(lc.getKc(), this);
			}
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
	 * then we throw StorageException to that effect.
	 * <p>
	 * If there is an expectedValue mismatch between any locking request
	 * and actual key-column value, then we throw StorageException
	 * to that effect.
	 * <p>
	 * In other words, if this method returns without throwing an
	 * exception, then the transaction holds all locks it has previously
	 * requested and the expectedValue associated with each transaction
	 * matches reality.
	 * 
	 */
	public void verifyAllLockClaims() throws StorageException {

		// wait one full lockWaitMS since the last claim attempt, if needed
		if (0 == lastLockApplicationTimesMS.size())
			return; // no locks
		
		long now = TimestampProvider.getApproxNSSinceEpoch(false);
		
		// Iterate over all backends and sleep, if necessary, until
		// the backend-specific grace period since our last lock application
		// has passed.
		for (LockConfig i : lastLockApplicationTimesMS.keySet()) {
			long appTimeMS = lastLockApplicationTimesMS.get(i);
			
			long mustSleepUntil = appTimeMS + i.getLockWaitMS();
			
			if (mustSleepUntil < now / MILLION) {
				continue;
			}
			
			sleepUntil(appTimeMS + i.getLockWaitMS());
		}
		
		// Check lock claim seniority
		for (LockClaim lc : lockClaims) {
			
			ByteBuffer lockKey = lc.getLockKey();
			ByteBuffer empty = ByteBuffer.allocate(0);
			
			LockConfig backer = lc.getBacker();
			
			List<Entry> entries = backer.getLockStore().getSlice(lockKey, empty, empty, null);
			
			// Determine the timestamp and rid of the earliest still-valid lock claim
			Long earliestNS = null;
			byte[] earliestRid = null;

            log.trace("Retrieved {} total lock claim(s) when verifying {}", entries.size(), lc);
			
			for (Entry e : entries) {
				ByteBuffer bb = e.getColumn();
				long tsNS = bb.getLong();
				byte[] curRid = new byte[bb.remaining()];
				bb.get(curRid);
				
				// Ignore expired lock claims
				if (tsNS < now - (backer.getLockExpireMS() * MILLION)) {
                    log.warn("Discarded expired lock with timestamp {}", tsNS);
					continue;
				}
				
				if (null == earliestNS || tsNS < earliestNS) {
					// Appoint new winner
					earliestNS = tsNS;
					earliestRid = curRid;
				} else if (earliestNS == tsNS) {
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
			byte rid[] = backer.getRid();
			if (! Arrays.equals(earliestRid, rid)) {
                log.trace("My rid={} lost to earlier rid={},ts={}",
                		new Object[] {
                		Hex.encodeHexString(rid),
                		null != earliestRid ? Hex.encodeHexString(earliestRid) : "null",
                		earliestNS });
				throw new PermanentLockingException("Lock could not be acquired because it is held by a remote transaction [" + lc + "]");
			}
			
			
			// Check expectedValue
			ByteBuffer bb = backer.getDataStore().get(lc.getKey(), lc.getColumn(), null);
			if ((null == bb && null != lc.getExpectedValue()) ||
			    (null != bb && null == lc.getExpectedValue()) ||
			    (null != bb && null != lc.getExpectedValue() && !lc.getExpectedValue().equals(bb))) {
				throw new PermanentLockingException("Updated state: lock acquired but value has changed since read [" + lc + "]");
			}
		}
	}

	private void unlockAll() {

		for (LockClaim lc : lockClaims) {

			assert null != lc;
			ByteBuffer lockKeyBuf = lc.getLockKey();
			assert null != lockKeyBuf;
			assert lockKeyBuf.hasRemaining();
			ByteBuffer lockColBuf = lc.getLockCol(lc.getTimestamp(), lc.getBacker().getRid());
			assert null != lockColBuf;
			assert lockColBuf.hasRemaining();
			
			try {	
				// Release lock remotely
				lc.getBacker().getLockStore().mutate(lockKeyBuf, null, Arrays.asList(lockColBuf), null);
			
				if (log.isTraceEnabled()) {
					log.trace("Wrote unlock {}", lc);
				}
			} catch (Throwable t) {
				log.error("Failed to unlock {}", lc, t);
			}

			try {
				// Release lock locally
				lc.getBacker().getLocalLockMediator().unlock(lc.getKc(), this);
				
				if (log.isTraceEnabled()) {
					log.trace("Locally unlocked {}", lc);
				}
			} catch (Throwable t) {
				log.error("Failed to locally unlock {}", lc, t);
			}
		}
	}

	
	private void sleepUntil(long untilTimeMillis) throws StorageException {
		long now;
		
		while (true) {
			now = System.currentTimeMillis();
			
			if (now > untilTimeMillis) {
				break;
			}
			
			long delta = untilTimeMillis - now + 1;
			
			assert 0 <= delta;
			
			try {
				log.debug("About to sleep for {} ms", delta);
				Thread.sleep(delta);
			} catch (InterruptedException e) {
				throw new TemporaryLockingException("Interrupted while waiting for lock verification",e);
			}
		}
	}
}
