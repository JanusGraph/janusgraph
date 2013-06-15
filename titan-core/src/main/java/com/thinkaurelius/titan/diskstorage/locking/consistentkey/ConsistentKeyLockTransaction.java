package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * A {@link StoreTransaction} that supports locking via
 * {@link LocalLockMediator} and writing and reading lock records in a
 * {@link ConsistentKeyLockStore}.
 * 
 * <p>
 * <b>This class is not safe for concurrent use by multiple threads.
 * Multithreaded access must be prevented or externally synchronized.</b>
 * 
 */
public class ConsistentKeyLockTransaction implements StoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(ConsistentKeyLockTransaction.class);

    private static final long MILLION = 1000000;


    /**
     * This variable starts false.  It remains false during the
     * locking stage of a transaction.  It is set to true at the
     * beginning of the first mutate/mutateMany call in a transaction
     * (before performing any writes to the backing store).
     */
    private boolean isMutationStarted;

    /**
     * This variable holds the last time we successfully wrote a
     * lock via the {@link #writeBlindLockClaim(ConsistentKeyLockStore, StaticBuffer, StaticBuffer, StaticBuffer)}
     * method.
     */
    private final Map<ConsistentKeyLockStore, Long> lastLockApplicationTimesMS =
            new HashMap<ConsistentKeyLockStore, Long>();

    /**
     * All locks currently claimed by this transaction.  Note that locks
     * in this set may not necessarily be actually held; membership in the
     * set only signifies that we've attempted to claim the lock via the
     * {@link #writeBlindLockClaim(ConsistentKeyLockStore, StaticBuffer, StaticBuffer, StaticBuffer)} method.
     */
    private final LinkedHashSet<LockClaim> lockClaims =
            new LinkedHashSet<LockClaim>();

    private final StoreTransaction baseTx;
    private final StoreTransaction consistentTx;

    public ConsistentKeyLockTransaction(StoreTransaction baseTx, StoreTransaction consistentTx) {
        Preconditions.checkArgument(consistentTx.getConsistencyLevel() == ConsistencyLevel.KEY_CONSISTENT);
        this.baseTx = baseTx;
        this.consistentTx = consistentTx;
    }

    StoreTransaction getWrappedTransaction() {
        return baseTx;
    }

    @Override
    public void rollback() throws StorageException {
        if (0 < lockClaims.size())
            unlockAll();
        baseTx.rollback();
    }

    @Override
    public void commit() throws StorageException {
        if (0 < lockClaims.size())
            unlockAll();
        baseTx.commit();
    }

    @Override
    public void flush() throws StorageException {
        baseTx.flush();
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        return baseTx.getConsistencyLevel();
    }

    /**
     * Tells whether this transaction has been used in a
     * {@link ConsistentKeyLockStore#mutate(StaticBuffer, List, List, StoreTransaction)}
     * call. When this returns true, the transaction is no longer allowed in
     * calls to
     * {@link ConsistentKeyLockStore#acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)}.
     *  
     * @return False until
     *         {@link ConsistentKeyLockStore#mutate(StaticBuffer, List, List, StoreTransaction)}
     *         is called on this transaction instance. Returns true forever
     *         after.
     */
    public boolean isMutationStarted() {
        return isMutationStarted;
    }

    /**
     * Signals the transaction that it has been used in a call to
     * {@link ConsistentKeyLockStore#mutate(StaticBuffer, List, List, StoreTransaction)}
     * . This transaction can't be used in subsequent calls to
     * {@link ConsistentKeyLockStore#acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)}
     * .
     * <p>
     * Calling this method at the appropriate time is handled automatically by
     * {@link ConsistentKeyLockStore}. Titan users don't need to call this
     * method by hand.
     */
    public void mutationStarted() {
        isMutationStarted = true;
    }

    /**
     * Attempts to lock the supplied {@code (key, column, expectedValue)}.
     * 
     * <p>
     * 
     * Conflicts with locks held by other transactions within the process are
     * detected using {@link LocalLockMediator} before this method returns. Such
     * conflicts generate LockingExceptions.
     * 
     * <p>
     * 
     * Conflicts with locks held by transactions in other processes will not be
     * detected before this method returns. We optimistically write a lock claim
     * to {@code backer}'s {@code lockStore}, but whether the claim takes
     * precedence and the lock succeeded won't be checked until
     * {@link #verifyAllLockClaims()}.
     * 
     * <p>
     * 
     * Unless there's an StorageException-worthy problem communicating with
     * {@code backer}'s {@code lockStore}, such as failure to connect or
     * exceptionally high write latency, this method concludes by appending a
     * {@link LockClaim} to its {@code #lockClaims} hash set and then returns.
     * The {@code LockClaim} lets {@code #verifyAllLockClaims()} find and check
     * this lock attempt when called.
     * 
     * <p>
     * 
     * Therefore, if this method returns instead of throwing an exception, we
     * know the following:
     * 
     * <ul>
     * <li>The attempted lock conflicts with no other transactions in the
     * process</li>
     * <li>Whether the attempted lock conflicts with transactions in remote
     * processes is uncertain until {@code #verifyAllLockClaims()} returns</li>
     * </ul>
     * 
     * @param backer
     *            the store containing lock data and configuration parameters
     * @param key
     *            the key to lock
     * @param column
     *            the column to lock
     * @param expectedValue
     *            the value which must be present at {@code (key, column)} in
     *            {@code backer}'s {@code dataStore} when
     *            {@code #verifyAllLockClaims()} is called later
     * @throws com.thinkaurelius.titan.diskstorage.locking.LockingException
     * @throws StorageException
     */
    public void writeBlindLockClaim(
            ConsistentKeyLockStore backer, StaticBuffer key,
            StaticBuffer column, StaticBuffer expectedValue)
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
		 * We use TimeUtility.getApproxNSSinceEpoch()/1000 instead of the
		 * superficially equivalent System.currentTimeMillis() to get consistent timestamp
		 * rollovers.
		 */
        long tempts = TimeUtility.getApproxNSSinceEpoch(false) +
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
        StaticBuffer lockKey = lc.getLockKey();

        StaticBuffer valBuf = ByteBufferUtil.getIntBuffer(0);

        boolean ok = false;
        long tsNS = 0;
        try {
            for (int i = 0; i < backer.getLockRetryCount(); i++) {
                tsNS = TimeUtility.getApproxNSSinceEpoch(false);
                Entry addition = StaticBufferEntry.of(lc.getLockCol(tsNS, backer.getRid()), valBuf);

                long before = System.currentTimeMillis();
                backer.getLockStore().mutate(lockKey, Arrays.asList(addition), KeyColumnValueStore.NO_DELETIONS, consistentTx);
                long after = System.currentTimeMillis();

                if (backer.getLockWaitMS() < after - before) {
                    // Too slow
                    // Delete lock claim and loop again
                    backer.getLockStore().mutate(lockKey, KeyColumnValueStore.NO_ADDITIONS, Arrays.asList(lc.getLockCol(tsNS, backer.getRid())), consistentTx);
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

    /**
     * For each object in the {@link #lockClaims} set, this method verifies both
     * of the following conditions:
     * 
     * <ol>
     * <li>that no transaction in another Titan process holds the lock</li>
     * <li>that the claim's expectedValue, as provided in the earlier call to
     * {@link #writeBlindLockClaim(ConsistentKeyLockStore, StaticBuffer, StaticBuffer, StaticBuffer)
     * writeBlindLockClaim()}, matches the actual value in the data store</li>
     * </ol>
     * 
     * <p>
     * If this method reads {@code lockStore} and finds that a transaction in a
     * different Titan process holds one of our claimed locks, then this method
     * throws a {@code LockingException} and the transaction's lock attempts
     * should be considered failed.
     * <p>
     * If this method finds a mismatch between a claim's expected value and
     * actual value, then it will also throw a {@code LockingException}.
     * <p>
     * If this method returns without throwing an exception, then the
     * transaction holds all locks it previously requested via
     * {@code writeBlindLockClaim()} and the expectedValue associated with each
     * transaction matches the actual values seen in the {@code dataStore}.
     * 
     * @throws StorageException
     *             if there's an unexpected problem talking to {@code backer}'s
     *             {@code dataStore} or {@code lockStore}
     * @throws com.thinkaurelius.titan.diskstorage.locking.LockingException
     *             if a lock claim has failed
     */
    public void verifyAllLockClaims() throws StorageException {

        // wait one full idApplicationWaitMS since the last claim attempt, if needed
        if (0 == lastLockApplicationTimesMS.size())
            return; // no locks

        long now = TimeUtility.getApproxNSSinceEpoch(false);

        // Iterate over all backends and sleep, if necessary, until
        // the backend-specific grace period since our last lock application
        // has passed.
        for (ConsistentKeyLockStore i : lastLockApplicationTimesMS.keySet()) {
            long appTimeMS = lastLockApplicationTimesMS.get(i);

            long mustSleepUntil = appTimeMS + i.getLockWaitMS();

            if (mustSleepUntil < now / MILLION) {
                continue;
            }

            TimeUtility.sleepUntil(appTimeMS + i.getLockWaitMS(), log);
        }

        // Check lock claim seniority
        for (LockClaim lc : lockClaims) {

            StaticBuffer lockKey = lc.getLockKey();

            ConsistentKeyLockStore backer = lc.getBacker();
            int bufferLen = backer.getRid().length+8;
            StaticBuffer lower = ByteBufferUtil.zeroBuffer(bufferLen);
            StaticBuffer upper = ByteBufferUtil.oneBuffer(bufferLen);
            List<Entry> entries = backer.getLockStore().getSlice(new KeySliceQuery(lockKey, lower, upper), consistentTx);

            // Determine the timestamp and rid of the earliest still-valid lock claim
            Long earliestNS = null;
            Long latestNS = null;
            byte[] earliestRid = null;
            Set<StaticBuffer> ridsSeen = new HashSet<StaticBuffer>();

            log.trace("Retrieved {} total lock claim(s) when verifying {}", entries.size(), lc);

            for (Entry e : entries) {
                StaticBuffer bb = e.getColumn();
                long tsNS = bb.getLong(0);
                byte[] curRid = new byte[bb.length()-8];
                for (int i=8;i<bb.length();i++) curRid[i-8]=bb.getByte(i);

                StaticBuffer curRidBuf = new StaticArrayBuffer(curRid);
                ridsSeen.add(curRidBuf);
                
                // Ignore expired lock claims
                if (tsNS < now - (backer.getLockExpireMS() * MILLION)) {
                    log.warn("Discarded expired lock with timestamp {}", tsNS);
                    continue;
                }
                
                if (null == latestNS || tsNS > latestNS) {
                    latestNS = tsNS;
                }
                
                if (null == earliestNS || tsNS < earliestNS) {
                    // Appoint new winner
                    earliestNS = tsNS;
                    earliestRid = curRid;
                } else if (earliestNS == tsNS) {
                    // Timestamp tie: break with column
                    // (Column must be unique because it contains Rid)
                    StaticBuffer earliestRidBuf = new StaticArrayBuffer(earliestRid);

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
            StaticBuffer myRidBuf = new StaticArrayBuffer(rid);
            if (!Arrays.equals(earliestRid, rid)) {
                log.trace("My rid={} lost to earlier rid={},ts={}",
                        new Object[]{
                                Hex.encodeHex(rid),          // TODO: I MADE THIS encodeHex from encodeHexString ?!
                                null != earliestRid ? Hex.encodeHex(earliestRid) : "null",
                                earliestNS});
                throw new PermanentLockingException("Lock could not be acquired because it is held by a remote transaction [" + lc + "]");
            }
            
            // Check timestamp
            if (earliestNS != lc.getTimestamp()) {
                if (1 == ridsSeen.size() && lc.getTimestamp() == latestNS && ridsSeen.iterator().next().equals(myRidBuf)) {
                    log.debug("Ignoring prior unexpired lock claim from own rid ({}) with timestamp {} (expected {})",
                            new Object[] { Hex.encodeHexString(earliestRid), earliestNS, latestNS } );
                } else {
                    log.warn("Timestamp mismatch: expected={}, actual={}", lc.getTimestamp(), earliestNS);
                    /*
                     * This is probably evidence of a prior attempt to write a lock
                     * that the client perceived as a failure but which in fact
                     * succeeded.
                     * 
                     * Since the Rid is ours, we could theoretically delete the lock
                     * and even attempt to obtain it all over again, but that
                     * implies significant refactoring.
                     * 
                     * Eventually, the earlier stale lock claim will expire and
                     * progress will resume.
                     */
                    throw new PermanentLockingException("Lock could not be acquired due to timestamp mismatch [" + lc + "]");
                }
            }

            // Check expectedValue
            StaticBuffer bb = KCVSUtil.get(backer.getDataStore(),lc.getKey(), lc.getColumn(), baseTx);
            if ((null == bb && null != lc.getExpectedValue()) ||
                    (null != bb && null == lc.getExpectedValue()) ||
                    (null != bb && null != lc.getExpectedValue() && !lc.getExpectedValue().equals(bb))) {
                throw new PermanentLockingException("Updated state: lock acquired but value has changed since read [" + lc + "]");
            }
        }
    }

    private void unlockAll() {
    	
    	long nowNS = TimeUtility.getApproxNSSinceEpoch(false);

        for (LockClaim lc : lockClaims) {

            assert null != lc;
            StaticBuffer lockKeyBuf = lc.getLockKey();
            assert null != lockKeyBuf;
//            assert lockKeyBuf.hasRemaining();
            StaticBuffer lockColBuf = lc.getLockCol(lc.getTimestamp(), lc.getBacker().getRid());
            assert null != lockColBuf;
//            assert lockColBuf.hasRemaining();
            
            // Log expired locks
            if (lc.getTimestamp() + (lc.getBacker().getLockExpireMS() * MILLION) < nowNS) {
            	log.error("Lock expired: {} (txn={})", lc, this);
            }

            try {
                // Release lock remotely
                lc.getBacker().getLockStore().mutate(lockKeyBuf, KeyColumnValueStore.NO_ADDITIONS, Arrays.asList(lockColBuf), consistentTx);

                if (log.isTraceEnabled()) {
                    log.trace("Released {} in lock store (txn={})", lc, this);
                }
            } catch (Throwable t) {
                log.error("Unexpected exception when releasing {} in lock store (txn={})", lc, this);
                log.error("Lock store failure exception follows", t);
            }

            try {
                // Release lock locally
            	// If lc is unlocked normally, then this method returns true
            	// If there's a problem (e.g. lc has expired), it returns false
            	boolean locallyUnlocked = lc.getBacker().getLocalLockMediator().unlock(lc.getKc(), this);
            	
            	if (locallyUnlocked) {
            		if (log.isTraceEnabled()) {
            			log.trace("Released {} locally (txn={})", lc, this);
            		}
            	} else {
            		log.warn("Failed to release {} locally (txn={})", lc, this);
            	}
            } catch (Throwable t) {
                log.error("Unexpected exception while locally releasing {} (txn={})", lc, this);
                log.error("Local release failure exception follows", t);
            }
        }
    }

}
