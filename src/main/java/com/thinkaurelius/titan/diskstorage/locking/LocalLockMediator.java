package com.thinkaurelius.titan.diskstorage.locking;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;

/**
 * This class resolves lock contention between two transactions on the same JVM.
 * 
 * This is not just an optimization to reduce network traffic. Locks written by
 * Titan to a distributed key-value store contain an identifier, the "Rid",
 * which is unique only to the process level. The Rid can't tell which
 * transaction in a process holds any given lock. This class prevents two
 * transactions in a single process from concurrently writing the same lock to a
 * distributed key-value store.
 * 
 * @author Dan LaRocque <dalaro@hopcount.org>
 */

public class LocalLockMediator {

	private static final Logger log = LoggerFactory
			.getLogger(LocalLockMediator.class);

	// Locking namespace
	private final String name;

	// TODO maybe tune the initial size or load factor arguments to
	// ConcurrentHashMap's constructor
	private final ConcurrentHashMap<KeyColumn, AuditRecord> locks = new ConcurrentHashMap<KeyColumn, AuditRecord>();

	public LocalLockMediator(String name) {
		this.name = name;

		assert null != this.name;
	}

	/**
	 * Acquire the lock specified by {@code kc}.
	 * 
	 * <p>
	 * For any particular key-column, whatever value of {@code requestor} is
	 * passed to this method must also be passed to the associated later call to
	 * {@link #unlock()}.
	 * <p>
	 * If some requestor {@code r} calls this method on a KeyColumn {@code k}
	 * and this method returns true, then subsequent calls to this method by
	 * {@code r} on {@code l} merely attempt to update the {@code expiresAt}
	 * timestamp. This differs from typical lock reentrance: multiple successful
	 * calls to this method do not require an equal number of calls to
	 * {@code #unlock()}. One {@code #unlock()} call is enough, no matter how
	 * many times a {@code requestor} called {@code lock} beforehand. Note that
	 * updating the timestamp may fail, in which case the lock is considered to
	 * have expired and the calling context should assume it no longer holds the
	 * lock specified by {@code kc}.
	 * <p>
	 * The number of nanoseconds elapsed since the UNIX Epoch is not readily
	 * available within the JVM. When reckoning expiration times, this method
	 * uses the approximation implemented by
	 * {@link TimestampProvider#getApproxNSSinceEpoch(false)}.
	 * <p>
	 * The current implementation of this method returns true when given an
	 * {@code expiresAt} argument in the past. Future implementations may return
	 * false instead.
	 * 
	 * @param kc
	 *            lock identifier
	 * @param requestor
	 *            the object locking {@code kc}
	 * @param expiresAt
	 *            the number of nanoseconds since the Epoch at which this
	 *            acquired lock will be automatically considered unlocked
	 * @return true if the lock is acquired, false if it was not acquired
	 */
	public boolean lock(KeyColumn kc, LockingTransactionHandle requestor,
			long expiresAt) {
		assert null != kc;
		assert null != requestor;

		AuditRecord audit = new AuditRecord(requestor, expiresAt);
		AuditRecord inmap = locks.putIfAbsent(kc, audit);

		boolean success = false;

		if (null == inmap) {
			// Uncontended lock succeeded
			if (log.isTraceEnabled()) {
				log.trace("New local lock created: {} namespace={} txn={}",
						new Object[] { kc, name, requestor });
			}
			success = true;
		} else if (inmap.equals(audit)) {
			// requestor has already locked kc; update expiresAt
			success = locks.replace(kc, inmap, audit);
			if (log.isTraceEnabled()) {
				if (success) {
					log.trace(
							"Updated local lock expiration: {} namespace={} txn={} oldexp={} newexp={}",
							new Object[] { kc, name, requestor, inmap.expires,
									audit.expires });
				} else {
					log.trace(
							"Failed to update local lock expiration: {} namespace={} txn={} oldexp={} newexp={}",
							new Object[] { kc, name, requestor, inmap.expires,
									audit.expires });
				}
			}
		} else if (inmap.expires <= TimestampProvider.getApproxNSSinceEpoch(false)) {
			// the recorded lock has expired; replace it
			success = locks.replace(kc, inmap, audit);
			if (log.isTraceEnabled()) {
				log.trace(
						"Discarding expired lock: {} namespace={} txn={} expired={}",
						new Object[] { kc, name, inmap.holder, inmap.expires });
			}
		} else {
			// we lost to a valid lock
			if (log.isTraceEnabled()) {
				log.trace(
						"Local lock failed: {} namespace={} txn={} (already owned by {})",
						new Object[] { kc, name, requestor, inmap });
			}
		}

		return success;
	}

	/**
	 * Release the lock specified by {@code kc} and which was previously
	 * {@link #lock()}ed by {@code requestor}, if it is possible to release it.
	 * 
	 * @param kc
	 *            lock identifier
	 * @param requestor
	 *            the object which previously locked {@code kc}
	 */
	public void unlock(KeyColumn kc, LockingTransactionHandle requestor) {

		assert locks.containsKey(kc);

		AuditRecord audit = new AuditRecord(requestor, 0);

		assert locks.get(kc).equals(audit);
		
		locks.remove(kc, audit);

		if (log.isTraceEnabled()) {
			log.trace("Local unlock succeeded: {} namespace={} txn={}",
					new Object[] { kc, name, requestor });
		}
	}

	public String toString() {
		return "LocalLockMediator [" + name + ",  ~" + locks.size()
				+ " current locks]";
	}

	private static class AuditRecord {
		private final LockingTransactionHandle holder;
		private final long expires;
		private int hashCode;

		private AuditRecord(LockingTransactionHandle holder, long expires) {
			this.holder = holder;
			this.expires = expires;
		}

		// Equals and hashCode depend only on holder (and not the expiration
		// time)

		@Override
		public int hashCode() {
			if (0 == hashCode)
				hashCode = holder.hashCode();

			return hashCode;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			AuditRecord other = (AuditRecord) obj;
			if (holder == null) {
				if (other.holder != null)
					return false;
			} else if (!holder.equals(other.holder))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "AuditRecord [txn=" + holder + ", expires=" + expires + "]";
		}

	}

}
