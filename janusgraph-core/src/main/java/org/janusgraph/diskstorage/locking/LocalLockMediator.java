// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.locking;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.util.KeyColumn;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class resolves lock contention between two transactions on the same JVM.
 * <p>
 * This is not just an optimization to reduce network traffic. Locks written by
 * JanusGraph to a distributed key-value store contain an identifier, the "Rid",
 * which is unique only to the process level. The Rid can't tell which
 * transaction in a process holds any given lock. This class prevents two
 * transactions in a single process from concurrently writing the same lock to a
 * distributed key-value store.
 *
 * @author Dan LaRocque (dalaro@hopcount.org)
 */

public class LocalLockMediator<T> {

    private static final Logger log = LoggerFactory
            .getLogger(LocalLockMediator.class);

    /**
     * Namespace for which this mediator is responsible
     *
     * @see LocalLockMediatorProvider
     */
    private final String name;

    private final TimestampProvider times;

    /**
     * Maps a ({@code key}, {@code column}) pair to the local transaction
     * holding a lock on that pair. Values in this map may have already expired
     * according to {@link AuditRecord#expires}, in which case the lock should
     * be considered invalid.
     */
    private final ConcurrentHashMap<KeyColumn, AuditRecord<T>> locks = new ConcurrentHashMap<>();

    public LocalLockMediator(String name, TimestampProvider times) {
        this.name = name;
        this.times = times;

        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(times);
    }

    /**
     * Acquire the lock specified by {@code kc}.
     * <p>
     * <p>
     * For any particular key-column, whatever value of {@code requestor} is
     * passed to this method must also be passed to the associated later call to
     * {@link #unlock}.
     * <p>
     * If some requestor {@code r} calls this method on a KeyColumn {@code k}
     * and this method returns true, then subsequent calls to this method by
     * {@code r} on {@code l} merely attempt to update the {@code expiresAt}
     * timestamp. This differs from typical lock re-entrance: multiple successful
     * calls to this method do not require an equal number of calls to
     * {@code #unlock()}. One {@code #unlock()} call is enough, no matter how
     * many times a {@code requester} called {@code lock} beforehand. Note that
     * updating the timestamp may fail, in which case the lock is considered to
     * have expired and the calling context should assume it no longer holds the
     * lock specified by {@code kc}.
     * <p>
     * The current implementation of this method returns true when given an
     * {@code expiresAt} argument in the past. Future implementations may return
     * false instead.
     *
     * @param kc        lock identifier
     * @param requester the object locking {@code kc}
     * @param expires   instant at which this lock will automatically expire
     * @return true if the lock is acquired, false if it was not acquired
     */
    public boolean lock(KeyColumn kc, T requester, Instant expires) {
        assert null != kc;
        assert null != requester;

        final StackTraceElement[] acquiredAt = log.isTraceEnabled() ?
                new Throwable("Lock acquisition by " + requester).getStackTrace() : null;

        final AuditRecord<T> audit = new AuditRecord<>(requester, expires, acquiredAt);
        final AuditRecord<T> inMap = locks.putIfAbsent(kc, audit);

        boolean success = false;

        if (null == inMap) {
            // Uncontended lock succeeded
            if (log.isTraceEnabled()) {
                log.trace("New local lock created: {} namespace={} txn={}",
                    kc, name, requester);
            }
            success = true;
        } else if (inMap.equals(audit)) {
            // requester has already locked kc; update expiresAt
            success = locks.replace(kc, inMap, audit);
            if (log.isTraceEnabled()) {
                if (success) {
                    log.trace("Updated local lock expiration: {} namespace={} txn={} oldexp={} newexp={}",
                        kc, name, requester, inMap.expires, audit.expires);
                } else {
                    log.trace("Failed to update local lock expiration: {} namespace={} txn={} oldexp={} newexp={}",
                        kc, name, requester, inMap.expires, audit.expires);
                }
            }
        } else if (0 > inMap.expires.compareTo(times.getTime())) {
            // the recorded lock has expired; replace it
            success = locks.replace(kc, inMap, audit);
            if (log.isTraceEnabled()) {
                log.trace("Discarding expired lock: {} namespace={} txn={} expired={}",
                    kc, name, inMap.holder, inMap.expires);
            }
        } else {
            // we lost to a valid lock
            if (log.isTraceEnabled()) {
                log.trace("Local lock failed: {} namespace={} txn={} (already owned by {})",
                    kc, name, requester, inMap);
                log.trace("Owner stacktrace:\n        {}", StringUtils.join(inMap.acquiredAt,"\n        "));
            }
        }

        return success;
    }

    /**
     * Release the lock specified by {@code kc} and which was previously
     * locked by {@code requester}, if it is possible to release it.
     *
     * @param kc        lock identifier
     * @param requester the object which previously locked {@code kc}
     */
    public boolean unlock(KeyColumn kc, T requester) {

        if (!locks.containsKey(kc)) {
            log.error("Local unlock failed: no locks found for {}", kc);
            return false;
        }

        final AuditRecord<T> unlocker = new AuditRecord<>(requester, null, null);

        final AuditRecord<T> holder = locks.get(kc);

        if (!holder.equals(unlocker)) {
            log.error("Local unlock of {} by {} failed: it is held by {}",
                kc, unlocker, holder);
            return false;
        }

        boolean removed = locks.remove(kc, unlocker);

        if (removed) {
            if (log.isTraceEnabled()) {
                log.trace("Local unlock succeeded: {} namespace={} txn={}",
                    kc, name, requester);
            }
        } else {
            log.warn("Local unlock warning: lock record for {} disappeared "
                    + "during removal; this suggests the lock either expired "
                    + "while we were removing it, or that it was erroneously "
                    + "unlocked multiple times.", kc);
        }

        // Even if !removed, we're finished unlocking, so return true
        return true;
    }

    public String toString() {
        return "LocalLockMediator [" + name + ",  ~" + locks.size()
                + " current locks]";
    }

    /**
     * A record containing the local transaction that holds a lock and the
     * lock's expiration time.
     */
    private static class AuditRecord<T> {

        /**
         * The local transaction that holds/held the lock.
         */
        private final T holder;
        /**
         * The expiration time of a the lock.
         */
        private final Instant expires;
        /**
         * Cached hashCode.
         */
        private int hashCode;

        /**
         * A optional call trace generated when the lock was acquired.
         */
        private final StackTraceElement[] acquiredAt;

        private AuditRecord(T holder, Instant expires, StackTraceElement[] acquiredAt) {
            this.holder = holder;
            this.expires = expires;
            this.acquiredAt = acquiredAt;
        }

        /**
         * This implementation depends only on the lock holder and not on the
         * lock expiration time.
         */
        @Override
        public int hashCode() {
            if (0 == hashCode)
                hashCode = holder.hashCode();

            return hashCode;
        }

        /**
         * This implementation depends only on the lock holder and not on the
         * lock expiration time.
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            /*
             * This warning suppression is harmless because we are only going to
             * call other.holder.equals(...), and since equals(...) is part of
             * Object, it is guaranteed to be defined no matter the concrete
             * type of parameter T.
             */
            @SuppressWarnings("rawtypes")
            AuditRecord other = (AuditRecord) obj;
            if (holder == null) {
                return other.holder == null;
            } else {
                return holder.equals(other.holder);
            }
        }

        @Override
        public String toString() {
            return "AuditRecord [txn=" + holder + ", expires=" + expires + "]";
        }

    }

}
