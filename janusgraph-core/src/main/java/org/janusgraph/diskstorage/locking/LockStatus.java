package com.thinkaurelius.titan.diskstorage.locking;


import java.time.Instant;

/**
 * A single held lock's expiration time. This is used by {@link AbstractLocker}.
 *
 * @see AbstractLocker
 * @see ConsistentKeyLockStatus
 */
public interface LockStatus {

    /**
     * Returns the moment at which this lock expires (inclusive).
     *
     * @return The expiration instant of this lock
     */
    public Instant getExpirationTimestamp();
}
