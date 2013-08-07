package com.thinkaurelius.titan.diskstorage.locking;

import java.util.concurrent.TimeUnit;

/**
 * A single held lock's expiration time. This is used by {@link AbstractLocker}.
 * 
 * @see AbstractLocker
 * @see ConsistentKeyLockStatus
 */
public interface LockStatus {
    
    /**
     * Returns the number of {@code unit} since the UNIX Epoch at which this lock expires.
     * 
     * @param unit The units of the timestamp to return
     * @return The exipiration timestamp of this lock
     */
    public long getExpirationTimestamp(TimeUnit unit);
}
