package com.thinkaurelius.titan.diskstorage.locking;

/**
 * A single held lock's expiration time. This is used by {@link AbstractLocker}.
 * 
 * @see AbstractLocker
 * @see com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockStatus
 */
public interface LockStatus {
    
    /**
     * Returns the number of {@link com.thinkaurelius.titan.diskstorage.time.Timestamps#SYSTEM}{@code .getUnit()} since the UNIX Epoch at which this lock expires.
     * 
     * @return The exipiration timestamp of this lock
     */
    public long getExpirationTimestamp();
}
