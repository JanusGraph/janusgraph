package com.thinkaurelius.titan.graphdb.transaction.lock;

import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TransactionLock {

    /**
     * Lock this lock but wait at most the given time to do so.
     * Time unit is {@link com.thinkaurelius.titan.diskstorage.time.Timestamps#SYSTEM()}
     * @param maxTime
     */
    public void lock(long maxTime);

    public void unlock();

    public boolean inUse();



}
