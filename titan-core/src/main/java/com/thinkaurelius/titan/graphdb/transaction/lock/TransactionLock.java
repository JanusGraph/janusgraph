package com.thinkaurelius.titan.graphdb.transaction.lock;

import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TransactionLock {

    public void lock(long timeMillisecond);

    public void unlock();

    public boolean inUse();



}
