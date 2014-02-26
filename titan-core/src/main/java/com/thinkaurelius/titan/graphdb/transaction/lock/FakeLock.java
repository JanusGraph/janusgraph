package com.thinkaurelius.titan.graphdb.transaction.lock;

import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class FakeLock implements TransactionLock {

    public static final FakeLock INSTANCE = new FakeLock();

    private FakeLock() {}

    @Override
    public void lock(long timeMillisecond) {
    }

    @Override
    public void unlock() {
    }

    @Override
    public boolean inUse() {
        throw new UnsupportedOperationException("Should never be used");
    }

}
