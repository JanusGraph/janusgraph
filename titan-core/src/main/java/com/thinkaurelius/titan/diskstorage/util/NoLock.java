package com.thinkaurelius.titan.diskstorage.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Implementation of a lock that has no effect, i.e. does not actually lock anything.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class NoLock implements Lock {

    public static final Lock INSTANCE = new NoLock();

    private NoLock() {}

    public static final Lock getLock() {
        return INSTANCE;
    }

    @Override
    public void lock() {
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock() {
        return true;
    }

    @Override
    public boolean tryLock(long l, TimeUnit timeUnit) throws InterruptedException {
        return true;
    }

    @Override
    public void unlock() {

    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

}
