package com.thinkaurelius.titan.graphdb.transaction.lock;


import java.time.Duration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class FakeLock implements TransactionLock {

    public static final FakeLock INSTANCE = new FakeLock();

    private FakeLock() {}

    @Override
    public void lock(Duration timeout) {
    }

    @Override
    public void unlock() {
    }

    @Override
    public boolean inUse() {
        throw new UnsupportedOperationException("Should never be used");
    }

}
