package com.thinkaurelius.titan.graphdb.transaction.lock;

import com.google.common.base.Preconditions;

import com.thinkaurelius.titan.diskstorage.util.time.Timer;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;

import java.time.Duration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class CombinerLock implements TransactionLock {

    private final TransactionLock first;
    private final TransactionLock second;
    private final TimestampProvider times;

    public CombinerLock(final TransactionLock first, final TransactionLock second, TimestampProvider times) {
        this.first = first;
        this.second = second;
        this.times = times;
        Preconditions.checkNotNull(this.first);
        Preconditions.checkNotNull(this.second);
        Preconditions.checkNotNull(this.times);
    }

    @Override
    public void lock(Duration timeout) {
        Timer t = times.getTimer().start();
        first.lock(timeout);
        Duration remainingTimeout = timeout.minus(t.elapsed());
        try {
            second.lock(remainingTimeout);
        } catch (RuntimeException e) {
            first.unlock();
            throw e;
        }
    }

    @Override
    public void unlock() {
        try {
            first.unlock();
        } finally {
            second.unlock();
        }

    }

    @Override
    public boolean inUse() {
        return first.inUse() || second.inUse();
    }
}
