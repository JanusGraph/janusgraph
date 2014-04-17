package com.thinkaurelius.titan.graphdb.transaction.lock;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.time.Timestamps;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class CombinerLock implements TransactionLock {

    private final TransactionLock first;
    private final TransactionLock second;

    public CombinerLock(final TransactionLock first, final TransactionLock second) {
        Preconditions.checkArgument(first!=null && second!=null);
        this.first = first;
        this.second = second;
    }

    @Override
    public void lock(long maxTime) {
        long start = Timestamps.SYSTEM().getTime();
        first.lock(maxTime);
        long remaining = Math.max(0,maxTime - (start-Timestamps.SYSTEM().getTime()));
        try {
            second.lock(remaining);
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
