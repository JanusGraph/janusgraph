package com.thinkaurelius.titan.graphdb.transaction.lock;

import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.attribute.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ReentrantTransactionLock extends ReentrantLock implements TransactionLock {

    private static final long serialVersionUID = -1533050153710486569L;

    /**
     * This value can be changed independent of any other TimeUnit in the
     * codebase. It's an implementation detail of this particular class, since
     * this class extends ReentrantLock which works with (long, TimeUnit) pairs.
     */
    private static final TimeUnit REENTRANT_LOCK_TIME_UNIT = TimeUnit.MICROSECONDS;

    private static final Logger log = LoggerFactory.getLogger(ReentrantTransactionLock.class);

    @Override
    public void lock(Duration timeout) {
        boolean success = false;
        try {
            success = super.tryLock(timeout.getLength(REENTRANT_LOCK_TIME_UNIT), REENTRANT_LOCK_TIME_UNIT);
        } catch (InterruptedException e) {
            log.warn("Interrupted waiting for lock: {}",e);
        }
        if (!success) throw new TitanException("Possible dead lock detected. Waited for transaction lock without success");
    }

    @Override
    public boolean inUse() {
        return super.isLocked() || super.hasQueuedThreads();
    }


}
