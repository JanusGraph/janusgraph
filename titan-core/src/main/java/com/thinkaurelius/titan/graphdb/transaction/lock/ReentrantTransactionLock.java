package com.thinkaurelius.titan.graphdb.transaction.lock;

import com.thinkaurelius.titan.core.TitanException;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ReentrantTransactionLock extends ReentrantLock implements TransactionLock {

    private static final long serialVersionUID = -1533050153710486569L;



    private static final Logger log = LoggerFactory.getLogger(ReentrantTransactionLock.class);

    @Override
    public void lock(Duration timeout) {
        boolean success = false;
        try {
            success = super.tryLock(timeout.toNanos(), TimeUnit.NANOSECONDS);
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
