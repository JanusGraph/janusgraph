package com.thinkaurelius.titan.graphdb.transaction.lock;

import com.thinkaurelius.titan.core.attribute.Duration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TransactionLock {

    public void lock(Duration timeout);

    public void unlock();

    public boolean inUse();

}
