package org.janusgraph.core.log;

import org.janusgraph.core.JanusGraphException;

/**
 * {@link TransactionRecovery} is a process that runs in the background and read's from the transaction
 * write-ahead log to determine which transactions have not been successfully persisted against all
 * backends. It then attempts to recover such transactions.
 * <p/>
 * This process is started via {@link org.janusgraph.core.JanusGraphFactory#}
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TransactionRecovery {

    /**
     * Shuts down the transaction recovery process
     *
     * @throws JanusGraphException
     */
    public void shutdown() throws JanusGraphException;

}
