package com.thinkaurelius.titan.core.log;


import java.time.Instant;

/**
 * Identifies a transaction. Used when processing user log entries to know which transaction caused a given change.
 * A transaction is uniquely identified by the unique identifier of the instance that executed the transaction, the time
 * of the transaction, and an instance local transaction id.
 * <p/>
 * Note, that all 3 pieces of information are required for a unique identification of the transaction.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TransactionId {

    /**
     * Returns the unique id of the Titan graph instance which executed the transaction.
     *
     * @return
     */
    public String getInstanceId();

    /**
     * Returns the unique transaction id within a particular Titan instance.
     * @return
     */
    public long getTransactionId();

    /**
     * Returns the time of the transaction
     *
     * @return
     */
    public Instant getTransactionTime();

}
