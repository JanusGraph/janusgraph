package com.thinkaurelius.titan.graphdb.log;

import com.thinkaurelius.titan.core.attribute.Timestamp;
import com.thinkaurelius.titan.core.log.TransactionId;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardTransactionId implements TransactionId {

    private final String instanceId;
    private final long transactionId;
    private final Timestamp transactionTime;

    public StandardTransactionId(String instanceId, long transactionId, Timestamp transactionTime) {
        this.instanceId = instanceId;
        this.transactionId = transactionId;
        this.transactionTime = transactionTime;
    }

    @Override
    public String getInstanceId() {
        return instanceId;
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public Timestamp getTransactionTime() {
        return transactionTime;
    }
}
