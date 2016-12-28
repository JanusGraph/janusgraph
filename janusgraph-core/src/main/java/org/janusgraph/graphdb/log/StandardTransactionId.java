package com.thinkaurelius.titan.graphdb.log;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.log.TransactionId;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.time.Instant;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardTransactionId implements TransactionId {

    private String instanceId;
    private long transactionId;
    private Instant transactionTime;

    public StandardTransactionId(String instanceId, long transactionId, Instant transactionTime) {
        Preconditions.checkArgument(instanceId!=null && transactionId>=0 && transactionTime!=null);
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
    public Instant getTransactionTime() {
        return transactionTime;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(instanceId).append(transactionId).append(transactionTime).toHashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (this==oth) return true;
        else if (oth==null || !getClass().isInstance(oth)) return false;
        StandardTransactionId id = (StandardTransactionId)oth;
        return instanceId.equals(id.instanceId) && transactionId==id.transactionId
                && transactionTime.equals(id.transactionTime);
    }

    @Override
    public String toString() {
        return transactionId+"@"+instanceId+"::"+transactionTime;
    }

}
