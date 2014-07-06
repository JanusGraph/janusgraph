package com.thinkaurelius.titan.graphdb.log;

import com.thinkaurelius.titan.core.attribute.Timestamp;
import com.thinkaurelius.titan.core.log.TransactionId;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardTransactionId implements TransactionId {

    private String instanceId;
    private long transactionId;
    private Timestamp transactionTime;

    private StandardTransactionId() {} //For serialization

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
