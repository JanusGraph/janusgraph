package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.TransactionHandleConfig;
import com.thinkaurelius.titan.diskstorage.util.StandardTransactionConfig;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StoreTxConfig implements TransactionHandleConfig {

    private final ConsistencyLevel consistency;
    private final StandardTransactionConfig txconfig;

    public StoreTxConfig() {
        this(new StandardTransactionConfig());
    }

    public StoreTxConfig(ConsistencyLevel consistency) {
        this(consistency,new StandardTransactionConfig());
    }

    public StoreTxConfig(TransactionHandleConfig txhc) {
        this(ConsistencyLevel.DEFAULT,txhc);
    }

    public StoreTxConfig(ConsistencyLevel consistency, TransactionHandleConfig txhc) {
        Preconditions.checkNotNull(consistency);
        Preconditions.checkNotNull(txhc);
        this.consistency = consistency;
        this.txconfig = (StandardTransactionConfig)txhc;
    }

    public ConsistencyLevel getConsistency() {
        return consistency;
    }

    //TODO: remove once removed in ConsistentKeyLocker
    public void setTimestamp(long timestamp) {
        txconfig.setTimestamp(timestamp);
    }

    @Override
    public long getTimestamp() {
        return txconfig.getTimestamp();
    }

    @Override
    public boolean hasMetricsPrefix() {
        return txconfig.hasMetricsPrefix();
    }

    @Override
    public String getMetricsPrefix() {
        return txconfig.getMetricsPrefix();
    }

}
