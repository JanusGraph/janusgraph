package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.TransactionHandleConfig;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardTransactionConfig implements TransactionHandleConfig {

    private final String metricsPrefix;

    private Long timestamp = null;

    public StandardTransactionConfig() {
        this(GraphDatabaseConfiguration.getSystemMetricsPrefix());
    }

    public StandardTransactionConfig(String metricsPrefix) {
        this.metricsPrefix = metricsPrefix;
    }

    public StandardTransactionConfig setTimestamp() {
        Preconditions.checkState(timestamp==null,"Timestamp has already been set");
        this.timestamp = TimeUtility.INSTANCE.getApproxNSSinceEpoch();
        return this;
    }

    public StandardTransactionConfig setTimestamp(long timestamp) {
//        Preconditions.checkState(this.timestamp==null,"Timestamp has already been set");
        this.timestamp = timestamp;
        return this;
    }

    @Override
    public long getTimestamp() {
        if (timestamp==null) setTimestamp();
        assert timestamp!=null;
        return timestamp;
    }

    @Override
    public boolean hasMetricsPrefix() {
        return metricsPrefix!=null;
    }

    @Override
    public String getMetricsPrefix() {
        return metricsPrefix;
    }

}
