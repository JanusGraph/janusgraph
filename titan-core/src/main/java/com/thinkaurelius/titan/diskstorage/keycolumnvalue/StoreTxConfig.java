package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StoreTxConfig {

    private final ConsistencyLevel consistency;
    
    private final String metricsPrefix;

    private Long timestamp = null;

    public StoreTxConfig() {
        this(ConsistencyLevel.DEFAULT,
             GraphDatabaseConfiguration.getSystemMetricsPrefix());
    }
    
    public StoreTxConfig(ConsistencyLevel consistency) {
        this(consistency,
             GraphDatabaseConfiguration.getSystemMetricsPrefix());
    }
    
    public StoreTxConfig(String metricsPrefix) {
        this(ConsistencyLevel.DEFAULT, metricsPrefix);
    }

    public StoreTxConfig(ConsistencyLevel consistency, String metricsPrefix) {
        Preconditions.checkNotNull(consistency);
        this.consistency = consistency;
        this.metricsPrefix = metricsPrefix;
    }

    public StoreTxConfig setTimestamp() {
        this.timestamp = TimeUtility.INSTANCE.getApproxNSSinceEpoch();
        return this;
    }

    public StoreTxConfig setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public ConsistencyLevel getConsistency() {
        return consistency;
    }

    public long getTimestamp() {
        if (timestamp==null) setTimestamp();
        assert timestamp!=null;
        return timestamp;
    }
    
    public String getMetricsPrefix() {
        return metricsPrefix;
    }

}
