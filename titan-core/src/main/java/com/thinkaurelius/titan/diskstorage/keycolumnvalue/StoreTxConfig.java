package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StoreTxConfig {

    private final ConsistencyLevel consistency;

    private Long timestamp = null;

    public StoreTxConfig() {
        this(ConsistencyLevel.DEFAULT);
    }

    public StoreTxConfig(ConsistencyLevel consistency) {
        Preconditions.checkNotNull(consistency);
        this.consistency = consistency;
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

    public boolean hasTimestamp() {
        return this.timestamp != null;
    }

    public long getTimestamp() {
        Preconditions.checkArgument(timestamp != null, "A timestamp has not been set");
        return timestamp;
    }

}
