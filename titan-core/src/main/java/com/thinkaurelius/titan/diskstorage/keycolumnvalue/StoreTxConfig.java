package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StoreTxConfig {

    private final ConsistencyLevel consistency;

    public StoreTxConfig() {
        this(ConsistencyLevel.DEFAULT);
    }

    public StoreTxConfig(ConsistencyLevel consistency) {
        Preconditions.checkNotNull(consistency);
        this.consistency = consistency;
    }

    public ConsistencyLevel getConsistency() {
        return consistency;
    }

}
