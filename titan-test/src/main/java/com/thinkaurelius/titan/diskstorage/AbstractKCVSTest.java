package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreManager;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.util.time.Timestamps;
import com.thinkaurelius.titan.diskstorage.util.StandardTransactionHandleConfig;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class AbstractKCVSTest {

    protected static final TimestampProvider times = Timestamps.MICRO;

    protected StandardTransactionHandleConfig getTxConfig() {
        return StandardTransactionHandleConfig.of(times);
    }

    protected StandardTransactionHandleConfig getConsistentTxConfig(StoreManager manager) {
        return StandardTransactionHandleConfig.of(times,manager.getFeatures().getKeyConsistentTxConfig());
    }

}
