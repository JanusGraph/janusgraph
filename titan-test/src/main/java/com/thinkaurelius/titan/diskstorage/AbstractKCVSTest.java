package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreManager;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.util.time.Timestamps;
import com.thinkaurelius.titan.diskstorage.util.StandardBaseTransactionConfig;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class AbstractKCVSTest {

    protected static final TimestampProvider times = Timestamps.MICRO;

    protected StandardBaseTransactionConfig getTxConfig() {
        return StandardBaseTransactionConfig.of(times);
    }

    protected StandardBaseTransactionConfig getConsistentTxConfig(StoreManager manager) {
        return StandardBaseTransactionConfig.of(times,manager.getFeatures().getKeyConsistentTxConfig());
    }

}
