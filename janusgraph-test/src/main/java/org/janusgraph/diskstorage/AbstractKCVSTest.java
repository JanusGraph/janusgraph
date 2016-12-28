package org.janusgraph.diskstorage;

import org.janusgraph.diskstorage.keycolumnvalue.StoreManager;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class AbstractKCVSTest {

    protected static final TimestampProvider times = TimestampProviders.MICRO;

    protected StandardBaseTransactionConfig getTxConfig() {
        return StandardBaseTransactionConfig.of(times);
    }

    protected StandardBaseTransactionConfig getConsistentTxConfig(StoreManager manager) {
        return StandardBaseTransactionConfig.of(times,manager.getFeatures().getKeyConsistentTxConfig());
    }

}
