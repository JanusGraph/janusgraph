package com.thinkaurelius.titan.diskstorage.common;

import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;

/**
 * Dummy transaction object that does nothing
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class NoOpStoreTransaction extends AbstractStoreTransaction {

    public NoOpStoreTransaction(BaseTransactionConfig config) {
        super(config);
    }
}
