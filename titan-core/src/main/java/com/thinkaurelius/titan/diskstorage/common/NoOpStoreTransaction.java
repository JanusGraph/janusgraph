package com.thinkaurelius.titan.diskstorage.common;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class NoOpStoreTransaction extends AbstractStoreTransaction {

    public NoOpStoreTransaction(StoreTxConfig config) {
        super(config);
    }
}
