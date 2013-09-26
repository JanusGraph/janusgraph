package com.thinkaurelius.titan.diskstorage.common;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class NoOpStoreTransaction extends AbstractStoreTransaction {

    public NoOpStoreTransaction(ConsistencyLevel level) {
        super(level);
    }
}
