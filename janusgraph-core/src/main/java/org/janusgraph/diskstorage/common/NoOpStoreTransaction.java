package org.janusgraph.diskstorage.common;

import org.janusgraph.diskstorage.BaseTransactionConfig;

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
