package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;

/**
 * This class overrides and adds nothing compared with
 * {@link com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingTransaction}; however, it creates a transaction type specific
 * to HBase, which lets us check for user errors like passing a Cassandra
 * transaction into a HBase method.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class HBaseTransaction extends AbstractStoreTransaction {

    public HBaseTransaction(final BaseTransactionConfig config) {
        super(config);
    }
}
