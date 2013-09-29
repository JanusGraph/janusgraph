package com.thinkaurelius.titan.diskstorage.common;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;

/**
 * Abstract implementation of {@link StoreTransaction} to be used as the basis for more specific implementations.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractStoreTransaction implements StoreTransaction {

    private static final long NO_COMMIT = Long.MIN_VALUE;

    private final StoreTxConfig config;
    private long commitTime = NO_COMMIT;

    public AbstractStoreTransaction(StoreTxConfig config) {
        Preconditions.checkNotNull(config);
        this.config = config;
    }

    @Override
    public void commit() throws StorageException {
        commitTime = TimeUtility.INSTANCE.getApproxNSSinceEpoch(true);
    }

    @Override
    public void rollback() throws StorageException {
    }

    @Override
    public void flush() throws StorageException {
    }

    @Override
    public long getTimestamp() {
        if (config.hasTimestamp()) return config.getTimestamp();
        Preconditions.checkArgument(commitTime != NO_COMMIT, "Transaction has not yet been commited");
        return commitTime;
    }

    @Override
    public StoreTxConfig getConfiguration() {
        return config;
    }

}
