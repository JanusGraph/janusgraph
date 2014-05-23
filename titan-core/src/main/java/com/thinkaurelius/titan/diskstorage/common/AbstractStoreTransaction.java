package com.thinkaurelius.titan.diskstorage.common;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandleConfig;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

/**
 * Abstract implementation of {@link StoreTransaction} to be used as the basis for more specific implementations.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractStoreTransaction implements StoreTransaction {

    private final TransactionHandleConfig config;

    public AbstractStoreTransaction(TransactionHandleConfig config) {
        Preconditions.checkNotNull(config);
        this.config = config;
    }

    @Override
    public void commit() throws StorageException {
    }

    @Override
    public void rollback() throws StorageException {
    }

    @Override
    public TransactionHandleConfig getConfiguration() {
        return config;
    }

}
