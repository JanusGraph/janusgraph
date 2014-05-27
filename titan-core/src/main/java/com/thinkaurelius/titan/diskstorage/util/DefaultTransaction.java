package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandleConfig;
import com.thinkaurelius.titan.diskstorage.TransactionHandleConfigurable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class DefaultTransaction implements TransactionHandleConfigurable {

    private final TransactionHandleConfig config;

    public DefaultTransaction(TransactionHandleConfig config) {
        Preconditions.checkNotNull(config);
        this.config = config;
    }

    @Override
    public TransactionHandleConfig getConfiguration() {
        return config;
    }

    @Override
    public void commit() throws StorageException {
    }

    @Override
    public void rollback() throws StorageException {
    }

}
