package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfigurable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class DefaultTransaction implements BaseTransactionConfigurable {

    private final BaseTransactionConfig config;

    public DefaultTransaction(BaseTransactionConfig config) {
        Preconditions.checkNotNull(config);
        this.config = config;
    }

    @Override
    public BaseTransactionConfig getConfiguration() {
        return config;
    }

    @Override
    public void commit() throws StorageException {
    }

    @Override
    public void rollback() throws StorageException {
    }

}
