package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class BackendTransaction implements TransactionHandle {

    private final StoreTransaction storeTx;

    public BackendTransaction(final StoreTransaction storeTx) {
        Preconditions.checkNotNull(storeTx);
        this.storeTx = storeTx;
    }

    public StoreTransaction getStoreTransactionHandle() {
        return storeTx;
    }

    @Override
    public void commit() throws StorageException {
        storeTx.commit();
    }

    @Override
    public void abort() throws StorageException {
        storeTx.abort();
    }

    @Override
    public void flush() throws StorageException {
        storeTx.flush();
    }
}
