package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransactionHandle;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class BackendTransactionHandle {

    private final StoreTransactionHandle storeTx;
    
    public BackendTransactionHandle(final StoreTransactionHandle storeTx) {
        Preconditions.checkNotNull(storeTx);
        this.storeTx=storeTx;
    }

    public StoreTransactionHandle getStoreTransactionHandle() {
        return storeTx;
    }


}
