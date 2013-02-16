package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.indexing.IndexTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class BackendTransaction implements TransactionHandle {

    private final StoreTransaction storeTx;
    private final Map<String,IndexTransaction> indexTx;

    public BackendTransaction(final StoreTransaction storeTx, Map<String, IndexTransaction> indexTx) {
        Preconditions.checkNotNull(storeTx);
        Preconditions.checkNotNull(indexTx);
        this.indexTx = indexTx;
        this.storeTx = storeTx;
    }

    public StoreTransaction getStoreTransactionHandle() {
        return storeTx;
    }

    public IndexTransaction getIndexTransactionHandle(String index) {
        Preconditions.checkArgument(StringUtils.isNotBlank(index));
        IndexTransaction itx = indexTx.get(index);
        Preconditions.checkNotNull(itx,"Unknown index: " + index);
        return itx;
    }

    @Override
    public void commit() throws StorageException {
        storeTx.commit();
        for (IndexTransaction itx : indexTx.values()) itx.commit();
    }

    @Override
    public void abort() throws StorageException {
        storeTx.abort();
        for (IndexTransaction itx : indexTx.values()) itx.abort();
    }

    @Override
    public void flush() throws StorageException {
        storeTx.flush();
        for (IndexTransaction itx : indexTx.values()) itx.flush();
    }
}
