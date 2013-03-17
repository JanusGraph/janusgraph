package com.thinkaurelius.titan.diskstorage.persistit;

import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;

import com.persistit.Transaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;

public class PersistitTransaction extends AbstractStoreTransaction {

    private Transaction tx;

    public PersistitTransaction(Transaction t, ConsistencyLevel level) {
        super(level);
        tx = t;
    }

    @Override
    public synchronized void abort() throws StorageException {
        if (tx == null) return;
        tx.rollback();
    }

    @Override
    public synchronized void commit() throws StorageException {
        if (tx == null) return;
        int retries = 3;
        try {
            int i = 0;
            while (true) {
                try {
                    tx.commit();
                    break;
                } catch (RollbackException ex) {
                    if (i++ >= retries) {
                        throw ex;
                    }
                }
            }
        } catch (PersistitException ex) {
            throw new PermanentStorageException(ex.toString());
        } finally {
            tx.end();
        }
    }
}
