package com.thinkaurelius.titan.diskstorage.persistit;

import com.persistit.TransactionRunnable;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;

import com.persistit.Transaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;

public class PersistitTransaction extends AbstractStoreTransaction {

    private Transaction tx;
    private boolean isOpen;

    public PersistitTransaction(Transaction t, ConsistencyLevel level) throws StorageException {
        super(level);
        tx = t;
        try {
            tx.begin();
            isOpen = true;
        } catch (PersistitException ex) {
            throw new PermanentStorageException(ex.toString());
        }
    }

    @Override
    public synchronized void abort() throws StorageException {
        if (tx == null) return;
        tx.rollback();
        isOpen = false;
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
            isOpen = false;
        }
    }

    /**
     * Runs a unit of work within this transaction
     *
     * @param r: the runnable job
     * @throws StorageException
     */
    public void run(TransactionRunnable r) throws StorageException {
        if (!isOpen) {
            throw new PermanentStorageException("transaction is not open");
        }
        try {
            tx.run(r);
        } catch (PersistitException ex) {
            throw new PermanentStorageException(ex.toString());
        }
    }

    public Transaction getTx() {
        return tx;
    }
}
