package com.thinkaurelius.titan.diskstorage.persistit;

import com.persistit.Persistit;
import com.persistit.TransactionRunnable;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;

import com.persistit.Transaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;

/**
 * @todo: read this and make sure multiple threads aren't sharing transactions http://akiban.github.com/persistit/javadoc/com/persistit/Transaction.html#_threadManagement
 */
public class PersistitTransaction extends AbstractStoreTransaction {

    private Persistit db;
    private Transaction tx;
    private boolean isOpen;

    public PersistitTransaction(Persistit p, Transaction t, ConsistencyLevel level) throws StorageException {
        super(level);
        db = p;
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
        db.setSessionId(tx.getSessionId());
        tx.rollback();
        isOpen = false;
    }

    @Override
    public synchronized void commit() throws StorageException {
        if (tx == null) return;
        db.setSessionId(tx.getSessionId());
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
        db.setSessionId(tx.getSessionId());
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
