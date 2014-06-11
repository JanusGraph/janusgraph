package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.google.common.base.Preconditions;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BerkeleyJETx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(BerkeleyJETx.class);

    private volatile Transaction tx;
    private final List<Cursor> openCursors = new ArrayList<Cursor>();
    private final LockMode lm;

    public BerkeleyJETx(Transaction t, LockMode lockMode, BaseTransactionConfig config) {
        super(config);
        tx = t;
        lm = lockMode;
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(lm);
    }

    public Transaction getTransaction() {
        return tx;
    }

    void registerCursor(Cursor cursor) {
        Preconditions.checkArgument(cursor != null);
        synchronized (openCursors) {
            //TODO: attempt to remove closed cursors if there are too many
            openCursors.add(cursor);
        }
    }

    private void closeOpenIterators() throws StorageException {
        for (Cursor cursor : openCursors) {
            cursor.close();
        }
    }

    LockMode getLockMode() {
        return lm;
    }

    @Override
    public synchronized void rollback() throws StorageException {
        super.rollback();
        if (tx == null) return;
        if (log.isTraceEnabled())
            log.trace("{} rolled back", this.toString(), new TransactionClose(this.toString()));
        try {
            closeOpenIterators();
            tx.abort();
            tx = null;
        } catch (DatabaseException e) {
            throw new PermanentStorageException(e);
        }
    }

    @Override
    public synchronized void commit() throws StorageException {
        super.commit();
        if (tx == null) return;
        if (log.isTraceEnabled())
            log.trace("{} committed", this.toString(), new TransactionClose(this.toString()));

        try {
            closeOpenIterators();
            tx.commit();
            tx = null;
        } catch (DatabaseException e) {
            throw new PermanentStorageException(e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + tx.toString();
    }

    private static class TransactionClose extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionClose(String msg) {
            super(msg);
        }
    }
}
