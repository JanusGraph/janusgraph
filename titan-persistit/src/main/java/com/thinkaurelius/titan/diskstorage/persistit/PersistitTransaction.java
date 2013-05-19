package com.thinkaurelius.titan.diskstorage.persistit;

import static com.thinkaurelius.titan.diskstorage.persistit.PersistitStoreManager.VOLUME_NAME;

import com.persistit.*;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @todo: read this and make sure multiple threads aren't sharing transactions http://akiban.github.com/persistit/javadoc/com/persistit/Transaction.html#_threadManagement
 *
 * @todo: add finalize method
 */
public class PersistitTransaction extends AbstractStoreTransaction {

    private Persistit db;
    private SessionId sessionId;
    private boolean isOpen = false;

    private static Queue<SessionId> sessionPool = new ConcurrentLinkedQueue<SessionId>();
    private static SessionId getSessionId() {
        SessionId s = sessionPool.poll();
        if (s == null) {
            s = new SessionId();
        }
        return s;
    }
    private static void returnSessionId(SessionId s) {
        sessionPool.offer(s);
    }

    public PersistitTransaction(Persistit p, ConsistencyLevel level) throws StorageException {
        super(level);
        db = p;
        sessionId = getSessionId();
        assign();
        Transaction tx = db.getTransaction();
        assert sessionId == tx.getSessionId();

        try {
            tx.begin();
            isOpen = true;
        } catch (PersistitException ex) {
            throw new PermanentStorageException(ex);
        }
    }

    /**
     * Assigns the session id to the current thread
     */
    public void assign() {
        synchronized (this) {
            db.setSessionId(sessionId);
        }
    }

    private void close() {
        returnSessionId(sessionId);
        sessionId = null;
        isOpen = false;
    }

    @Override
    public void abort() throws StorageException {
        synchronized (this) {
            assign();
            Transaction tx = db.getTransaction();
            if (tx.isActive() && !tx.isCommitted()) {
                tx.rollback();
            }
            tx.end();
            close();
        }
    }

    @Override
    public void commit() throws StorageException {
        synchronized (this) {
            assign();
            Transaction tx = db.getTransaction();
            int retries = 3;
            try {
                if (tx.isActive() && !tx.isRollbackPending()){
                    int i = 0;
                    while (true) {
                        try {
                            tx.commit(Transaction.CommitPolicy.HARD);
                            tx.end();
                            break;
                        } catch (RollbackException ex) {
                            if (i++ >= retries) {
                                throw ex;
                            }
                        }
                    }
                    close();
                }
            } catch (PersistitException ex) {
                throw new PermanentStorageException(ex);
            }
        }
    }

    public Exchange getExchange(String treeName) throws StorageException {
        return getExchange(treeName, true);
    }

    public Exchange getExchange(String treeName, Boolean create) throws StorageException {
        synchronized (this) {
            Exchange exchange;
            try {
                assign();
                exchange = db.getExchange(VOLUME_NAME, treeName, create);
                return exchange;
            } catch (PersistitException ex) {
                throw new PermanentStorageException(ex);
            }
        }
    }

    public void releaseExchange(Exchange exchange) {
        //
    }
}
