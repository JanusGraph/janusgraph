package com.thinkaurelius.titan.diskstorage.persistit;

import com.persistit.*;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * @todo: read this and make sure multiple threads aren't sharing transactions http://akiban.github.com/persistit/javadoc/com/persistit/Transaction.html#_threadManagement
 *
 * @todo: add finalize method
 */
public class PersistitTransaction extends AbstractStoreTransaction {

    /**
     * Temporary hack to get around the private session id constructor
     * @return
     */
    private static SessionId getSessionIdHack() {
        Constructor<SessionId> constructor = null;
        try {
            constructor = SessionId.class.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
        constructor.setAccessible(true);
        try {
            return constructor.newInstance();
        } catch (InstantiationException e) {
            throw new AssertionError(e);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        } catch (InvocationTargetException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Temporary hack to get around the private transaction constructor
     * @return
     */
    private static Transaction getTransactionHack(Persistit db, SessionId sid) {
        Constructor constructor = Transaction.class.getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        try {
            return (Transaction) constructor.newInstance(db, sid);
        } catch (InstantiationException e) {
            throw new AssertionError(e);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        } catch (InvocationTargetException e) {
            throw new AssertionError(e);
        }
    }

    private Persistit db;
    private Transaction tx;
    private SessionId sessionId;


    public PersistitTransaction(Persistit p, ConsistencyLevel level) throws StorageException {
        super(level);
        db = p;
        sessionId = getSessionIdHack();
        tx = getTransactionHack(db, sessionId);

        try {
            tx.begin();
        } catch (PersistitException ex) {
            throw new PermanentStorageException(ex);
        }
    }

    private void begin() throws StorageException {
        if (!tx.isActive()) {
            try {
                tx.begin();
            } catch (PersistitException ex) {
                throw new PermanentStorageException(ex);
            }
        }
        db.setSessionId(tx.getSessionId());
    }

    @Override
    public synchronized void abort() throws StorageException {
        if (!tx.isActive()) return;
        begin();

        tx.rollback();
        tx.end();
    }

    @Override
    public synchronized void commit() throws StorageException {
        if (!tx.isActive())
            return;
        begin();

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
            throw new PermanentStorageException(ex);
        } finally {
            tx.end();
        }
    }

    /**
     * Runs a unit of work within this transaction
     *
     * @param r: the runnable job
     * @throws StorageException
     */
    public void run(PersistitKeyValueStore.PersistitJob r) throws StorageException {
        begin();

        try {
            tx.run(r);
        } catch (PersistitException ex) {
            throw new PermanentStorageException(ex.toString());
        }
    }

    public Exchange getExchange(String treeName) throws StorageException {
        return getExchange(treeName, true);
    }

    public Exchange getExchange(String treeName, Boolean create) throws StorageException {
        try {
            db.setSessionId(sessionId);
            return db.getExchange(db.getSystemVolume().getName(), treeName, create);
        } catch (PersistitException ex) {
            throw new PermanentStorageException(ex);
        }
    }

    public void releaseExchange(Exchange exchange) {
        db.releaseExchange(exchange);
    }

    public Transaction getTx() {
        return tx;
    }
}
