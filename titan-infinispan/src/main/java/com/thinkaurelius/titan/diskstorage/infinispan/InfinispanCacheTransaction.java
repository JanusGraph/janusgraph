package com.thinkaurelius.titan.diskstorage.infinispan;

import java.util.concurrent.atomic.AtomicReference;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;

/**
 * Titan's diskstorage API requires transactions at the backend level, whereas
 * Infinispan's API encourages the use of transactions at the cache level.
 */
public class InfinispanCacheTransaction extends AbstractStoreTransaction {

    private final AtomicReference<State> state;
    
    private static final Logger log = LoggerFactory.getLogger(InfinispanCacheTransaction.class);
    
    public InfinispanCacheTransaction(StoreTxConfig config) {
        super(config);
        state = new AtomicReference<State>();
    }
    
    public void init(Cache<?,?> c) throws StorageException {
        assert null != c;
        State current = state.get();
        if (null != current)
            return;
        
        State s = makeStateForCache(c);
        
        if (!state.compareAndSet(null, s)) {
            Preconditions.checkArgument(s.m == c.getAdvancedCache().getTransactionManager());
            // Cleanup unused transaction
            try {
                TransactionManager tm = c.getAdvancedCache().getTransactionManager();
                tm.resume(s.t);
                tm.rollback();
            } catch (IllegalStateException e) {
                throw new PermanentStorageException(e);
            } catch (SystemException e) {
                throw new TemporaryStorageException(e);
            } catch (InvalidTransactionException e) {
                throw new TemporaryStorageException(e);
            }
        }
//        boolean wasNull = state.compareAndSet(null, s);
//        
//        if (wasNull)
//            return;
//        
//        State actual = state.get();
//        assert null != actual;
//        assert null != actual.c;
//        return c.equals(actual.c);
    }
    
    private State makeStateForCache(Cache<?,?> c) throws StorageException {
        try {
            TransactionManager tm = c.getAdvancedCache().getTransactionManager();
            tm.begin();
            Transaction t = tm.suspend();
            log.trace("Begin    {}", t);
            return new State(c, t);
        } catch (NotSupportedException e) {
            throw new PermanentStorageException(e);
        } catch (SystemException e) {
            throw new TemporaryStorageException(e);
        }
    }
    
    @SuppressWarnings("unchecked")
    public <K,V> Cache<K,V> getCache() {
        return (Cache<K,V>)state.get().c;
    }
    
    @Override
    public void commit() throws StorageException {
        State s = state.get();
        if (null == s)
            return;
        
        try {
            s.m.resume(s.t);
            log.trace("Commit   {}", s.m);
            s.m.commit();
        } catch (SecurityException e) {
            throw new PermanentStorageException(e);
        } catch (IllegalStateException e) {
            throw new TemporaryStorageException(e);
        } catch (RollbackException e) {
            throw new TemporaryStorageException(e);
        } catch (HeuristicMixedException e) {
            throw new TemporaryStorageException(e);
        } catch (HeuristicRollbackException e) {
            throw new TemporaryStorageException(e);
        } catch (SystemException e) {
            throw new TemporaryStorageException(e);
        } catch (InvalidTransactionException e) {
            throw new PermanentStorageException(e);
        }
    }
    
    @Override
    public void rollback() throws StorageException {
        State s = state.get();
        if (null == s)
            return;
        
        try {
            s.m.resume(s.t);
            log.trace("Rollback {}", s.m);
            s.m.rollback();
        } catch (IllegalStateException e) {
            throw new TemporaryStorageException(e);
        } catch (SecurityException e) {
            throw new PermanentStorageException(e);
        } catch (SystemException e) {
            throw new TemporaryStorageException(e);
        } catch (InvalidTransactionException e) {
            throw new PermanentStorageException(e);
        }
    }
    
    @Override
    public void flush() throws StorageException {
        // TODO ?
        log.debug("Flush is not meaningful under Infinispan");
    }
    
    public void resume() throws StorageException {
        State s = state.get();
        if (null == s)
            return;
        
        try {
            s.m.resume(s.t);
        } catch (InvalidTransactionException e) {
            throw new PermanentStorageException(e);
        } catch (IllegalStateException e) {
            throw new TemporaryStorageException(e);
        } catch (SystemException e) {
            throw new TemporaryStorageException(e);
        }
    }
    
    public void suspend() throws StorageException {
        State s = state.get();
        if (null == s)
            return;
        
        // In Arjuna local, this only mutates a threadlocal and not s.t
        // This assumption may not hold for Arjuna XA or, say, Atomikos
        // Worst case: we would have to serialize all actions within a particular tx
        try {
            s.m.suspend();
        } catch (SystemException e) {
            throw new TemporaryStorageException(e);
        }
    }
    
    private static class State {
        private final Cache<?,?> c;
        private final TransactionManager m;
        private final Transaction t;
        
        public State(Cache<?,?> c, Transaction t) {
            this.c = c;
            this.m = c.getAdvancedCache().getTransactionManager();
            this.t = t;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((c == null) ? 0 : c.hashCode());
            result = prime * result + ((t == null) ? 0 : t.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            State other = (State) obj;
            if (c == null) {
                if (other.c != null)
                    return false;
            } else if (!c.equals(other.c))
                return false;
            if (t == null) {
                if (other.t != null)
                    return false;
            } else if (!t.equals(other.t))
                return false;
            return true;
        }
    }
}
