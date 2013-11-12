package com.thinkaurelius.titan.diskstorage.infinispan;

import java.io.IOException;
import java.util.concurrent.Callable;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.apache.commons.configuration.BaseConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.NoOpStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.CacheStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;

public class InfinispanCacheTransactionalStore extends InfinispanCacheStore {
    
    private static final Logger log = LoggerFactory.getLogger(InfinispanCacheTransactionalStore.class);

    private final NoOpStoreTransaction noopTx = new NoOpStoreTransaction(new StoreTxConfig()); // TODO pass through actual tx config?  i think this just affects metrics
    
    public InfinispanCacheTransactionalStore(String fullName, String shortName, EmbeddedCacheManager manager) {
        super(fullName, shortName, manager);
    }
    
    @Override
    public void delete(StaticBuffer key, StoreTransaction txh)
            throws StorageException {
        InfinispanCacheTransaction ict = (InfinispanCacheTransaction)txh;
        ict.init(cache);
//        if (!sameOrNullCache) {
//            log.error("Infinispan transaction spans multiple caches ({} and {})", cache, ict.getCache());
//        }
        try {
            ict.resume();
            super.delete(key, noopTx);
        } finally {
            ict.suspend();
        }
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh)
            throws StorageException {
        InfinispanCacheTransaction ict = (InfinispanCacheTransaction)txh;
        ict.init(cache);
//        if (!sameOrNullCache) {
//            log.error("Infinispan transaction spans multiple caches ({} and {})", cache, ict.getCache());
//        }
        try {
            ict.resume();
            return super.get(key, noopTx);
        } finally {
            ict.suspend();
        }
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh)
            throws StorageException {
        InfinispanCacheTransaction ict = (InfinispanCacheTransaction)txh;
        ict.init(cache);
//        if (!sameOrNullCache) {
//            log.error("Infinispan transaction spans multiple caches ({} and {})", cache, ict.getCache());
//        }
        try {
            ict.resume();
            return super.containsKey(key, noopTx);
        } finally {
            ict.suspend();
        }
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue,
            StoreTransaction txh) throws StorageException { // TODO implement with native locking
        InfinispanCacheTransaction ict = (InfinispanCacheTransaction)txh;
        ict.init(cache);
//        if (!sameOrNullCache) {
//            log.error("Infinispan transaction spans multiple caches ({} and {})", cache, ict.getCache());
//        }
        try {
            ict.resume();
            super.acquireLock(key, expectedValue, noopTx);
        } finally {
            ict.suspend();
        }
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        return null; // TODO
    }

    @Override
    public String getName() {
        return super.getName();
    }

    @Override
    public void close() throws StorageException {
        super.close();
    }

    @Override
    public void replace(StaticBuffer key, StaticBuffer newValue,
            StaticBuffer oldValue, StoreTransaction txh)
            throws StorageException {
        InfinispanCacheTransaction ict = (InfinispanCacheTransaction)txh;
        ict.init(cache);
//        if (!sameOrNullCache) {
//            log.error("Infinispan transaction spans multiple caches ({} and {})", cache, ict.getCache());
//        }
        try {
            ict.resume();
            super.replace(key, newValue, oldValue, noopTx);
        } finally {
            ict.suspend();
        }
    }

    @Override
    public RecordIterator<KeyValueEntry> getKeys(KeySelector selector,
            StoreTransaction txh) throws StorageException {
        InfinispanCacheTransaction ict = (InfinispanCacheTransaction)txh;
        ict.init(cache);
//        if (!sameOrNullCache) {
//            log.error("Infinispan transaction spans multiple caches ({} and {})", cache, ict.getCache());
//        }
        try {
            ict.resume();
            /*
             * Iterator returned by super.getKeys(...) does lazy cache.get(...)
             * calls when next() is invoked. We must wrap it with our
             * transaction context, else these iterators calls will essentially
             * ignore the transactional context. The behavior observed in this
             * case is correct keys but nonsensical values, and this in turn is
             * likely to break the "null != value" assertions in KeyValueEntry.
             */
            return new ISTxKeyIter(super.getKeys(selector, noopTx), ict);
        } finally {
            ict.suspend();
        }
    }

    @Override
    public void clearStore() {
        TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
        try {
            tm.begin();
            super.clearStore();
            tm.commit();
        } catch (NotSupportedException e) {
            throw new RuntimeException(e);
        } catch (SystemException e) {
            throw new RuntimeException(e);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } catch (RollbackException e) {
            throw new RuntimeException(e);
        } catch (HeuristicMixedException e) {
            throw new RuntimeException(e);
        } catch (HeuristicRollbackException e) {
            throw new RuntimeException(e);
        }
    }

//    private <T> T runInTx(Callable<T> f) {
//        InfinispanCacheTransaction ict = (InfinispanCacheTransaction)txh;
//        boolean sameOrNullCache = ict.init(cache);
//        if (!sameOrNullCache) {
//            log.error("Infinispan transaction spans multiple caches ({} and {})", cache, ict.getCache());
//        }
//        try {
//            ict.resume();
//            return f.call();
//        } finally {
//            ict.suspend();
//        }
//    }
    
    private static class ISTxKeyIter implements RecordIterator<KeyValueEntry> {
        
        private final RecordIterator<KeyValueEntry> underlying;
        private final InfinispanCacheTransaction cacheTx;
        
        public ISTxKeyIter(RecordIterator<KeyValueEntry> underlying, InfinispanCacheTransaction cacheTx) {
            this.underlying = underlying;
            this.cacheTx = cacheTx;
        }

        @Override
        public boolean hasNext() {
            /*
             * Underlying impl iterates over a collection eagerly retrieved
             * during getKeys(...) invocation. No need to wrap it with a
             * transactional context.
             */
            return underlying.hasNext();
        }

        @Override
        public KeyValueEntry next() {
            // Underlying does an ISPN cache.get(...), must wrap with tx
            try {
                cacheTx.resume();
                return underlying.next();
            } catch (StorageException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    cacheTx.suspend();
                } catch (StorageException e) {
                    log.warn("Failed to suspend Infinispan transaction {}", cacheTx, e);
                }
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            // Do nothing
        }
    }
}
