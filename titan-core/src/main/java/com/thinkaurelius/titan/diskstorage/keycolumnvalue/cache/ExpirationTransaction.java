package com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ExpirationTransaction implements StoreTransaction {

    private static final Logger log =
            LoggerFactory.getLogger(ExpirationTransaction.class);

    private final StoreTransaction tx;
    private final boolean validateKeysOnly;
    private final List<InvalidationEntry> invalidations;


    public ExpirationTransaction(StoreTransaction tx) {
        Preconditions.checkNotNull(tx);
        this.tx = tx;
        this.validateKeysOnly = true;
        this.invalidations = new ArrayList<InvalidationEntry>();
    }

    public StoreTransaction getWrappedTransactionHandle() {
        return tx;
    }

    void expireMutations(KCVSCache cache, StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions) {
        Preconditions.checkNotNull(cache);
        InvalidationEntry invalidation;
        if (validateKeysOnly) invalidation = new InvalidationEntry(cache,key);
        else invalidation = new InvalidationEntry(cache,key,additions,deletions);
        invalidations.add(invalidation);
    }

    @Override
    public void flush() throws StorageException {
        flushInternal();
        tx.flush();
    }

    private void flushInternal() throws StorageException {
        if (!invalidations.isEmpty()) {
            for (InvalidationEntry invalidation : invalidations) {
                invalidation.execute();
            }
            clear();
        }
    }

    private void clear() {
        invalidations.clear();
    }

    @Override
    public void commit() throws StorageException {
        flushInternal();
        tx.commit();
    }

    @Override
    public void rollback() throws StorageException {
        clear();
        tx.rollback();
    }

    @Override
    public StoreTxConfig getConfiguration() {
        return tx.getConfiguration();
    }

    private static class InvalidationEntry {

        private static final List<CachableStaticBuffer> NO_ENTRIES = ImmutableList.of();

        private final KCVSCache cache;
        private final StaticBuffer key;
        private final List<CachableStaticBuffer> entries;

        private InvalidationEntry(KCVSCache cache, StaticBuffer key) {
            this.cache = cache;
            this.key = key;
            this.entries = NO_ENTRIES;
        }

        private InvalidationEntry(KCVSCache cache, StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions) {
            this.cache = cache;
            this.key = key;
            this.entries = new ArrayList<CachableStaticBuffer>(additions.size()+deletions.size());
            for (Entry e : additions) {
                assert e instanceof CachableStaticBuffer;
                entries.add((CachableStaticBuffer)e);
            }
            for (StaticBuffer e : deletions) {
                assert e instanceof CachableStaticBuffer;
                entries.add((CachableStaticBuffer)e);
            }
        }

        void execute() {
            cache.invalidate(key,entries);
        }
    }

}
