package com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.util.stats.MetricManager;

import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class KCVSCache implements KeyColumnValueStore {

    public enum Action {
        RETRIEVAL("retrievals"), MISS("misses");

        private final String name;

        private Action(String name) {
            this.name = name;
        }

        public String getName() { return name; }
    }

    private static final String METRICS_RETRIEVAL = "retrievals";
    private static final String METRICS_MISS = "misses";

    private final String metricsName;
    private final boolean validateKeysOnly = true;


    protected final KeyColumnValueStore store;

    protected KCVSCache(KeyColumnValueStore store, String metricsName) {
        this.metricsName = metricsName;
        Preconditions.checkNotNull(store);
        this.store = store;
    }

    protected boolean hasValidateKeysOnly() {
        return validateKeysOnly;
    }

    protected void incActionBy(int by, Action action, StoreTransaction txh) {
        assert by>=1;
        if (metricsName!=null) MetricManager.INSTANCE.getCounter(txh.getConfiguration().getMetricsPrefix(), metricsName, action.getName()).inc(by);
    }

    public abstract void clearCache();

    protected abstract void invalidate(StaticBuffer key, List<CachableStaticBuffer> entries);

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws StorageException {
        assert txh instanceof ExpirationTransaction;
        ((ExpirationTransaction) txh).expireMutations(this, key, additions, deletions);
        store.mutate(key, additions, deletions, getTx(txh));
    }


    //############### SIMPLE PROXY ###########

    private final StoreTransaction getTx(StoreTransaction txh) {
        assert txh instanceof ExpirationTransaction;
        return ((ExpirationTransaction) txh).getWrappedTransactionHandle();
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        return store.containsKey(key, getTx(txh));
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        store.acquireLock(key, column, expectedValue, getTx(txh));
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery keyQuery, StoreTransaction txh) throws StorageException {
        return store.getKeys(keyQuery, getTx(txh));
    }

    @Override
    public KeyIterator getKeys(SliceQuery columnQuery, StoreTransaction txh) throws StorageException {
        return store.getKeys(columnQuery, getTx(txh));
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws StorageException {
        return store.getLocalKeyPartition();
    }

    @Override
    public String getName() {
        return store.getName();
    }

    @Override
    public void close() throws StorageException {
        store.close();
    }

}
