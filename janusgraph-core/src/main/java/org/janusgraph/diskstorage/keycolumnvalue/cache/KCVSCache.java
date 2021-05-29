// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.keycolumnvalue.cache;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVSProxy;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.CacheMetricsAction;
import org.janusgraph.util.stats.MetricManager;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class KCVSCache extends KCVSProxy {

    public static final List<Entry> NO_DELETIONS = Collections.emptyList();

    private final String metricsName;

    protected KCVSCache(KeyColumnValueStore store, String metricsName) {
        super(store);
        this.metricsName = metricsName;
    }

    protected boolean hasValidateKeysOnly() {
        return true;
    }

    protected void incActionBy(int by, CacheMetricsAction action, StoreTransaction txh) {
        assert by>=1;
        if (metricsName!=null && txh.getConfiguration().hasGroupName()) {
            MetricManager.INSTANCE.getCounter(txh.getConfiguration().getGroupName(), metricsName, action.getName()).inc(by);
        }
    }

    public abstract void clearCache();

    protected abstract void invalidate(StaticBuffer key, List<CachableStaticBuffer> entries);

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("Only supports mutateEntries()");
    }

    public void mutateEntries(StaticBuffer key, List<Entry> additions, List<Entry> deletions, StoreTransaction txh) throws BackendException {
        assert txh instanceof CacheTransaction;
        ((CacheTransaction) txh).mutate(this, key, additions, deletions);
    }

    @Override
    protected final StoreTransaction unwrapTx(StoreTransaction txh) {
        assert txh instanceof CacheTransaction;
        return ((CacheTransaction) txh).getWrappedTransaction();
    }

    public EntryList getSliceNoCache(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        return store.getSlice(query,unwrapTx(txh));
    }

    public Map<StaticBuffer, EntryList> getSliceNoCache(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        return store.getSlice(keys,query,unwrapTx(txh));
    }

}
