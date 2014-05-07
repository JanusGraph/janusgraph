package com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache;

import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.List;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class NoKCVSCache extends KCVSCache {


    public NoKCVSCache(KeyColumnValueStore store) {
        super(store, null);
    }

    @Override
    public void clearCache() {
    }

    @Override
    protected void invalidate(StaticBuffer key, List<CachableStaticBuffer> entries) {
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        return store.getSlice(query,getTx(txh));
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
        return store.getSlice(keys,query,getTx(txh));
    }
}
