package com.thinkaurelius.titan.graphdb.database.cache;

import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;

import java.util.List;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class PassThroughStoreCache implements StoreCache {

    @Override
    public EntryList query(KeySliceQuery query, BackendTransaction tx) {
        return tx.edgeStoreQuery(query);
    }

    @Override
    public Map<StaticBuffer,EntryList> multiQuery(List<StaticBuffer> keys, SliceQuery query, BackendTransaction tx) {
        return tx.edgeStoreMultiQuery(keys,query);
    }

    @Override
    public void invalidate(StaticBuffer key) {
        //Do nothing
    }

    @Override
    public void close() {
        //Nothing to clean up
    }

}
