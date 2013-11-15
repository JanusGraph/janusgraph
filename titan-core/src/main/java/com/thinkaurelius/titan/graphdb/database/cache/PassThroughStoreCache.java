package com.thinkaurelius.titan.graphdb.database.cache;

import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;

import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class PassThroughStoreCache implements StoreCache {

    @Override
    public List<Entry> query(KeySliceQuery query, BackendTransaction tx) {
        return tx.edgeStoreQuery(query);
    }

    @Override
    public List<List<Entry>> multiQuery(List<StaticBuffer> keys, SliceQuery query, BackendTransaction tx) {
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
