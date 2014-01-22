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
public interface StoreCache {

    public EntryList query(KeySliceQuery query, BackendTransaction tx);

    public Map<StaticBuffer,EntryList> multiQuery(List<StaticBuffer> keys, SliceQuery query, BackendTransaction tx);

    public void invalidate(StaticBuffer key);

    public void close();


}
