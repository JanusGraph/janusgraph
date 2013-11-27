package com.thinkaurelius.titan.graphdb.database.cache;

import com.carrotsearch.hppc.LongArrayList;
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;

import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface StoreCache {

    public List<Entry> query(KeySliceQuery query, BackendTransaction tx);

    public List<List<Entry>> multiQuery(List<StaticBuffer> keys, SliceQuery query, BackendTransaction tx);

    public void invalidate(StaticBuffer key);

    public void close();


}
