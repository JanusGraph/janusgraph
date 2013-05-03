package com.thinkaurelius.titan.graphdb.vertices.querycache;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ConcurrentQueryCache extends SimpleQueryCache implements QueryCache {

    @Override
    public synchronized boolean isCovered(SliceQuery query) {
        return super.isCovered(query);
    }

    @Override
    public synchronized boolean add(SliceQuery query) {
        return super.add(query);
    }
}
