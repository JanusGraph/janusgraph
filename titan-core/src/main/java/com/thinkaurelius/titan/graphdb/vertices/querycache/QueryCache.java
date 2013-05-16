package com.thinkaurelius.titan.graphdb.vertices.querycache;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface QueryCache {

    public boolean isCovered(SliceQuery query);

    public boolean add(SliceQuery query);

}
