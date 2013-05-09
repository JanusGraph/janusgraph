package com.thinkaurelius.titan.graphdb.vertices;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.vertices.querycache.ConcurrentQueryCache;
import com.thinkaurelius.titan.graphdb.vertices.querycache.QueryCache;
import com.thinkaurelius.titan.graphdb.vertices.querycache.SimpleQueryCache;
import com.thinkaurelius.titan.util.datastructures.Retriever;

import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CacheVertex extends StandardVertex {

    private SortedSet<Entry> relationCache=null;
    private QueryCache queryCache=null;

    public CacheVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    @Override
    public Iterable<Entry> loadRelations(SliceQuery query, Retriever<SliceQuery, List<Entry>> lookup) {
        if (isNew()) return ImmutableList.of();
        else {
            if (relationCache==null) {
                //Initialize datastructures
                if (tx().getConfiguration().isSingleThreaded()) {
                    relationCache = new ConcurrentSkipListSet<Entry>();
                    queryCache = new SimpleQueryCache();
                } else {
                    synchronized (this) {
                        if (relationCache==null) {
                            relationCache = new ConcurrentSkipListSet<Entry>();
                            queryCache = new ConcurrentQueryCache();
                        }
                    }
                }
            }
            if (queryCache.isCovered(query)) {
                SortedSet<Entry> results = relationCache.subSet(StaticBufferEntry.of(query.getSliceStart(), null),StaticBufferEntry.of(query.getSliceEnd(),null));
                return results;
            } else {
                List<Entry> results = lookup.get(query);
                relationCache.addAll(results);
                queryCache.add(query);
                return results;
            }
        }
    }

}
