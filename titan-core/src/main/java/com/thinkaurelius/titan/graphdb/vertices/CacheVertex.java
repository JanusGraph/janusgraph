package com.thinkaurelius.titan.graphdb.vertices;

import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.vertices.querycache.ConcurrentQueryCache;
import com.thinkaurelius.titan.graphdb.vertices.querycache.QueryCache;
import com.thinkaurelius.titan.util.datastructures.Retriever;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CacheVertex extends StandardVertex {

    /**
     * Holder object for two concurrent collections.
     * 
     * We could forego the holder and make {@link State#queryCache} and
     * {@link State#relationCache} fields on this class. However, when we do
     * that one of the two fields must then be marked volatile for use in the
     * double-checked locking pattern below. Using the nonvolatile field in DCL
     * would be a thread safety failure. I think the holder idiom is slightly
     * easier to use, so I'm defaulting to it unless this proves to be a memory
     * or cpu problem.
     */
    private volatile State state;

    public CacheVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    @Override
    public Iterable<Entry> loadRelations(SliceQuery query, Retriever<SliceQuery, List<Entry>> lookup) {
        if (isNew()) return ImmutableList.of();
        else {
            if (null == state) {
                //Initialize datastructures
                if (tx().getConfiguration().isSingleThreaded()) {
                    state = new State();
                } else {
                    synchronized (this) {
                        if (null == state) {
                            state = new State();
                        }
                    }
                }
            }
            if (state.queryCache.isCovered(query)) {
                SortedSet<Entry> results = state.relationCache.subSet(StaticBufferEntry.of(query.getSliceStart(), null),StaticBufferEntry.of(query.getSliceEnd(),null));
                return results;
            } else {
                List<Entry> results = lookup.get(query);
                state.relationCache.addAll(results);
                state.queryCache.add(query);
                return results;
            }
        }
    }

    private static class State {
        private SortedSet<Entry> relationCache;
        private QueryCache queryCache;
        
        private State() {
            relationCache = new ConcurrentSkipListSet<Entry>();
            queryCache = new ConcurrentQueryCache();
        }
    }
}
