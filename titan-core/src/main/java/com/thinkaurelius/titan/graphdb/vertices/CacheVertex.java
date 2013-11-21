package com.thinkaurelius.titan.graphdb.vertices;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.datastructures.Retriever;

import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CacheVertex extends StandardVertex {
    // We don't try to be smart and match with previous queries
    // because that would waste more cycles on lookup than save actual memory
    // We use a normal map with synchronization since the likelihood of contention
    // is super low in a single transaction
    private final Map<SliceQuery,List<Entry>> queryCache;

    public CacheVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
        queryCache = new HashMap<SliceQuery,List<Entry>>(4);
    }

    @Override
    public Collection<Entry> loadRelations(final SliceQuery query, final Retriever<SliceQuery, List<Entry>> lookup) {
        if (isNew())
            return Collections.EMPTY_SET;

        List<Entry> result;
        synchronized (queryCache) {
            result = queryCache.get(query);
        }
        if (result==null) {
            //First check for super
            Map.Entry<SliceQuery, List<Entry>> superset = getSuperResultSet(query);
            if (superset==null) {
                result = lookup.get(query);
            } else {
                result = query.getSubset(superset.getKey(), superset.getValue());
            }
            synchronized (queryCache) {
                //TODO: become smarter about what to cache and when (e.g. memory pressure)
                queryCache.put(query,result);
            }
        }
        return result;
    }

    @Override
    public boolean hasLoadedRelations(final SliceQuery query) {
        synchronized (queryCache) {
            return queryCache.get(query) != null || getSuperResultSet(query) != null;
        }
    }

    private Map.Entry<SliceQuery, List<Entry>> getSuperResultSet(final SliceQuery query) {
        if (queryCache.size() > 0) {
            synchronized (queryCache) {
                for (Map.Entry<SliceQuery, List<Entry>> entry : queryCache.entrySet()) {
                    if (entry.getKey().subsumes(query)) return entry;
                }
            }
        }
        return null;
    }

}
