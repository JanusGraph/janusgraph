package com.thinkaurelius.titan.graphdb.vertices;

import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.datastructures.Retriever;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CacheVertex extends StandardVertex {
    // We don't try to be smart and match with previous queries
    // because that would waste more cycles on lookup than save actual memory
    // We use a normal map with synchronization since the likelihood of contention
    // is super low in a single transaction
    protected final Map<SliceQuery, EntryList> queryCache;

    public CacheVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
        queryCache = new HashMap<SliceQuery, EntryList>(4);
    }

    protected void addToQueryCache(final SliceQuery query, final EntryList entries) {
        synchronized (queryCache) {
            //TODO: become smarter about what to cache and when (e.g. memory pressure)
            queryCache.put(query, entries);
        }
    }

    protected int getQueryCacheSize() {
        synchronized (queryCache) {
            return queryCache.size();
        }
    }

    @Override
    public EntryList loadRelations(final SliceQuery query, final Retriever<SliceQuery, EntryList> lookup) {
        if (isNew())
            return EntryList.EMPTY_LIST;

        EntryList result;
        synchronized (queryCache) {
            result = queryCache.get(query);
        }
        if (result == null) {
            //First check for super
            Map.Entry<SliceQuery, EntryList> superset = getSuperResultSet(query);
            if (superset == null) {
                result = lookup.get(query);
            } else {
                result = query.getSubset(superset.getKey(), superset.getValue());
            }
            addToQueryCache(query, result);

        }
        return result;
    }

    @Override
    public boolean hasLoadedRelations(final SliceQuery query) {
        synchronized (queryCache) {
            return queryCache.get(query) != null || getSuperResultSet(query) != null;
        }
    }

    private Map.Entry<SliceQuery, EntryList> getSuperResultSet(final SliceQuery query) {

        synchronized (queryCache) {
            if (queryCache.size() > 0) {
                for (Map.Entry<SliceQuery, EntryList> entry : queryCache.entrySet()) {
                    if (entry.getKey().subsumes(query)) return entry;
                }
            }
        }
        return null;
    }

}
