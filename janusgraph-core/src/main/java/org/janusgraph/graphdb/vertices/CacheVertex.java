// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.vertices;

import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.util.datastructures.Retriever;

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
    //How many times refresh() has cleared the cache, guarded by it: a load which a refresh overlaps does not store what
    //it loaded before the refresh, since that may predate the change the refresh was for. loadRelations reads the count
    //before it loads; a caller which loads before it calls, as a multi-query does, reads it through refreshes() first
    private long refreshes;

    public CacheVertex(StandardJanusGraphTx tx, Object id, byte lifecycle) {
        super(tx, id, lifecycle);
        queryCache = new HashMap<>(4);
    }

    public void refresh() {
        synchronized (queryCache) {
            refreshes++;
            queryCache.clear();
        }
    }

    /**
     * How many times {@link #refresh()} has run so far. A caller which loads relations before it hands them to
     * {@link #loadRelations(SliceQuery, Retriever, long)} reads this before it loads, so that a refresh which overlaps
     * the load keeps its result out of the cache.
     */
    public long refreshes() {
        synchronized (queryCache) {
            return refreshes;
        }
    }

    public EntryList getFromCache(final SliceQuery query) {
        synchronized (queryCache) {
            return queryCache.get(query);
        }
    }

    public void addToQueryCache(final SliceQuery query, final EntryList entries) {
        synchronized (queryCache) {
            //TODO: become smarter about what to cache and when (e.g. memory pressure)
            queryCache.put(query, entries);
        }
    }

    //Stores what a load read unless refresh() has run since the load read the count: the result may predate the
    //change the refresh was for, and the next load reads it again
    private void addToQueryCache(final SliceQuery query, final EntryList entries, final long refreshesBefore) {
        synchronized (queryCache) {
            if (refreshes == refreshesBefore) queryCache.put(query, entries);
        }
    }

    protected int getQueryCacheSize() {
        synchronized (queryCache) {
            return queryCache.size();
        }
    }

    @Override
    public EntryList loadRelations(final SliceQuery query, final Retriever<SliceQuery, EntryList> lookup) {
        return loadRelations(query, lookup, refreshes());
    }

    /**
     * As {@link #loadRelations(SliceQuery, Retriever)}, for a result the retriever has loaded before this call:
     * {@code refreshesBefore} is what {@link #refreshes()} returned before that load, and the result is kept in the
     * cache only if no refresh has come since.
     */
    public EntryList loadRelations(final SliceQuery query, final Retriever<SliceQuery, EntryList> lookup,
                                   final long refreshesBefore) {
        if (isNew())
            return EntryList.EMPTY_LIST;

        EntryList result;
        synchronized (queryCache) {
            result = queryCache.get(query);
        }
        return result != null ? result : load(query, lookup, refreshesBefore);
    }

    private EntryList load(final SliceQuery query, final Retriever<SliceQuery, EntryList> lookup,
                           final long refreshesBefore) {
        //First check for super
        final Map.Entry<SliceQuery, EntryList> superset = getSuperResultSet(query);
        final EntryList result = superset == null || superset.getValue() == null
            ? lookup.get(query) : query.getSubset(superset.getKey(), superset.getValue());
        addToQueryCache(query, result, refreshesBefore);
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
