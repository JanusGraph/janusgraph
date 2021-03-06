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

    public CacheVertex(StandardJanusGraphTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
        queryCache = new HashMap<>(4);
    }

    public void refresh() {
        synchronized (queryCache) {
            queryCache.clear();
        }
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
            if (superset == null || superset.getValue() == null) {
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
