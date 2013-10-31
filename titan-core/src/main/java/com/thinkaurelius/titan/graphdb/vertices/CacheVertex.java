package com.thinkaurelius.titan.graphdb.vertices;

import java.util.*;
import java.util.concurrent.Callable;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.datastructures.Retriever;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CacheVertex extends StandardVertex {
    // We don't try to be smart and match with previous queries
    // because that would waste more cycles on lookup than save actual memory
    private final Cache<SliceQuery, List<Entry>> queryCache;

    public CacheVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
        // TODO: add MemoryMeter agent to Titan to use memory measurement instead of size
        queryCache = CacheBuilder.newBuilder()
                .concurrencyLevel(1)
                .maximumSize(32)
                .build();
    }

    @Override
    public Collection<Entry> loadRelations(final SliceQuery query, final Retriever<SliceQuery, List<Entry>> lookup) {
        if (isNew())
            return Collections.EMPTY_SET;

        try {
            return queryCache.get(query, new Callable<List<Entry>>() {
                @Override
                public List<Entry> call() throws Exception {
                    Map.Entry<SliceQuery, List<Entry>> superset = getSuperResultSet(query);
                    if (superset == null) {
                        return lookup.get(query);
                    } else {
                        return query.getSubset(superset.getKey(), superset.getValue());
                    }
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasLoadedRelations(final SliceQuery query) {
        return queryCache.getIfPresent(query) != null || getSuperResultSet(query) != null;
    }

    private Map.Entry<SliceQuery, List<Entry>> getSuperResultSet(final SliceQuery query) {
        if (queryCache.size() > 0) {
            for (Map.Entry<SliceQuery, List<Entry>> entry : queryCache.asMap().entrySet()) {
                if (entry.getKey().subsumes(query)) return entry;
            }
        }
        return null;
    }

}
