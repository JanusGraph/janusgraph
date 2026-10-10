// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.graphdb.transaction.subquerycache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;

/**
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
public class CaffeineSubqueryCache extends SubsetSubqueryCache {
    //An entry weighs the size of its list of results plus this
    private static final int ENTRY_OVERHEAD = 2;

    private final Cache<JointIndexQuery.Subquery, SubqueryResult> caffeineCache;

    private final int maximumCachedResultSize;

    public CaffeineSubqueryCache(long maximumWeight) {
        caffeineCache = Caffeine.newBuilder()
            .weigher((Weigher<JointIndexQuery.Subquery, SubqueryResult>) (q, r) -> ENTRY_OVERHEAD + r.size())
            .maximumWeight(maximumWeight)
            //Maintained on the thread which uses it, as the vertex cache of a transaction is, rather than in a task of
            //the common pool after every change
            .executor(Runnable::run)
            .build();
        //Caffeine drops an entry heavier than the maximum weight when it maintains the cache, and may keep one which
        //weighs no more
        maximumCachedResultSize = (int) Math.min(Integer.MAX_VALUE, maximumWeight - ENTRY_OVERHEAD);
    }

    @Override
    public int maximumCachedResultSize() {
        return maximumCachedResultSize;
    }

    @Override
    protected SubqueryResult get(JointIndexQuery.Subquery key) {
        return caffeineCache.getIfPresent(key);
    }

    @Override
    protected void put(JointIndexQuery.Subquery key, SubqueryResult result) {
        caffeineCache.put(key, result);
    }

    @Override
    public synchronized void close() {
        caffeineCache.invalidateAll();
        caffeineCache.cleanUp();
    }
}
