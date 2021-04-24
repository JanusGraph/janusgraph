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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;

/**
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
public class GuavaSubqueryCache extends SubsetSubqueryCache {
    private Cache<JointIndexQuery.Subquery, SubqueryResult> guavaCache;

    public GuavaSubqueryCache(int concurrencyLevel, long maximumWeight) {
        guavaCache = CacheBuilder.newBuilder()
            .weigher((Weigher<JointIndexQuery.Subquery, SubqueryResult>) (q, r) -> 2 + r.size())
            .concurrencyLevel(concurrencyLevel).maximumWeight(maximumWeight).build();
    }

    @Override
    protected SubqueryResult get(JointIndexQuery.Subquery key) {
        return guavaCache.getIfPresent(key);
    }

    @Override
    protected void put(JointIndexQuery.Subquery key, SubqueryResult result) {
        guavaCache.put(key, result);
    }

    @Override
    public synchronized void close() {
        guavaCache.invalidateAll();
        guavaCache.cleanUp();
    }
}
