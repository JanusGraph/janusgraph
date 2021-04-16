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

import org.janusgraph.graphdb.query.graph.JointIndexQuery;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Different from a simple exact mapping from keys to values,
 * it leverages the fact that the results of a Subquery A with limit X are first X entries of the results of the
 * a Subquery B with limit Y, where A and B only differ in limit, and X is less than or equal to Y.
 * <p>
 * For example, suppose the cache is empty at the beginning. Firstly, prop1_idx:multiKSQ[1]@100 query leads to a cache
 * miss, and then results are loaded into cache. Secondly, prop1_idx:multiKSQ[1]@20 query leads to a cache hit, and
 * then first 20 results of cached responses are returned. Thirdly, prop1_idx:multiKSQ[1]@2000 leads to a cache miss,
 * then results are loaded into cache, and initial results saved by prop1_idx:multiKSQ[1]@100 query are overriden by
 * the new results.
 * <p>
 * Internally, raw query results are encapsulated in {@link SubqueryResult} object together with limit of the query.
 * Meanwhile, keys are stored without limit (or with a dummy limit). Whenever there is a hit for key, the limit of
 * cached results and that of the query will be compared, and whether this is a cache hit or miss will be determined.
 * <p>
 * Compared to a simply {@link JointIndexQuery.Subquery} to {@code List<Object>} mapping, this cache has two benefits:
 * 1) it reduces latency, as "subset" queries can effectively leverage cached results.
 * 2) it saves space, as queries only differing in limit will not be saved more than once in cache.
 *
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
public abstract class SubsetSubqueryCache implements SubqueryCache {

    protected abstract SubqueryResult get(JointIndexQuery.Subquery key);

    protected abstract void put(JointIndexQuery.Subquery key, SubqueryResult result);

    @Override
    public List<Object> getIfPresent(JointIndexQuery.Subquery key) {
        int limit = key.getLimit();
        JointIndexQuery.Subquery noLimitKey = key.updateLimit(0);
        SubqueryResult result = get(noLimitKey);
        if (result != null && (result.getQueryLimit() >= limit || result.getQueryLimit() > result.size())) {
            return result.getValues().subList(0, Math.min(limit, result.size()));
        }
        return null;
    }

    @Override
    public void put(JointIndexQuery.Subquery key, List<Object> values) {
        int limit = key.getLimit();
        JointIndexQuery.Subquery noLimitKey = key.updateLimit(0);
        put(noLimitKey, new SubqueryResult(values, limit));
    }

    @Override
    public List<Object> get(JointIndexQuery.Subquery key, Callable<? extends List<Object>> valueLoader) throws Exception {
        List<Object> values = getIfPresent(key);
        if (values == null) {
            values = valueLoader.call();
            put(key, values);
        }
        return values;
    }

    static class SubqueryResult {
        private List<Object> values;
        private int queryLimit;

        protected SubqueryResult(List<Object> values, int queryLimit) {
            this.values = values;
            this.queryLimit = queryLimit;
        }

        public List<Object> getValues() {
            return values;
        }

        public int getQueryLimit() {
            return queryLimit;
        }

        public int size() {
            return values.size();
        }
    }
}

