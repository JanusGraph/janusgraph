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
 * Cache for {@link JointIndexQuery.Subquery} results. Cache entries are manually added using
 * {@link #get(JointIndexQuery.Subquery, Callable)} or {@link #put(JointIndexQuery.Subquery, List)},
 * and stored in the cache until evicted or invalidated.
*
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
public interface SubqueryCache {

    /**
     * Return a list of results if given query exists in cache, otherwise
     * return null
     *
     * @param query a subquery of joint index query
     * @return a list of matching results or null if is not in the cache
     */
    List<Object> getIfPresent(JointIndexQuery.Subquery query);

    /**
     * Add given values into cache
     *
     * @param query  a subquery of joint index query
     * @param values a list of results to be cached
     */
    void put(JointIndexQuery.Subquery query, List<Object> values);

    /**
     * Returns a list of results. If given query not in cache, call the
     * value loader to retrieve results and put into the cache
     *
     * @param query       a subquery of joint index query
     * @param valueLoader a callable that returns a list of results
     * @return a list of results
     * @throws Exception if exception thrown when calling the value loader
     */
    List<Object> get(JointIndexQuery.Subquery query, Callable<? extends List<Object>> valueLoader) throws Exception;

    /**
     * Closes the cache which allows the cache to release allocated memory.
     * Calling any of the other methods after closing a cache has undetermined behavior.
     */
    void close();
}
