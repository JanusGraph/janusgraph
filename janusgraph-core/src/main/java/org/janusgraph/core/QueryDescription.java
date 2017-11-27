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

package org.janusgraph.core;


import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface QueryDescription {

    /**
     * Returns a string representation of the entire query
     * @return
     */
    @Override
    String toString();

    /**
     * Returns how many individual queries are combined into this query, meaning, how many
     * queries will be executed in one batch.
     *
     * @return
     */
    int getNoCombinedQueries();

    /**
     * Returns the number of sub-queries this query is comprised of. Each sub-query represents one OR clause, i.e.,
     * the union of each sub-query's result is the overall result.
     *
     * @return
     */
    int getNoSubQueries();

    /**
     * Returns a list of all sub-queries that comprise this query
     * @return
     */
    List<? extends SubQuery> getSubQueries();

    /**
     * Represents one sub-query of this query. Each sub-query represents one OR clause.
     */
    interface SubQuery {

        /**
         * Whether this query is fitted, i.e. whether the returned results must be filtered in-memory.
         * @return
         */
        boolean isFitted();

        /**
         * Whether this query respects the sort order of parent query or requires sorting in-memory.
         * @return
         */
        boolean isSorted();

    }


}
