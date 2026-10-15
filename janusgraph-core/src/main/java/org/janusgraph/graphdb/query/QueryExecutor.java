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

package org.janusgraph.graphdb.query;

import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.graphdb.query.profile.QueryProfiler;

import java.util.Iterator;

/**
 * Executes a given query and its subqueries against an underlying data store and transaction.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface QueryExecutor<Q extends ElementQuery,R extends JanusGraphElement,B extends BackendQuery> {

    /**
     * Returns all newly created elements in a transactional context that match the given query.
     *
     * @param query
     * @return
     */
    Iterator<R> getNew(Q query);

    /**
     * Whether the transactional context contains any deletions that could potentially affect the result set of the given query.
     * This is used to determine whether results need to be checked for deletion with {@link #isDeleted(ElementQuery, org.janusgraph.core.JanusGraphElement)}.
     *
     * @param query
     * @return
     */
    boolean hasDeletions(Q query);

    /**
     * Whether the given result entry has been deleted in the transactional context and should hence be removed from the result set.
     *
     * @param query
     * @param result
     * @return
     */
    boolean isDeleted(Q query, R result);

    /**
     * Executes the given sub-query against a data store and returns an iterator over the results. These results are not yet adjusted
     * to any modification made in the transactional context which are done by the {@link QueryProcessor} using the other methods
     * of this interface.
     *
     * @param query
     * @param subquery
     * @param executionInfo
     * @param profiler
     * @return
     */
    Iterator<R> execute(Q query, B subquery, Object executionInfo, QueryProfiler profiler);

    /**
     * Returns a sub-query which returns only those results of the given one whose value of the query's first order key
     * doesn't come after the given value in the query's order. The {@link QueryProcessor} asks for it where the
     * transaction's new elements fill the query's limit, and the given value is that of the last of them within it: no
     * result whose value comes after it can come within the limit. Returns the given sub-query itself where its backend
     * can't leave the other results out, which is what this default does.
     *
     * @param query the query, which has a limit and an order
     * @param subquery a sub-query of the query whose backend returns its results in the query's order
     * @param value the value of the query's first order key which no result returned may come after
     * @return the narrowed sub-query, or the given one
     */
    default B withoutResultsAfter(Q query, B subquery, Object value) {
        return subquery;
    }

}
