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
import org.janusgraph.graphdb.query.profile.ProfileObservable;

import java.util.Comparator;

/**
 * A query that returns {@link JanusGraphElement}s. This query can consist of multiple sub-queries that together
 * form the desired result set.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface ElementQuery<R extends JanusGraphElement,B extends BackendQuery<B>> extends Query, ProfileObservable {

    /**
     * Whether the combination of the individual sub-queries can result in duplicate
     * results. Indicates to the query executor whether the results need to be de-duplicated
     *
     * @return true, if duplicate results are possible, else false
     */
    boolean hasDuplicateResults();

    /**
     * Whether the result set of this query is empty
     *
     * @return
     */
    boolean isEmpty();

    /**
     * Returns the number of sub-queries this query is comprised of.
     *
     * @return
     */
    int numSubQueries();

    /**
     * Returns the backend query at the given position that comprises this ElementQuery
     * @param position
     * @return
     */
    BackendQueryHolder<B> getSubQuery(int position);

    /**
     * Whether the given element matches the conditions of this query.
     * <p>
     * Used for result filtering if the result set returned by the query executor is not fitted.
     *
     * @param element
     * @return
     */
    boolean matches(R element);

    /**
     * Whether this query expects the results to be in a particular sort order.
     *
     * @return
     */
    boolean isSorted();

    /**
     * Returns the expected sort order of this query if any was specified. Check {@link #isSorted()} first.
     * @return
     */
    Comparator<R> getSortOrder();

}
