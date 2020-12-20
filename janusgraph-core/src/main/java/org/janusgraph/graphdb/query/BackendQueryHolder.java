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

import com.google.common.base.Preconditions;
import org.janusgraph.graphdb.query.profile.ProfileObservable;
import org.janusgraph.graphdb.query.profile.QueryProfiler;

/**
 * Holds a {@link BackendQuery} and captures additional information that pertains to its execution and to be used by a
 * {@link QueryExecutor}:
 * <ul>
 *     <li>Whether the query is fitted, i.e., whether all results returned from executing the backend query are part
 *     of the result set or must be filtered in memory.</li>
 *     <li>Whether the query results will already be sorted in the user defined sort order or whether extra sorting is
 *     required.</li>
 *     <li>Additional execution info required by the query executor. This would be compiled by the query optimizer
 *     and is passed through verbatim. Can be null.</li>
 * </ul>
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class BackendQueryHolder<E extends BackendQuery<E>> implements ProfileObservable {

    private final E backendQuery;
    private final boolean isFitted;
    private final boolean isSorted;
    private final Object executionInfo;
    private QueryProfiler profiler = QueryProfiler.NO_OP;

    public BackendQueryHolder(E backendQuery, boolean fitted, boolean sorted, Object executionInfo) {
        Preconditions.checkArgument(backendQuery!=null);
        this.backendQuery = backendQuery;
        isFitted = fitted;
        isSorted = sorted;
        this.executionInfo = executionInfo;
    }

    public BackendQueryHolder(E backendQuery, boolean fitted, boolean sorted) {
        this(backendQuery, fitted, sorted, null);
    }

    public Object getExecutionInfo() {
        return executionInfo;
    }

    public boolean isFitted() {
        return isFitted;
    }

    public boolean isSorted() {
        return isSorted;
    }

    public E getBackendQuery() {
        return backendQuery;
    }

    public QueryProfiler getProfiler() {
        return profiler;
    }

    @Override
    public void observeWith(QueryProfiler parentProfiler, boolean hasSiblings) {
        Preconditions.checkArgument(parentProfiler!=null);
        profiler = parentProfiler.addNested(QueryProfiler.OR_QUERY, hasSiblings);
        profiler.setAnnotation(QueryProfiler.FITTED_ANNOTATION,isFitted);
        profiler.setAnnotation(QueryProfiler.ORDERED_ANNOTATION,isSorted);
        profiler.setAnnotation(QueryProfiler.QUERY_ANNOTATION,backendQuery);
        if (backendQuery instanceof ProfileObservable) ((ProfileObservable)backendQuery).observeWith(profiler);
    }
}
