package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;

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
public class BackendQueryHolder<E extends BackendQuery<E>> {

    private final E backendQuery;
    private final boolean isFitted;
    private final boolean isSorted;
    private final Object executionInfo;

    public BackendQueryHolder(E backendQuery, boolean fitted, boolean sorted, Object executionInfo) {
        Preconditions.checkNotNull(backendQuery);
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
}
