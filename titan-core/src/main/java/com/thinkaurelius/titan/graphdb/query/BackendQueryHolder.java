package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;

/**
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
