package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BackendQueryHolder<E extends BackendQuery<E>> {

    private final E backendQuery;
    private final boolean isFitted;
    private final Object executionInfo;

    public BackendQueryHolder(E backendQuery, boolean fitted, Object executionInfo) {
        Preconditions.checkNotNull(backendQuery);
        this.backendQuery = backendQuery;
        isFitted = fitted;
        this.executionInfo = executionInfo;
    }

    public BackendQueryHolder(E backendQuery, boolean fitted) {
        this(backendQuery,fitted,null);
    }

    public Object getExecutionInfo() {
        return executionInfo;
    }

    public boolean isFitted() {
        return isFitted;
    }

    public E getBackendQuery() {
        return backendQuery;
    }
}
