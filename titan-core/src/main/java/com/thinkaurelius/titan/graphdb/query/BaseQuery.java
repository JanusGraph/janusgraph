package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BaseQuery implements Query {

    private int limit = Query.NO_LIMIT;

    public BaseQuery() {
    }

    public BaseQuery(final int limit) {
        Preconditions.checkArgument(limit >= 0, "Inavlid limit: %s", limit);
        this.limit = limit;
    }

    public BaseQuery setLimit(final int limit) {
        Preconditions.checkArgument(limit >= 0, "Inavlid limit: %s", limit);
        this.limit = limit;
        return this;
    }

    /**
     * @return The maximum number of results to return
     */
    @Override
    public int getLimit() {
        return limit;
    }

    @Override
    public boolean hasLimit() {
        return limit != Query.NO_LIMIT;
    }

}
