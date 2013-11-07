package com.thinkaurelius.titan.graphdb.query;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BaseQuery implements Query {

    private int limit;

    public BaseQuery() {
        this(NO_LIMIT);
    }

    public BaseQuery(final int limit) {
        assert limit >= 0;
        this.limit = limit;
    }

    public BaseQuery setLimit(final int limit) {
        assert limit >= 0;
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
