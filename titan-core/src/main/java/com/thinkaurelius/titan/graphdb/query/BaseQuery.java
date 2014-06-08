package com.thinkaurelius.titan.graphdb.query;

/**
 * Standard implementation of {@link Query}.
 *
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

    /**
     * Sets the limit of the query if it wasn't specified in the constructor
     * @param limit
     * @return
     */
    public BaseQuery setLimit(final int limit) {
        assert limit >= 0;
        this.limit = limit;
        return this;
    }

    @Override
    public int getLimit() {
        return limit;
    }

    @Override
    public boolean hasLimit() {
        return limit != Query.NO_LIMIT;
    }

}
