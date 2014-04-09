package com.thinkaurelius.titan.graphdb.query;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */

public class BaseQuery implements Query {

    private int offset;
    private int limit;

    public BaseQuery() {
        this(NO_OFFSET, NO_LIMIT);
    }

    public BaseQuery(final int offset, final int limit) {
        assert offset >= 0;
        assert limit >= 0;
        this.offset = offset;
        this.limit = limit;
    }

    public BaseQuery setOffset(final int offset) {
        assert offset >= 0;
        this.offset = offset;
        return this;
    }

    public BaseQuery setLimit(final int limit) {
        assert limit >= 0;
        this.limit = limit;
        return this;
    }

    @Override
    public int getOffset() {
        return offset;
    }

    @Override
    public boolean hasOffset() {
        return offset != Query.NO_OFFSET;
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
