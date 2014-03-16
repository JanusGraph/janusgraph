package com.thinkaurelius.titan.graphdb.query;

/**
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */

public interface Query {

    public static final int NO_OFFSET = 0;
    public static final int NO_LIMIT = Integer.MAX_VALUE;

    public boolean hasLimit();
    public boolean hasOffset();

    public int getLimit();
    public int getOffset();
}
