package com.thinkaurelius.titan.graphdb.query;

/**
 * Standard Query interface specifying that a query may have a limit.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Query {

    public static final int NO_LIMIT = Integer.MAX_VALUE;

    /**
     * Whether this query has a defined limit
     *
     * @return
     */
    public boolean hasLimit();

    /**
     *
     * @return The maximum number of results this query should return
     */
    public int getLimit();



}
