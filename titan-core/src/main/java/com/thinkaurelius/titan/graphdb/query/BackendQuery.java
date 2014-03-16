package com.thinkaurelius.titan.graphdb.query;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */

public interface BackendQuery<Q extends BackendQuery> extends Query {

    /**
     * Creates a new query identical to the current one but with the specified limit.
     *
     * @param newLimit
     * @return
     */
    public Q updateLimit(int newLimit);

    /**
     * Creates a new query identical to the current one but with the specified offset.
     *
     * @param newOffset
     * @return
     */
    public Q updateOffset(int newOffset);
}
