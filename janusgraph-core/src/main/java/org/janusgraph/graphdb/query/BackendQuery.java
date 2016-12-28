package com.thinkaurelius.titan.graphdb.query;

/**
 * A BackendQuery is a query that can be updated to a new limit.
 * </p>
 * This is useful in query execution where the query limit is successively relaxed to find all the needed elements
 * of the result set.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface BackendQuery<Q extends BackendQuery> extends Query {

    /**
     * Creates a new query identical to the current one but with the specified limit.
     *
     * @param newLimit
     * @return
     */
    public Q updateLimit(int newLimit);

}
