package com.thinkaurelius.titan.graphdb.query;

import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.graphdb.query.profile.QueryProfiler;

import java.util.Iterator;

/**
 * Executes a given query and its subqueries against an underlying data store and transaction.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface QueryExecutor<Q extends ElementQuery,R extends TitanElement,B extends BackendQuery> {

    /**
     * Returns all newly created elements in a transactional context that match the given query.
     *
     * @param query
     * @return
     */
    public Iterator<R> getNew(Q query);

    /**
     * Whether the transactional context contains any deletions that could potentially affect the result set of the given query.
     * This is used to determine whether results need to be checked for deletion with {@link #isDeleted(ElementQuery, com.thinkaurelius.titan.core.TitanElement)}.
     *
     * @param query
     * @return
     */
    public boolean hasDeletions(Q query);

    /**
     * Whether the given result entry has been deleted in the transactional context and should hence be removed from the result set.
     *
     * @param query
     * @param result
     * @return
     */
    public boolean isDeleted(Q query, R result);

    /**
     * Executes the given sub-query against a data store and returns an iterator over the results. These results are not yet adjusted
     * to any modification made in the transactional context which are done by the {@link QueryProcessor} using the other methods
     * of this interface.
     *
     * @param query
     * @param subquery
     * @param executionInfo
     * @param profiler
     * @return
     */
    public Iterator<R> execute(Q query, B subquery, Object executionInfo, QueryProfiler profiler);

}
