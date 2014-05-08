package com.thinkaurelius.titan.graphdb.query;

import com.thinkaurelius.titan.core.TitanElement;

import java.util.Collection;
import java.util.Comparator;

/**
 * A query that returns {@link TitanElement}s. This query can consist of multiple sub-queries that together
 * form the desired result set.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface ElementQuery<R extends TitanElement,B extends BackendQuery<B>> extends Query {

    /**
     * Whether the combination of the individual sub-queries can result in duplicate
     * results. Indicates to the query executor whether the results need to be de-duplicated
     *
     * @return true, if duplicate results are possible, else false
     */
    public boolean hasDuplicateResults();

    /**
     * Whether the result set of this query is empty
     *
     * @return
     */
    public boolean isEmpty();

    /**
     * Returns the number of sub-queries this query is comprised of.
     *
     * @return
     */
    public int numSubQueries();

    /**
     * Returns the backend query at the given position that comprises this ElementQuery
     * @param position
     * @return
     */
    public BackendQueryHolder<B> getSubQuery(int position);

    /**
     * Whether the given element matches the conditions of this query.
     * </p>
     * Used for result filtering if the result set returned by the query executor is not fitted.
     *
     * @param element
     * @return
     */
    public boolean matches(R element);

    /**
     * Whether this query expects the results to be in a particular sort order.
     *
     * @return
     */
    public boolean isSorted();

    /**
     * Returns the expected sort order of this query if any was specified. Check {@link #isSorted()} first.
     * @return
     */
    public Comparator<R> getSortOrder();

}
