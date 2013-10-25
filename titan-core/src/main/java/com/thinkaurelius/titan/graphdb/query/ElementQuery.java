package com.thinkaurelius.titan.graphdb.query;

import com.thinkaurelius.titan.core.TitanElement;

import java.util.Collection;
import java.util.Comparator;

/**
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

    public boolean isEmpty();

    public int numSubQueries();

    /**
     * Returns the backend query at the given position that comprises this ElementQuery
     * @param position
     * @return
     */
    public BackendQueryHolder<B> getSubQuery(int position);

    public boolean matches(R element);

    public boolean isSorted();

    public Comparator<R> getSortOrder();

}
