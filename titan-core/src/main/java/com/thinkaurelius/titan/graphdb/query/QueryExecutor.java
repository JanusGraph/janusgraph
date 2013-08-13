package com.thinkaurelius.titan.graphdb.query;

import com.thinkaurelius.titan.core.TitanElement;

import java.util.Iterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface QueryExecutor<Q extends ElementQuery,R extends TitanElement,B extends BackendQuery> {

    public Iterator<R> getNew(Q query);

    public boolean hasDeletions(Q query);

    public boolean isDeleted(Q query, R result);

    public Iterator<R> execute(Q query, B subquery, Object executionInfo);

}
