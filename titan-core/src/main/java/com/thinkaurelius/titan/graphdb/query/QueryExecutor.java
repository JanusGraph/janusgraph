package com.thinkaurelius.titan.graphdb.query;

import java.util.Iterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface QueryExecutor<Q extends Query<Q>,R> {

    public boolean hasNew(Q query);

    public Iterator<R> getNew(Q query);

    public Iterator<R> execute(Q query);

}
