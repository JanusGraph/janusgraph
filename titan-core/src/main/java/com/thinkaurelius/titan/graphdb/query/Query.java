package com.thinkaurelius.titan.graphdb.query;

/**
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface Query {

    public static final int NO_LIMIT = Integer.MAX_VALUE;

    public boolean hasLimit();

    public int getLimit();



}
