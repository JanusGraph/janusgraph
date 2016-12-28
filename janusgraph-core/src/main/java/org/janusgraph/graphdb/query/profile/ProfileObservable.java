package com.thinkaurelius.titan.graphdb.query.profile;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ProfileObservable {

    public void observeWith(QueryProfiler profiler);

}
