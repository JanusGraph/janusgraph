package com.thinkaurelius.titan.core.olap;

import com.thinkaurelius.titan.core.TitanVertexQuery;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface OLAPJobBuilder<S> {

    public OLAPJobBuilder<S> setJob(OLAPJob job);

    public OLAPJobBuilder<S> setStateKey(String stateKey);

    public OLAPJobBuilder<S> setInitialState(final S defaultValue);

    public OLAPJobBuilder<S> setInitializer(StateInitializer<S> initial);

    public OLAPJobBuilder<S> setInitialState(Map<Long,S> values);

    public OLAPJobBuilder<S> setNumVertices(long numVertices);

    public OLAPJobBuilder<S> setNumProcessingThreads(int numThreads);

    public OLAPQueryBuilder<S> addQuery();

    public Future<Map<Long,S>> execute();

}
