package com.thinkaurelius.titan.core.olap;

import java.util.Map;

/**
 * The result of an {@link OLAPJob} execution against a graph. The result contains the final vertex states for the
 * vertices in the graph which can be retrieved by vertex id for iterated over like a map.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface OLAPResult<S> {

    /**
     * Returns an {@link Iterable} over all final vertex states.
     *
     * @return
     */
    public Iterable<S> values();

    /**
     * Returns an {@link Iterable} over all final (vertex-id,vertex-state) pairs resulting from a job's execution.
     * @return
     */
    public Iterable<Map.Entry<Long,S>> entries();

    /**
     * Returns the number of vertices in the result
     * @return
     */
    public long size();

    /**
     * Returns the final vertex state for a given vertex identified by its id.
     *
     * @param vertexid
     * @return
     */
    public S get(long vertexid);

}
