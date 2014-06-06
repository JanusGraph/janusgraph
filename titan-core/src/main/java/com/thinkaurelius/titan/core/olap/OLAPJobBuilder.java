package com.thinkaurelius.titan.core.olap;

import com.thinkaurelius.titan.core.TitanVertexQuery;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

/**
 * Builder to define and configure an {@link OLAPJob} for execution.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface OLAPJobBuilder<S> {

    /**
     * Configures the {@link OLAPJob} vertex-centric program to execute on all vertices.
     * @param job
     * @return
     */
    public OLAPJobBuilder<S> setJob(OLAPJob<S> job);

    /**
     * Defines the name of the key to be used as the dedicated "state" key. Retrieving the property value for this
     * key on any given vertex in an {@link OLAPJob} will return the current vertex state.
     *
     * @param stateKey
     * @return
     */
    public OLAPJobBuilder<S> setStateKey(String stateKey);

    /**
     * Sets an {@link StateInitializer} function to initialize the vertex state on demand.
     *
     * @param initial
     * @return
     */
    public OLAPJobBuilder<S> setInitializer(StateInitializer<S> initial);

    /**
     * Set the initial state of the vertices where the key is the vertex id and the value is the state
     * of the vertex.
     *
     * @param values
     * @return
     */
    public OLAPJobBuilder<S> setInitialState(Map<Long,S> values);

    /**
     * Sets the initial state of the vertices to the result from a previous OLAP computation.
     * <p />
     * Note, that the data structures underlying the {@link OLAPResult} may be removed to be memory efficient
     * which means that the previous state can be overwritten and is therefore lost.
     *
     * @param values
     * @return
     */
    public OLAPJobBuilder<S> setInitialState(OLAPResult<S> values);


    /**
     * If the exact number of vertices to be processed is know a priori, it can be specified
     * via this method to make memory allocation more efficient. Setting is value is optional
     * and does not impact correctness. Providing the exact number of vertices might make
     * execution faster.
     *
     * @param numVertices
     * @return
     */
    public OLAPJobBuilder<S> setNumVertices(long numVertices);

    /**
     * Configure the number of threads to execute the configured {@link OLAPJob}.
     *
     * @param numThreads
     * @return
     */
    public OLAPJobBuilder<S> setNumProcessingThreads(int numThreads);

    /**
     * Adds a new vertex query to this job. The vertex queries specify which edges and properties will be accessible when
     * the configured {@link OLAPJob} executes. Only the data that can be retrieved through previously configured queries
     * will be accessible during execution.
     *
     * @return
     */
    public OLAPQueryBuilder<S,?,?> addQuery();

    /**
     * Starts the execution of this job and returns the computed vertex states as a {@link OLAPResult}.
     *
     * @return
     */
    public Future<OLAPResult<S>> execute();

}
