package com.thinkaurelius.titan.graphdb.transaction.vertexcache;

import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.util.datastructures.Retriever;

public interface VertexCache {

    /**
     * Checks whether the cache contains a vertex with the given id
     *
     * @param id Vertex id
     * @return true if a vertex with the given id is contained, else false
     */
    public boolean contains(long id);

    /**
     * Returns the vertex with the given id or null if it is not in the cache
     *
     * @param id
     * @return
     */
    public InternalVertex get(long id, Retriever<Long,InternalVertex> vertexConstructor);

    /**
     * Adds the given vertex with the given id to the cache
     *
     * @param vertex
     * @param id
     * @throws IllegalArgumentException if the vertex is null or the id negative
     */
    public void add(InternalVertex vertex, long id);

    /**
     * Returns an iterable over all vertices in the cache
     *
     * @return
     */
    public Iterable<InternalVertex> getAll();

    /**
     * Closes the cache which allows the cache to release allocated memory.
     * Calling any of the other methods after closing a cache has undetermined behavior.
     */
    public void close();

}
