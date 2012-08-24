package com.thinkaurelius.titan.graphdb.transaction;

import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;

public interface VertexCache {

    /**
     * Checks whether the cache contains a vertex with the given id
     * @param id Vertex id
     * @return true if a vertex with the given id is contained, else false
     */
	public boolean contains(long id);

    /**
     * Returns the vertex with the given id or null if it is not in the cache
     * @param id
     * @return
     */
	public InternalTitanVertex get(long id);

    /**
     * Adds the given vertex with the given id to the cache
     * @param vertex
     * @param id
     * @throws IllegalArgumentException if the vertex is null or the id negative
     */
	public void add(InternalTitanVertex vertex, long id);

    /**
     * Returns an iterable over all vertices in the cache
     * @return
     */
    public Iterable<InternalTitanVertex> getAll();

    /**
     * Removes the vertex with the given id from the cache
     * @param vertexid
     * @return
     */
    public boolean remove(long vertexid);

    /**
     * Closes the cache which allows the cache to release allocated memory.
     * Calling any of the other methods after closing a cache has undetermined behavior.
     */
	public void close();
	
}
