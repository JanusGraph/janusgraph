package com.thinkaurelius.titan.core;

import com.carrotsearch.hppc.LongArrayList;

/**
 * List of {@link TitanVertex}s.
 * <p/>
 * Basic interface for a list of vertices which supports retrieving individuals vertices or iterating over all of them,
 * but does not support modification.
 * <p/>
 * VertexList is returned by {@link TitanVertexQuery}. Depending on how the query was executed that returned this VertexList,
 * getting vertex ids might be significantly faster than retrieving vertex objects.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 */
public interface VertexList extends Iterable<TitanVertex> {


    /**
     * Returns the number of vertices in this list.
     *
     * @return Number of vertices in the list.
     */
    public int size();

    /**
     * Returns the vertex at a given position in the list.
     *
     * @param pos Position for which to retrieve the vertex.
     * @return TitanVertex at the given position
     */
    public TitanVertex get(int pos);

    /**
     * Sorts this list according to vertex ids in increasing order.
     * If the list is already sorted, invoking this method incurs no cost.
     *
     * @throws UnsupportedOperationException If not all vertices in this list have an id
     */
    public void sort();

    /**
     * Whether this list of vertices is sorted by id in increasing order.
     *
     * @return
     */
    public boolean isSorted();

    /**
     * Returns a sub list of this list of vertices from the given position with the given number of vertices.
     *
     * @param fromPosition
     * @param length
     * @return
     */
    public VertexList subList(int fromPosition, int length);

    /**
     * Returns a list of ids of all vertices in this list of vertices in the same order of the original vertex list.
     * <p/>
     * Uses an efficient primitive variable-sized array.
     *
     * @return A list of idAuthorities of all vertices in this list of vertices in the same order of the original vertex list.
     * @see LongArrayList
     */
    public LongArrayList getIDs();

    /**
     * Returns the id of the vertex at the specified position
     *
     * @param pos The position of the vertex in the list
     * @return The id of that vertex
     */
    public long getID(int pos);

}
