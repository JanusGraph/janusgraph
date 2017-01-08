package org.janusgraph.graphdb.query.vertex;

import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.VertexList;

/**
 * Extends on the {@link VertexList} interface by provided methods to add elements to the list
 * which is needed during query execution when the result list is created.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexListInternal extends VertexList {

    /**
     * Adds the provided vertex to this list.
     *
     * @param n
     */
    public void add(JanusGraphVertex n);

    /**
     * Copies all vertices in the given vertex list into this list.
     *
     * @param vertices
     */
    public void addAll(VertexList vertices);

}
