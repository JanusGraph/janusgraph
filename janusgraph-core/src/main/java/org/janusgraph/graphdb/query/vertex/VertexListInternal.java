package com.thinkaurelius.titan.graphdb.query.vertex;

import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.VertexList;

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
    public void add(TitanVertex n);

    /**
     * Copies all vertices in the given vertex list into this list.
     *
     * @param vertices
     */
    public void addAll(VertexList vertices);

}
