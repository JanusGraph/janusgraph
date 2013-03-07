
package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

/**
 * A TitanEdge connects two {@link TitanVertex}. It extends the functionality provided by Blueprint's {@link Edge}.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see Edge
 * @see TitanRelation
 * @see TitanLabel
 */
public interface TitanEdge extends TitanRelation, Edge {

    /**
     * Returns the edge label of this edge
     *
     * @return edge label of this edge
     */
    public TitanLabel getTitanLabel();

    /**
     * Returns the vertex for the specified direction.
     * The direction cannot be Direction.BOTH.
     *
     * @return the vertex for the specified direction
     */
    public TitanVertex getVertex(Direction dir);

    /**
     * Returns the vertex at the opposite end of the edge.
     *
     * @param vertex vertex on which this edge is incident
     * @return The vertex at the opposite end of the edge.
     * @throws InvalidElementException if the edge is not incident on the specified vertex
     */
    public TitanVertex getOtherVertex(TitanVertex vertex);


    /**
     * Checks whether this relation is directed, i.e. has a start and end vertex
     * both of which are aware of the incident edge.
     *
     * @return true, if this relation is directed, else false.
     */
    public boolean isDirected();

    /**
     * Checks whether this relation is unidirected, i.e. only the start vertex is aware of
     * the edge's existence. A unidirected edge is similar to a link.
     *
     * @return true, if this relation is unidirected, else false.
     */
    public boolean isUnidirected();


}
