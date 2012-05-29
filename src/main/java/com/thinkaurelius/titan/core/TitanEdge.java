
package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

/**
 * A TitanEdge connects two {@link TitanVertex}. It extends the functionality provided by Blueprint's {@link Edge}.
 *
 * @see Edge
 * @see TitanRelation
 * @see TitanLabel
 *
 * @author	Matthias Br&ouml;cheler (http://www.matthiasb.com)
 *
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
	 *
	 * @param vertex vertex on which this edge is incident
	 * @return The vertex at the opposite end of the edge.
	 * @throws InvalidElementException if the edge is not incident on the specified vertex
	 */
	public TitanVertex getOtherVertex(TitanVertex vertex);

	
}
