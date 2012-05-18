
package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.exceptions.InvalidNodeException;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

/**
 * A relationship is an edge which connects multiple nodes.
 * Compared to {@link TitanRelation}, a relationship provides additional methods for retrieving the connected nodes.
 * 
 * 
 * @see TitanRelation
 * @see TitanLabel
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface TitanEdge extends TitanRelation, Edge {

	/**
	 * Returns the relationship type of this relationship
	 * 
	 * @return relationship type of this relationship
	 */
	public TitanLabel getTitanLabel();

    /**
     * Returns the vertex for the specified direction
     *
     * @return The start node of this edge
     * @throws com.thinkaurelius.titan.exceptions.InvalidEdgeException if the edge is not binary
     */
    public TitanVertex getVertex(Direction dir);

	/**
	 * Returns the node at the opposite end of the relationship.
	 * 
	 *
	 * @param vertex TitanVertex on which this relationship is incident
	 * @return The node at the opposite end of the relationship.
	 * @throws InvalidNodeException if the relationship is not incident on the specified node or if the 
	 * relationship is not binary.
	 */
	public TitanVertex getOtherVertex(TitanVertex vertex);

	
}
