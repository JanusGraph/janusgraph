
package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.exceptions.InvalidNodeException;

/**
 * A relationship is an edge which connects multiple nodes.
 * Compared to {@link com.thinkaurelius.titan.core.Edge}, a relationship provides additional methods for retrieving the connected nodes.
 * 
 * 
 * @see com.thinkaurelius.titan.core.Edge
 * @see RelationshipType
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface Relationship extends Edge {

	/**
	 * Returns the relationship type of this relationship
	 * 
	 * @return relationship type of this relationship
	 */
	public RelationshipType getRelationshipType();
	
	/**
	 * Returns the unique end node of this relationship. This method assumes that the relationship is binary.
	 * 
	 * Note that this method is only clearly defined for directed relationship.
	 * For undirected relationship, it returns any one of the two nodes (with {@link com.thinkaurelius.titan.core.Edge#getStart()}
	 * returning the other one).
	 *
	 * @return The end node of this relationship
	 * @throws com.thinkaurelius.titan.exceptions.InvalidEdgeException if the relationship is not binary
	 */
	public Node getEnd();
	
	/**
	 * Returns the node at the opposite end of the relationship.
	 * 
	 *
	 * @param n Node on which this relationship is incident
	 * @return The node at the opposite end of the relationship.
	 * @throws InvalidNodeException if the relationship is not incident on the specified node or if the 
	 * relationship is not binary.
	 */
	public Node getOtherNode(Node n);

	/**
	 * Counts how many times the given node occurs in this relationship.
	 * 
	 * If the relationship is not incident on the specified node, this method returns 0.
	 * If the relationship is a loop-edge with respect to the specified node, a value bigger than 1 is returned.
	 * 
	 * @param n Node for which to count multiplicity for this relationship.
	 * @return Multiplicity of this edge for the specified node.
	 */
	public int getMultiplicity(Node n);

	
	
}
