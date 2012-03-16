
package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.exceptions.InvalidNodeException;

import java.util.Collection;

/**
 * Edge is one of the two basic entities of a graph - the other one being {@link Node}.
 * An edge, also called <i>connection</i> or <i>link</i>, connects entities in a graph. 
 * For more information on the mathematical definition of a graph and the concept of edges see 
 * <a href="http://en.wikipedia.org/wiki/Graph_%28mathematics%29">Graph Definition</a>.
 * 
 * An edge connects multiple nodes, in which case it is called {@link Relationship}, or a node with an
 * attribute, in which case it is called a {@link Property}. An edge can be either a relationship <b>or</b>
 * a property - never both.
 * All edges have a unique associated {@link EdgeType} which defines characteristics of the edge such
 * as its arity (i.e. the number of nodes it connects).
 * 
 * Depending on the implementation, edges are treated as entities or nodes in their own right, meaning
 * edges may have incident relationships or properties. An edge with incident properties is called <i>labeled</i>.
 * 
 * Edges are created on a node by calling the node's {@link Node#createRelationship(RelationshipType, Node)} and
 * {@link Node#createProperty(PropertyType, Object)} methods, or using the methods 
 * {@link GraphTransaction#createRelationship(RelationshipType, Node start, Node end)} and {@link GraphTransaction#createProperty(PropertyType, Node, Object)}
 * provided by a graph transaction.
 * 
 * @see	Node
 * @see Relationship
 * @see Property
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface Edge extends Node {


	/**
	 * Returns the edge type of this edge
	 * 
	 * @return Edge Type of this edge
	 */
	public EdgeType getEdgeType();

	/**
	 * Returns a collection of all nodes connected by this edge
	 * 
	 * @return All nodes connected by this edge
	 */
	public Collection<? extends Node> getNodes();
	
	/**
	 * Returns the unique start node of this edge. This method assumes that the edge is binary.
	 * 
	 * Note that this method is only clearly defined for directed edges.
	 * For undirected edges, it returns any one of the two nodes (with {@link Relationship#getEnd()}
	 * returning the other one).
	 *
	 * @return The start node of this edge
	 * @throws com.thinkaurelius.titan.exceptions.InvalidEdgeException if the edge is not binary
	 */
	public Node getStart();

	
	/**
	 * Determines and returns the position of the specified node for this edge. 
	 * 
	 * Note that this is only defined for non-loop edges, otherwise this method throws an exception.
	 * 
	 * @param n Specified node on which edge is incident
	 * @return The position of the specified node with respect to this edge
	 * @throws InvalidNodeException if edge is not incident on node
	 * @throws com.thinkaurelius.titan.exceptions.InvalidEdgeException if edge is a loop
	 * 
	 * @see NodePosition
	 */
	public NodePosition getPosition(Node n);
	
	/**
	 * Returns the direction of this edge from the perspective of the specified node.
	 * 
	 * @param n node on which the edge is incident
	 * @return The direction of this edge from the perspective of the specified node.
	 * @throws InvalidNodeException if this edge is not incident on the node
	 * 
	 * @see com.thinkaurelius.titan.core.Direction
	 */
	public Direction getDirection(Node n);
	
	/**
	 * Checks whether this edge is incident on the specified node.
	 * 
	 * @param n Node to check incidence for
	 * @return true, if this edge is incident on the node, else false
	 */
	public boolean isIncidentOn(Node n);
	
	/**
	 * Checks whether this edge is directed.
	 * 
	 * @return true, if this edge is directed, else false.
	 * @see com.thinkaurelius.titan.core.Directionality
	 */
	public boolean isDirected();
	
	
	/**
	 * Checks whether this edge is undirected.
	 * 
	 * @return true, if this edge is undirected, else false.
	 * @see com.thinkaurelius.titan.core.Directionality
	 */
	public boolean isUndirected();
	
	/**
	 * Checks whether this edge is unidirected.
	 * 
	 * @return true, if this edge is unidirected, else false.
	 * @see com.thinkaurelius.titan.core.Directionality
	 */
	public boolean isUnidirected();
	
	/**
	 * Checks whether this edge can be modified in the context of this transaction.
	 * 
	 * @return true, if this edge can be modified, else false.
	 */
	public boolean isModifiable();

	/**
	 * Checks whether this edge is simple.
	 * 
	 * @return true, if this edge is simple, else false
	 * @see EdgeCategory
	 */
	public boolean isSimple();

	
	/**
	 * Checks whether this edge is a loop with respect to the specified node.
	 * An edge is a loop if it connects a node with itself.
	 * 
	 * @param node Node to check the loop condition for
	 * @return true, if this edge is a loop with respect to the given node, else false.
	 */
	boolean isSelfLoop(Node node);

	/**
	 * Checks whether this edge is a property.
	 * 
	 * 
	 * @return true, if this edge is a property, else false.
	 * @see Property
	 */
	boolean isProperty();
	
	/**
	 * Checks whether this edge is a relationship.
	 * 
	 * 
	 * @return true, if this edge is a relationship, else false.
	 * @see Relationship
	 */
	boolean isRelationship();

	
}
