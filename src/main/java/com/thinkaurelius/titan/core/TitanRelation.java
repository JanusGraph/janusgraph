
package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.exceptions.InvalidNodeException;
import com.tinkerpop.blueprints.Direction;

/**
 * TitanRelation is one of the two basic entities of a graph - the other one being {@link TitanVertex}.
 * An edge, also called <i>connection</i> or <i>link</i>, connects entities in a graph. 
 * For more information on the mathematical definition of a graph and the concept of edges see 
 * <a href="http://en.wikipedia.org/wiki/Graph_%28mathematics%29">Graph Definition</a>.
 * 
 * An edge connects multiple nodes, in which case it is called {@link TitanEdge}, or a node with an
 * attribute, in which case it is called a {@link TitanProperty}. An edge can be either a relationship <b>or</b>
 * a property - never both.
 * All edges have a unique associated {@link TitanType} which defines characteristics of the edge such
 * as its arity (i.e. the number of nodes it connects).
 * 
 * Depending on the implementation, edges are treated as entities or nodes in their own right, meaning
 * edges may have incident relationships or properties. An edge with incident properties is called <i>labeled</i>.
 * 
 * Edges are created on a node by calling the node's {@link TitanVertex#addEdge(TitanLabel, TitanVertex)} and
 * {@link TitanVertex#addProperty(TitanKey, Object)} methods, or using the methods
 * {@link TitanTransaction#addEdge(TitanLabel, TitanVertex start, TitanVertex end)} and {@link TitanTransaction#addProperty(TitanKey, TitanVertex, Object)}
 * provided by a graph transaction.
 * 
 * @see    TitanVertex
 * @see TitanEdge
 * @see TitanProperty
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface TitanRelation extends TitanVertex {


	/**
	 * Returns the edge type of this edge
	 * 
	 * @return TitanRelation Type of this edge
	 */
	public TitanType getType();

	/**
	 * Returns the direction of this edge from the perspective of the specified node.
	 * 
	 * @param vertex node on which the edge is incident
	 * @return The direction of this edge from the perspective of the specified node.
	 * @throws InvalidNodeException if this edge is not incident on the node
	 *
	 */
	public Direction getDirection(TitanVertex vertex);
	
	/**
	 * Checks whether this edge is incident on the specified node.
	 * 
	 * @param vertex TitanVertex to check incidence for
	 * @return true, if this edge is incident on the node, else false
	 */
	public boolean isIncidentOn(TitanVertex vertex);
	
	/**
	 * Checks whether this edge is directed.
	 * 
	 * @return true, if this edge is directed, else false.
	 * @see com.thinkaurelius.titan.graphdb.edgetypes.Directionality
	 */
	public boolean isDirected();
	
	
	/**
	 * Checks whether this edge is undirected.
	 * 
	 * @return true, if this edge is undirected, else false.
	 * @see com.thinkaurelius.titan.graphdb.edgetypes.Directionality
	 */
	public boolean isUndirected();
	
	/**
	 * Checks whether this edge is unidirected.
	 * 
	 * @return true, if this edge is unidirected, else false.
	 * @see com.thinkaurelius.titan.graphdb.edgetypes.Directionality
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
	 * @see com.thinkaurelius.titan.graphdb.edgetypes.EdgeCategory
	 */
	public boolean isSimple();

	
	/**
	 * Checks whether this edge is a loop with respect to the specified node.
	 * An edge is a loop if it connects a node with itself.
	 * 
	 * @param vertex TitanVertex to check the loop condition for
	 * @return true, if this edge is a loop with respect to the given node, else false.
	 */
	boolean isSelfLoop();

	/**
	 * Checks whether this edge is a property.
	 * 
	 * 
	 * @return true, if this edge is a property, else false.
	 * @see TitanProperty
	 */
	boolean isProperty();
	
	/**
	 * Checks whether this edge is a relationship.
	 * 
	 * 
	 * @return true, if this edge is a relationship, else false.
	 * @see TitanEdge
	 */
	boolean isEdge();

	
}
