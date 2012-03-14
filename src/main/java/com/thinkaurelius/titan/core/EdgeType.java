
package com.thinkaurelius.titan.core;


/**
 * EdgeType defines the schema for edges. 
 * Each {@link com.thinkaurelius.titan.core.Edge} has a unique edge type which defines many important characteristics of the edge,
 * {@link com.thinkaurelius.titan.core.Directionality} and {@link EdgeTypeGroup}.
 * 
 * An edge type is constructed via an {@link EdgeTypeMaker} instance which is returned by a {@link GraphTransaction}
 * when calling {@link GraphTransaction#createEdgeType()}.
 * 
 * Edge type names must be unique in a graph database. Many methods allow the name of the edge type as an argument
 * instead of the actual edge type denotes by this name.
 * 
 * @see	com.thinkaurelius.titan.core.Edge
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface EdgeType extends Node {

	/**
	 * Returns the name of this edge type.
	 * Edge type names must be unique.
	 * @return Name of this edge type.
	 */
	public String getName();
	
	/**
	 * Checks whether this edge type is functional.
	 * If an edge type is functional, all edges of this type must be functional in the mathematical sense.
	 * An edge is functional if its start node has at most one incident edge of this type.  
	 * Only binary edge types can be functional.
	 * 
	 * @return true, if the edge type is functional, else false
	 */
	public boolean isFunctional();
	
	/**
	 * Returns the directionality of this edge type
	 * @return Directionality of this edge type
	 * @see com.thinkaurelius.titan.core.Directionality
	 */
	public Directionality getDirectionality();	
	
	/**
	 * Checks whether this edge type is hidden.
	 * If an edge type is hidden, its edges are not included in edge retrieval operations.
	 * 
	 * @return true, if the edge type is hidden, else false.
	 */
	public boolean isHidden();
	
	/**
	 * Checks whether edges of this type are modifiable after creation.
	 * 
	 * @return true, if edges of this type are modifiable, else false.
	 */
	public boolean isModifiable();
	
	/**
	 * Returns the edge category of this edge type.
	 * 
	 * @return Edge category of this edge type.
	 * @see com.thinkaurelius.titan.core.EdgeCategory
	 */
	public EdgeCategory getCategory();
	
	/**
	 * Returns the edge type group of this edge type.
	 * 
	 * @return Edge type group of this edge type.
	 * @see EdgeTypeGroup
	 */
	public EdgeTypeGroup getGroup();
	
	/**
	 * Checks if this edge type is a property type
	 * 
	 * @return true, if this edge type is a property type, else false.
	 * @see PropertyType
	 */
	public boolean isPropertyType();
	
	/**
	 * Checks if this edge type is a relationship type
	 * 
	 * @return true, if this edge type is a relationship type, else false.
	 * @see RelationshipType
	 */
	public boolean isRelationshipType();
	
}
