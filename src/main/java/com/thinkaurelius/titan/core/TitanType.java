
package com.thinkaurelius.titan.core;


/**
 * TitanType defines the schema for edges.
 * Each {@link TitanRelation} has a unique edge type which defines many important characteristics of the edge,
 * {@link com.thinkaurelius.titan.graphdb.edgetypes.Directionality} and {@link TypeGroup}.
 * 
 * An edge type is constructed via an {@link TypeMaker} INSTANCE which is returned by a {@link TitanTransaction}
 * when calling {@link TitanTransaction#makeType()}.
 * 
 * TitanRelation type names must be unique in a graph database. Many methods allow the name of the edge type as an argument
 * instead of the actual edge type denotes by this name.
 * 
 * @see    TitanRelation
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface TitanType extends TitanVertex {

	/**
	 * Returns the name of this edge type.
	 * TitanRelation type names must be unique.
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
	
	public boolean isSimple();

	/**
	 * Returns the edge type group of this edge type.
	 * 
	 * @return TitanRelation type group of this edge type.
	 * @see TypeGroup
	 */
	public TypeGroup getGroup();
	
	/**
	 * Checks if this edge type is a property type
	 * 
	 * @return true, if this edge type is a property type, else false.
	 * @see TitanKey
	 */
	public boolean isPropertyKey();
	
	/**
	 * Checks if this edge type is a relationship type
	 * 
	 * @return true, if this edge type is a relationship type, else false.
	 * @see TitanLabel
	 */
	public boolean isEdgeLabel();
	
}
