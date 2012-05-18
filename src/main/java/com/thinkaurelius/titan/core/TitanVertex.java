
package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;


/**
 * {@link TitanVertex} is one of the two basic entities of a graph - the other one being {@link TitanRelation}.
 * A node, also called vertex, can represent an object, individual, or other identifiable entity which have properties ({@link TitanProperty}
 * and engages in relationships ({@link TitanEdge}). Nodes are connected to one other via relationships.
 * For more information on the mathematical definition of a graph and the concept of nodes see 
 * <a href="http://en.wikipedia.org/wiki/Graph_%28mathematics%29">Graph Definition</a>.
 * 
 * Nodes are the first thing to be created in a graph in the context of a {@link TitanTransaction}. Nodes have incident {@link TitanRelation}s which
 * are either {@link TitanEdge}s to other nodes or {@link TitanProperty}es defining attributes of the node such as unique ids.
 * 
 * @see    TitanRelation
 * @see TitanEdge
 * @see TitanProperty
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface TitanVertex extends TitanElement, Vertex {

	
	/* ---------------------------------------------------------------
	 * Incident TitanRelation Access methods
	 * ---------------------------------------------------------------
	 */
	
	/**
	 * Creates a new relationship incident on this node.
	 * 
	 * Creates and returns a new {@link TitanEdge} of the specified type with this node being the start
	 * node and the given node being the end node. 
	 * Hence, the created relationship is binary. Hyperedges can only be created through {@link TitanTransaction}s.
	 * 
	 * @param label	type of the relationship to be created
	 * @param vertex		end point of the relationship to be created
	 * @return 			New relationship of specified type
	 */
	public TitanEdge addEdge(TitanLabel label, TitanVertex vertex);
	
	/**
	 * Creates a new relationship incident on this node.
	 * 
	 * Creates and returns a new {@link TitanEdge} of the specified type with this node being the start
	 * node and the given node being the end node. 
	 * Hence, the created relationship is binary. Hyperedges can only be created through {@link TitanTransaction}s.
	 * 
	 * @param label	name of the relationship to be created
	 * @param vertex		end point of the relationship to be created
	 * @return 			New relationship of specified type
	 * @throws	IllegalArgumentException if name of the relationship type is unknown, i.e. a relationship type with said name has not yet been created.
	 */
	public TitanEdge addEdge(String label, TitanVertex vertex);
		
	/**
	 * Creates a new property for this node with the specified attribute
	 * 
	 * Creates and returns a new {@link TitanProperty} of the specified type with this node being the start
	 * node and the given object being the attribute.
	 * 
	 * @param key	type of the property to be created
	 * @param attribute	attribute of the property to be created
	 * @return 			New property of specified type
	 * @throws	IllegalArgumentException if the attribute does not match the data type of the given property type.
	 */
	public TitanProperty addProperty(TitanKey key, Object attribute);
	
	/**
	 * Creates a new property for this node with the specified attribute
	 * 
	 * Creates and returns a new {@link TitanProperty} of the specified type with this node being the start
	 * node and the given object being the attribute.
	 * 
	 * @param key	name of the property to be created
	 * @param attribute	attribute of the property to be created
	 * @return 			New property of specified type
	 * @throws	IllegalArgumentException if the attribute does not match the data type of the given property type.
	 * @throws	IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
	 */
	public TitanProperty addProperty(String key, Object attribute);

    /**
     * Add a property with the given key and value to this vertex.
     * If the key is functional, then a possibly existing value is replaced. If it is not functional, then
     * a new property is added.
     *
     * @param key   the string key of the property
     * @param value the object value o the property
     */
    public void setProperty(String key, Object value);

    /**
     * Removes all properties with the given key from this vertex.
     *
     * @param key the key of the properties to remove from the element
     * @return the object value (or null) associated with that key prior to removal if the key is functional,
     * or an iterable over all values (which may be empty) if it is not functional
     */
    public Object removeProperty(String key);


	
	/* ---------------------------------------------------------------
	 * Incident TitanRelation Access methods
	 * ---------------------------------------------------------------
	 */
	
	/**
	 * Starts a new TitanQuery for this node.
	 * 
	 * Initializes and returns a new {@link TitanQuery} centered on this node.
	 * 
	 * @return New TitanQuery for this node
	 * @see TitanQuery
	 */
	public TitanQuery query();

	/**
	 * Retrieves the attribute value for the only property of the specified property type incident on this node.
	 * 
	 * This method call expects that there is at most one property of the specified {@link TitanKey} incident on this node.
	 * 
	 * @param key TitanProperty type of the property for which to retrieve the attribute value
	 * @return Attribute value of the property with the specified type or null if no such property exists
	 * @throws IllegalArgumentException	if more than one property of the specified type are incident on this node.
	 */
	public Object getProperty(TitanKey key);
	
	/**
	 * Retrieves the attribute value for the only property of the specified property type incident on this node.
	 * 
	 * This method call expects that there is at most one property of the specified {@link TitanKey} incident on this node.
	 * 
	 * @param key TitanProperty type name of the property for which to retrieve the attribute value
	 * @return Attribute value of the property with the specified type or null if no such property exists
	 * @throws IllegalArgumentException	if more than one property of the specified type are incident on this node.
	 * @throws IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
	 */
	public Object getProperty(String key);

	/**
	 * Retrieves the attribute value for the only property of the specified property type incident on this node and casts
	 * it to the specified {@link Class}.
	 * 
	 * This method call expects that there is at most one property of the specified {@link TitanKey} incident on this node.
	 * 
	 * @param key TitanProperty type name of the property for which to retrieve the attribute value
	 * @return Attribute value of the property with the specified type
	 * @throws IllegalArgumentException	if more than one property of the specified type are incident on this node.
	 */
	public<O> O getProperty(TitanKey key, Class<O> clazz);
	
	/**
	 * Retrieves the attribute value for the only property of the specified property type incident on this node and casts
	 * it to the specified {@link Class}.
	 * 
	 * This method call expects that there is at most one property of the specified {@link TitanKey} incident on this node.
	 * 
	 * @param key TitanProperty type of the property for which to retrieve the attribute value
	 * @return Attribute value of the property with the specified type
	 * @throws IllegalArgumentException	if more than one property of the specified type are incident on this node.
	 * @throws IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
	 */
	public<O> O getProperty(String key, Class<O> clazz);

    /***
     * Returns an iterable over all properties incident on this node.
     *
     * There is no guarantee concerning the order in which the properties are returned. All properties incident
     * on this node are returned irrespective of type.
     *
     * @return Iterable over all properties incident on this node
     */
    public Iterable<TitanProperty> getProperties();

    /***
     * Returns an iterable over all properties of the specified property type incident on this node.
     *
     * There is no guarantee concerning the order in which the properties are returned. All returned properties are
     * of the specified type.
     *
     * @param key TitanKey of the returned properties
     * @return Iterable over all properties of the specified type incident on this node
     */
    public Iterable<TitanProperty> getProperties(TitanKey key);

    /***
     * Returns an iterable over all properties of the specified property type incident on this node.
     *
     * There is no guarantee concerning the order in which the properties are returned. All returned properties are
     * of the specified type.
     *
     * @param key TitanKey name of the returned properties
     * @return Iterable over all properties of the specified type incident on this node
     * @throws IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
     */
    public Iterable<TitanProperty> getProperties(String key);


	/***
	 * Returns an iterable over all relationships of the specified relationship type in the given direction incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the relationships are returned. All returned relationships are 
	 * of the specified type and the direction of the relationship from the perspective of this vertex matches the specified
	 * direction.
	 * 
	 * @param labels RelationType of the returned relationships
	 * @param d Direction of the returned relationships with respect to this node
	 * @return Iterable over all relationships of the specified type in the given direction incident on this node
	 */
	public Iterable<TitanEdge> getTitanEdges(Direction d, TitanLabel... labels);

	/***
	 * Returns an iterable over all relationships of the specified relationship type in the given direction incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the relationships are returned. All returned relationships are 
	 * of the specified type and the direction of the relationship from the perspective of this vertex matches the specified
	 * direction.
	 * 
	 * @param labels RelationType name of the returned relationships
	 * @param d Direction of the returned relationships with respect to this node
	 * @return Iterable over all relationships of the specified type in the given direction incident on this node
	 * @throws IllegalArgumentException if name of the relationship type is unknown, i.e. a relationship type 
	 * with said name has not yet been created.
	 */
	public Iterable<Edge> getEdges(Direction d, String... labels);

	/***
	 * Returns an iterable over all relationships incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the relationships are returned. All incident relationships
	 * are returned irrespective their type or direction.
	 * 
	 * @return Iterable over all relationships incident on this node
	 */
	public Iterable<TitanEdge> getEdges();

	/***
	 * Returns an iterable over all edges incident on this node.
	 * 
	 * There is no guarantee concerning the order in which the edges are returned. All edges incident
	 * on this node, meaning both relationships and properties, are returned irrespective of type and direction.
	 * 
	 * @return Iterable over all properties of the specified type incident on this node
	 */
	public Iterable<TitanRelation> getRelations();

	/***
	 * Returns the number of relationships incident on this node.
	 * 
	 * Returns the total number of relationships irrespective of type and direction.
	 * Note, that self-loop relationships, i.e. relationships with identical start and end node, might
	 * get counted twice depending on implementation.
	 * 
	 * @return The number of relationships incident on this node.
	 */
	public long getEdgeCount();
	
	/***
	 * Returns the number of properties incident on this node.
	 * 
	 * Returns the total number of properties irrespective of type and direction.
	 * 
	 * @return The number of properties incident on this node.
	 */
	public long getPropertyCount();
	
	/**
	 * Checks whether this node has at least one incident relationship.
	 * In other words, it returns getEdgeCount()>0, but might be implemented more efficiently.
	 * 
	 * @return true, if this node has at least one incident relationship, else false
	 */
	public boolean isConnected();
	
}
