
package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;


/**
 * TitanVertex is the basic unit of a {@link TitanGraph}.
 * It extends the functionality provided by Blueprint's {@link Vertex} by helper and convenience methods.
 *
 *
 * @author	Matthias Br&ouml;cheler (http://www.matthiasb.com)
 *
 */
public interface TitanVertex extends TitanElement, Vertex {

    /* ---------------------------------------------------------------
      * Creation and modification methods
      * ---------------------------------------------------------------
      */
    
	/**
	 * Creates a new edge incident on this vertex.
	 * 
	 * Creates and returns a new {@link TitanEdge} of the specified label with this vertex being the outgoing vertex
	 * and the given vertex being the incoming vertex.
	 * 
	 * @param label	label of the edge to be created
	 * @param vertex incoming vertex of the edge to be created
	 * @return 	new edge
	 */
	public TitanEdge addEdge(TitanLabel label, TitanVertex vertex);
	
	/**
     * Creates a new edge incident on this vertex.
     *
     * Creates and returns a new {@link TitanEdge} of the specified label with this vertex being the outgoing vertex
     * and the given vertex being the incoming vertex.
     * <br />
     * Automatically creates the edge label if it does not exist and automatic creation of types is enabled. Otherwise,
     * this method with throw an {@link IllegalArgumentException}.
     *
     * @param label	label of the edge to be created
     * @param vertex incoming vertex of the edge to be created
     * @return 	new edge
	 */
	public TitanEdge addEdge(String label, TitanVertex vertex);
		
	/**
	 * Creates a new property for this vertex and given key with the specified attribute.
	 * 
	 * Creates and returns a new {@link TitanProperty} for the given key on this vertex with the specified
     * object being the attribute.
	 * 
	 * @param key	key of the property to be created
	 * @param attribute	attribute of the property to be created
	 * @return 			New property
	 * @throws	IllegalArgumentException if the attribute does not match the data type of the property key.
	 */
	public TitanProperty addProperty(TitanKey key, Object attribute);

    /**
     * Creates a new property for this vertex and given key with the specified attribute.
     *
     * Creates and returns a new {@link TitanProperty} for the given key on this vertex with the specified
     * object being the attribute.
     * <br />
     * Automatically creates the property key if it does not exist and automatic creation of types is enabled. Otherwise,
     * this method with throw an {@link IllegalArgumentException}.
     *
     * @param key	key of the property to be created
     * @param attribute	attribute of the property to be created
     * @return 			New property
     * @throws	IllegalArgumentException if the attribute does not match the data type of the property key.
     */
	public TitanProperty addProperty(String key, Object attribute);

    /**
     * Add a property with the given key and value to this vertex.
     * If the key is functional, then a possibly existing value is replaced. If it is not functional, then
     * a new property is added.
     *
     * @param key   the string key of the property
     * @param value the object value o the property
     * @see #addProperty(String, Object) 
     */
    public void setProperty(String key, Object value);

    /**
     * Removes all properties with the given key from this vertex.
     *
     * @param key the key of the properties to remove from the element
     * @return the object value (or null) associated with that key prior to removal if the key is functional,
     * or an {@link Iterable} over all values (which may be empty) if it is not functional
     */
    public Object removeProperty(String key);


	
	/* ---------------------------------------------------------------
	 * Incident TitanRelation Access methods
	 * ---------------------------------------------------------------
	 */
	
	/**
	 * Starts a new TitanQuery for this vertex.
	 * 
	 * Initializes and returns a new {@link TitanQuery} centered on this vertex.
	 * 
	 * @return New TitanQuery for this vertex
	 * @see TitanQuery
	 */
	public TitanQuery query();

	/**
	 * Retrieves the attribute value for the only property of the specified property key incident on this vertex.
	 * 
	 * This method call expects that there is at most one property of the specified {@link TitanKey} incident on this vertex.
     * If there could be multiple properties (i.e. for non-functional property keys), then use {@link #getProperties(TitanKey)}
	 * 
	 * @param key key of the property for which to retrieve the attribute value
	 * @return Attribute value of the property with the specified type or null if no such property exists
	 * @throws IllegalArgumentException	if more than one property of the specified key are incident on this vertex.
	 */
	public Object getProperty(TitanKey key);
	
	/**
	 * Retrieves the attribute value for the only property of the specified property key incident on this vertex.
	 * 
	 * This method call expects that there is at most one property of the specified {@link TitanKey} incident on this vertex.
	 * If there could be multiple properties (i.e. for non-functional property keys), then use {@link #getProperties(String)}
     *
	 * @param key key name of the property for which to retrieve the attribute value
	 * @return Attribute value of the property with the specified type or null if no such property exists
	 * @throws IllegalArgumentException	if more than one property of the specified key are incident on this vertex.
	 */
	public Object getProperty(String key);

	/**
	 * Retrieves the attribute value for the only property of the specified property key incident on this vertex and casts
	 * it to the specified {@link Class}.
	 * 
	 * This method call expects that there is at most one property of the specified {@link TitanKey} incident on this vertex.
	 * 
	 * @param key key name of the property for which to retrieve the attribute value
	 * @return Attribute value of the property with the specified type
	 * @throws IllegalArgumentException	if more than one property of the specified key are incident on this vertex.
	 */
	public<O> O getProperty(TitanKey key, Class<O> clazz);
	
	/**
	 * Retrieves the attribute value for the only property of the specified property key incident on this vertex and casts
	 * it to the specified {@link Class}.
	 * 
	 * This method call expects that there is at most one property of the specified {@link TitanKey} incident on this vertex.
	 * 
	 * @param key key of the property for which to retrieve the attribute value
	 * @return Attribute value of the property with the specified type
	 * @throws IllegalArgumentException	if more than one property of the specified key are incident on this vertex.
	 */
	public<O> O getProperty(String key, Class<O> clazz);

    /**
     * Returns an iterable over all properties incident on this vertex.
     *
     * There is no guarantee concerning the order in which the properties are returned. All properties incident
     * on this vertex are returned irrespective of their key.
     *
     * @return {@link Iterable} over all properties incident on this vertex
     */
    public Iterable<TitanProperty> getProperties();

    /***
     * Returns an iterable over all properties of the specified property key incident on this vertex.
     *
     * There is no guarantee concerning the order in which the properties are returned. All returned properties are
     * of the specified key.
     *
     * @param key {@link TitanKey} of the returned properties
     * @return {@link Iterable} over all properties of the specified key incident on this vertex
     */
    public Iterable<TitanProperty> getProperties(TitanKey key);

    /***
     * Returns an iterable over all properties of the specified property key incident on this vertex.
     *
     * There is no guarantee concerning the order in which the properties are returned. All returned properties are
     * of the specified key.
     *
     * @param key key of the returned properties
     * @return {@link Iterable} over all properties of the specified key incident on this vertex
     */
    public Iterable<TitanProperty> getProperties(String key);


	/***
	 * Returns an iterable over all edges of the specified edge label in the given direction incident on this vertex.
	 * 
	 * There is no guarantee concerning the order in which the edges are returned. All returned edges have the given
     * label and the direction of the edge from the perspective of this vertex matches the specified direction.
	 * 
	 * @param labels label of the returned edges
	 * @param d Direction of the returned edges with respect to this vertex
	 * @return {@link Iterable} over all edges with the given label and direction incident on this vertex
	 */
	public Iterable<TitanEdge> getTitanEdges(Direction d, TitanLabel... labels);

    /***
     * Returns an iterable over all edges of the specified edge label in the given direction incident on this vertex.
     *
     * There is no guarantee concerning the order in which the edges are returned. All returned edges have the given
     * label and the direction of the edge from the perspective of this vertex matches the specified direction.
     *
     * @param labels label of the returned edges
     * @param d Direction of the returned edges with respect to this vertex
     * @return {@link Iterable} over all edges with the given label and direction incident on this vertex
     */
	public Iterable<Edge> getEdges(Direction d, String... labels);

	/***
	 * Returns an iterable over all edges incident on this vertex.
	 * 
	 * There is no guarantee concerning the order in which the edges are returned.
	 * 
	 * @return {@link Iterable} over all edges incident on this vertex
	 */
	public Iterable<TitanEdge> getEdges();

	/***
	 * Returns an iterable over all relations incident on this vertex.
	 * 
	 * There is no guarantee concerning the order in which the relations are returned. Note, that this
     * method potentially returns both {@link TitanEdge} and {@link TitanProperty}.
	 * 
	 * @return {@link Iterable} over all properties and edges incident on this vertex.
	 */
	public Iterable<TitanRelation> getRelations();

	/***
	 * Returns the number of edges incident on this vertex.
	 * 
	 * Returns the total number of edges irrespective of label and direction.
	 * Note, that loop edges, i.e. edges with identical in- and outgoing vertex, are counted twice.
	 * 
	 * @return The number of edges incident on this vertex.
	 */
	public long getEdgeCount();
	
	/***
	 * Returns the number of properties incident on this vertex.
	 * 
	 * Returns the total number of properties irrespective of key.
	 * 
	 * @return The number of properties incident on this vertex.
	 */
	public long getPropertyCount();
	
	/**
	 * Checks whether this vertex has at least one incident edge.
	 * In other words, it returns getEdgeCount()>0, but might be implemented more efficiently.
	 * 
	 * @return true, if this vertex has at least one incident edge, else false
	 */
	public boolean isConnected();

}
