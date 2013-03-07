
package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;


/**
 * TitanVertex is the basic unit of a {@link TitanGraph}.
 * It extends the functionality provided by Blueprint's {@link Vertex} by helper and convenience methods.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 */
public interface TitanVertex extends TitanElement, Vertex {

    /* ---------------------------------------------------------------
      * Creation and modification methods
      * ---------------------------------------------------------------
      */

    /**
     * Creates a new edge incident on this vertex.
     * <p/>
     * Creates and returns a new {@link TitanEdge} of the specified label with this vertex being the outgoing vertex
     * and the given vertex being the incoming vertex.
     *
     * @param label  label of the edge to be created
     * @param vertex incoming vertex of the edge to be created
     * @return new edge
     */
    public TitanEdge addEdge(TitanLabel label, TitanVertex vertex);

    /**
     * Creates a new edge incident on this vertex.
     * <p/>
     * Creates and returns a new {@link TitanEdge} of the specified label with this vertex being the outgoing vertex
     * and the given vertex being the incoming vertex.
     * <br />
     * Automatically creates the edge label if it does not exist and automatic creation of types is enabled. Otherwise,
     * this method with throw an {@link IllegalArgumentException}.
     *
     * @param label  label of the edge to be created
     * @param vertex incoming vertex of the edge to be created
     * @return new edge
     */
    public TitanEdge addEdge(String label, TitanVertex vertex);

    /**
     * Creates a new property for this vertex and given key with the specified attribute.
     * <p/>
     * Creates and returns a new {@link TitanProperty} for the given key on this vertex with the specified
     * object being the attribute.
     *
     * @param key       key of the property to be created
     * @param attribute attribute of the property to be created
     * @return New property
     * @throws IllegalArgumentException if the attribute does not match the data type of the property key.
     */
    public TitanProperty addProperty(TitanKey key, Object attribute);

    /**
     * Creates a new property for this vertex and given key with the specified attribute.
     * <p/>
     * Creates and returns a new {@link TitanProperty} for the given key on this vertex with the specified
     * object being the attribute.
     * <br />
     * Automatically creates the property key if it does not exist and automatic creation of types is enabled. Otherwise,
     * this method with throw an {@link IllegalArgumentException}.
     *
     * @param key       key of the property to be created
     * @param attribute attribute of the property to be created
     * @return New property
     * @throws IllegalArgumentException if the attribute does not match the data type of the property key.
     */
    public TitanProperty addProperty(String key, Object attribute);

	/* ---------------------------------------------------------------
     * Incident TitanRelation Access methods
	 * ---------------------------------------------------------------
	 */

    /**
     * Starts a new TitanQuery for this vertex.
     * <p/>
     * Initializes and returns a new {@link TitanVertexQuery} centered on this vertex.
     *
     * @return New TitanQuery for this vertex
     * @see TitanVertexQuery
     */
    public TitanVertexQuery query();


    /**
     * Returns an iterable over all properties incident on this vertex.
     * <p/>
     * There is no guarantee concerning the order in which the properties are returned. All properties incident
     * on this vertex are returned irrespective of their key.
     *
     * @return {@link Iterable} over all properties incident on this vertex
     */
    public Iterable<TitanProperty> getProperties();

    /**
     * Returns an iterable over all properties of the specified property key incident on this vertex.
     * <p/>
     * There is no guarantee concerning the order in which the properties are returned. All returned properties are
     * of the specified key.
     *
     * @param key {@link TitanKey} of the returned properties
     * @return {@link Iterable} over all properties of the specified key incident on this vertex
     */
    public Iterable<TitanProperty> getProperties(TitanKey key);

    /**
     * Returns an iterable over all properties of the specified property key incident on this vertex.
     * <p/>
     * There is no guarantee concerning the order in which the properties are returned. All returned properties are
     * of the specified key.
     *
     * @param key key of the returned properties
     * @return {@link Iterable} over all properties of the specified key incident on this vertex
     */
    public Iterable<TitanProperty> getProperties(String key);


    /**
     * Returns an iterable over all edges of the specified edge label in the given direction incident on this vertex.
     * <p/>
     * There is no guarantee concerning the order in which the edges are returned. All returned edges have the given
     * label and the direction of the edge from the perspective of this vertex matches the specified direction.
     *
     * @param labels label of the returned edges
     * @param d      Direction of the returned edges with respect to this vertex
     * @return {@link Iterable} over all edges with the given label and direction incident on this vertex
     */
    public Iterable<TitanEdge> getTitanEdges(Direction d, TitanLabel... labels);

    /**
     * Returns an iterable over all edges of the specified edge label in the given direction incident on this vertex.
     * <p/>
     * There is no guarantee concerning the order in which the edges are returned. All returned edges have the given
     * label and the direction of the edge from the perspective of this vertex matches the specified direction.
     *
     * @param labels label of the returned edges
     * @param d      Direction of the returned edges with respect to this vertex
     * @return {@link Iterable} over all edges with the given label and direction incident on this vertex
     */
    public Iterable<Edge> getEdges(Direction d, String... labels);

    /**
     * Returns an iterable over all edges incident on this vertex.
     * <p/>
     * There is no guarantee concerning the order in which the edges are returned.
     *
     * @return {@link Iterable} over all edges incident on this vertex
     */
    public Iterable<TitanEdge> getEdges();

    /**
     * Returns an iterable over all relations incident on this vertex.
     * <p/>
     * There is no guarantee concerning the order in which the relations are returned. Note, that this
     * method potentially returns both {@link TitanEdge} and {@link TitanProperty}.
     *
     * @return {@link Iterable} over all properties and edges incident on this vertex.
     */
    public Iterable<TitanRelation> getRelations();

    /**
     * Returns the number of edges incident on this vertex.
     * <p/>
     * Returns the total number of edges irrespective of label and direction.
     * Note, that loop edges, i.e. edges with identical in- and outgoing vertex, are counted twice.
     *
     * @return The number of edges incident on this vertex.
     */
    public long getEdgeCount();

    /**
     * Returns the number of properties incident on this vertex.
     * <p/>
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


    /**
     * Checks whether this entity has been loaded into the current transaction and modified.
     *
     * @return True, has been loaded and modified, else false.
     */
    public boolean isModified();

}
