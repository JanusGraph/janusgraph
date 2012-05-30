
package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;

/***
 * TitanTransaction defines a transactional context for a {@link TitanGraph}. Since TitanGraph is a transactional graph
 * database, all interactions with the graph are mitigated by a TitanTransaction.
 * 
 * All vertex and edge retrievals are channeled by a graph transaction which bundles all such retrievals, creations and
 * deletions into one transaction. A graph transaction is analogous to a 
 * <a href="http://en.wikipedia.org/wiki/Database_transaction">database transaction</a>.
 * The isolation level and <a href="http://en.wikipedia.org/wiki/ACID">ACID support</a> are configured through the storage
 * backend, meaning whatever level of isolation is supported by the storage backend is mirrored by a graph transaction.
 * 
 * A graph transaction supports:
 * <ul>
 * <li>Creating vertices, properties and edges</li>
 * <li>Creating types</li>
 * <li>Index-based retrieval of vertices</li>
 * <li>Querying edges and vertices</li>
 * <li>Aborting and committing transaction</li>
 * </ul>
 *
 * @author	Matthias Br&ouml;cheler (http://www.matthiasb.com)
 *
 */
public interface TitanTransaction extends TransactionalGraph, KeyIndexableGraph {

	/***
	 * Creates a new vertex in the graph.
	 * 
	 * @return New vertex in the graph created in the context of this transaction.
	 */
	public TitanVertex addVertex();
	
	/**
	 * Creates a new edge connecting the specified vertices.
	 * 
	 * Creates and returns a new {@link TitanEdge} with given label connecting the vertices in the order
	 * specified.
	 * 
	 * @param label	label of the edge to be created
     * @param outVertex	outgoing vertex of the edge
     * @param inVertex  incoming vertex of the edge
	 * @return 	new edge
	 */
	public TitanEdge addEdge(TitanVertex outVertex, TitanVertex inVertex, TitanLabel label);

    /**
     * Creates a new edge connecting the specified vertices.
     *
     * Creates and returns a new {@link TitanEdge} with given label connecting the vertices in the order
     * specified.
     * <br />
     * Automatically creates the edge label if it does not exist and automatic creation of types is enabled. Otherwise,
     * this method with throw an {@link IllegalArgumentException}.
     *
     * @param label	label of the edge to be created
     * @param outVertex	outgoing vertex of the edge
     * @param inVertex  incoming vertex of the edge
     * @return 	new edge
     */
	public TitanEdge addEdge(TitanVertex outVertex, TitanVertex inVertex, String label);
							
	/**
	 * Creates a new property for the given vertex and key with the specified attribute.
	 * 
	 * Creates and returns a new {@link TitanProperty} with specified property key and the given object being the attribute.
	 * 
	 * @param key	key of the property to be created
	 * @param vertex vertex for which to create the property
	 * @param attribute	attribute of the property to be created
	 * @return 	new property
	 * @throws	IllegalArgumentException if the attribute does not match the data type of the given property key.
	 */
	public TitanProperty addProperty(TitanVertex vertex, TitanKey key, Object attribute);

    /**
     * Creates a new property for the given vertex and key with the specified attribute.
     *
     * Creates and returns a new {@link TitanProperty} with specified property key and the given object being the attribute.
     * <br />
     * Automatically creates the property key if it does not exist and automatic creation of types is enabled. Otherwise,
     * this method with throw an {@link IllegalArgumentException}.
     *
     * @param key	key of the property to be created
     * @param vertex vertex for which to create the property
     * @param attribute	attribute of the property to be created
     * @return 	new property
     * @throws	IllegalArgumentException if the attribute does not match the data type of the given property key.
     */
	public TitanProperty addProperty(TitanVertex vertex, String key, Object attribute);
	
	/**
	 * Retrieves the vertex for the specified id.
	 *
	 * @param id id of the vertex to retrieve
	 * @return vertex with the given id if it exists, else null
	 * @see #containsVertex
	 */
	public TitanVertex getVertex(long id);

	/**
	 * Checks whether a vertex with the specified id exists in the graph database.
	 * 
	 * @param vertexid vertex id
	 * @return true, if a vertex with that id exists, else false
	 */
	public boolean containsVertex(long vertexid);

	/**
	 * Creates a new query for the vertex identified by the given id.
	 * 
	 * @param vertexid Id of the vertex for which to create a new query
	 * @return An {@link TitanQuery}
	 * @throws InvalidElementException if id does not exist in the database
	 * @see TitanQuery
	 */
	public TitanQuery query(long vertexid);
	
	/**
	 * Retrieves the vertex whose attribute value for the specified property key matches the given value.
	 * 
	 * This method assumes that the property key is unique, i.e. that each attribute for a property of
	 * this key is associated with at most one vertex. In other words, a vertex can be uniquely identified
	 * given the property key and the attribute
	 * 
	 * @param key property key
	 * @param value value to retrieve vertex for
	 * @return The vertex uniquely identified by the property key and value, or null if no such exists
	 * @throws IllegalArgumentException if the property key is not unique
	 * @see TitanKey#isUnique()
	 */
	public TitanVertex getVertex(TitanKey key, Object value);

    /**
     * Retrieves the vertex whose attribute value for the specified property key matches the given value.
     *
     * This method assumes that the property key is unique, i.e. that each attribute for a property of
     * this key is associated with at most one vertex. In other words, a vertex can be uniquely identified
     * given the property key and the attribute
     *
     * @param key property key
     * @param value value to retrieve vertex for
     * @return The vertex uniquely identified by the property key and value, or null if no such exists
     * @throws IllegalArgumentException if the property key is not unique
     * @see TitanKey#isUnique()
     */
	public TitanVertex getVertex(String key, Object value);

	/**
	 * Retrieves all vertices which have an incident property of the given key with the specified value.
	 * 
	 * The given property key must be indexed. In this regard, it violates the Blueprints contract which
     * requires iterating over all vertices and filtering based on the attribute. However, Titan does not
     * support vertex iteration.
	 * 
	 * @param key property key
	 * @param attribute attribute value
	 * @return	All vertices which have an incident property of the given key with the specified value.
	 * @throws	IllegalArgumentException if the property key is not indexed.
     * @see com.thinkaurelius.titan.core.TitanKey#hasIndex()
	 */
	public Iterable<TitanVertex> getVertices(TitanKey key, Object attribute);

    /**
     * Retrieves all vertices which have an incident property of the given key with the specified value.
     *
     * The given property key must be indexed. In this regard, it violates the Blueprints contract which
     * requires iterating over all vertices and filtering based on the attribute. However, Titan does not
     * support vertex iteration.
     *
     * @param key property key
     * @param attribute attribute value
     * @return	All vertices which have an incident property of the given key with the specified value.
     * @throws	IllegalArgumentException if the property key is not indexed.
     * @see com.thinkaurelius.titan.core.TitanKey#hasIndex()
     */
	public Iterable<Vertex> getVertices(String key, Object attribute);

	/**
	 * Returns an iterable over all vertices loaded in the current transaction.
	 * 
	 * The order in which the vertices are returned is arbitrary. Note, that this method only returns those
     * vertices that have been PREVIOUSLY loaded to avoid excessive memory loads. Hence, this method violates
     * the contract of {@link com.tinkerpop.blueprints.Graph#getVertices()} unless all vertices are currently
     * loaded in the transaction.
	 * 
	 * @return An iterable over all vertices in the transaction
	 */
	public Iterable<Vertex> getVertices();
	
	/**
	 * Returns an iterable over all edges incident on the vertices loaded in the current transaction.
	 * 
	 * The order in which the edges are returned is arbitrary. Note, that this method only returns those
     * edges for which at least one vertex that have been previously loaded to avoid excessive memory loads.
     * This method might still require significant disk access to retrieve all of these edges. Hence, this method violates
     * the contract of {@link com.tinkerpop.blueprints.Graph#getEdges()}} unless all edges are connected to a vertex
     * currently loaded in this transaction.
     *
	 * @return An iterable over all edges in the transaction
	 */
	public Iterable<Edge> getEdges();

	/**
	 * Checks whether a type with the specified name exists.
	 * 
	 * @param name name of the type
	 * @return true, if a type with the given name exists, else false
	 */
	public boolean containsType(String name);
	
	/**
	 * Returns the type with the given name.
	 * Note, that type names must be unique.
	 * 
	 * @param name name of the type to return
	 * @return The type with the given name, or null if such does not exist
     * @see TitanType
	 */
	public TitanType getType(String name);

	/**
	 * Returns the property key with the given name.
	 * 
	 * @param name name of the property key to return
	 * @return the property key with the given name
	 * @throws IllegalArgumentException if a property key with the given name does not exist or if the
	 * type with the given name is not a property key
     * @see TitanKey
	 */
	public TitanKey getPropertyKey(String name);

	/**
	 * Returns the edge label with the given name.
	 * 
	 * @param name name of the edge label to return
	 * @return the edge label with the given name
	 * @throws IllegalArgumentException if an edge label with the given name does not exist or if the
	 * type with the given name is not an edge label
     * @see TitanLabel
	 */
	public TitanLabel getEdgeLabel(String name);
	
	/**
	 * Returns a new {@link TypeMaker} instance to create types.
	 * 
	 * The type constructed with this maker will be created in the context of this transaction.
	 * 
	 * @return a type maker linked to this transaction.
	 * @see TypeMaker
	 * @see TitanType
	 */
	public TypeMaker makeType();

	/**
	 * Commits and closes the transaction.
     *
     * Will attempt to persist all modifications which may result in exceptions in case of persistence failures or
     * lock contention.
	 * <br />
	 * The call releases data structures if possible. All element references (e.g. vertex objects) retrieved
	 * through this transaction are stale after the transaction closes and should no longer be used.
	 *
	 * @throws GraphStorageException if an error arises during persistence
	 */
	public void commit();

	/**
	 * Aborts and closes the transaction. Will discard all modifications.
	 *
     * The call releases data structures if possible. All element references (e.g. vertex objects) retrieved
     * through this transaction are stale after the transaction closes and should no longer be used.
	 * 
	 * @throws GraphStorageException if an error arises when releasing the transaction handle
	 */
	public void abort();
	
	/**
	 * Checks whether the transaction is still open.
	 * 
	 * @return true, when the transaction is open, else false
	 */
	public boolean isOpen();
	
	/**
	 * Checks whether the transaction has been closed.
	 * 
	 * @return true, if the transaction has been closed, else false
	 */
	public boolean isClosed();
	
	/**
	 * Checks whether any changes to the graph database have been made in this transaction.
	 * 
	 * A modification may be an edge or vertex update, addition, or deletion.
	 * 
	 * @return true, if the transaction contains updates, else false.
	 */
	public boolean hasModifications();
	
}
