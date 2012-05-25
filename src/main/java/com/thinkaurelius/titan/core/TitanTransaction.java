
package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;

/***
 * A graph transaction is the interface to interact with a graph database.
 * 
 * All node and edge retrievals are channeled by a graph transaction which bundles all such retrievals, creations and
 * deletions into one transaction. A graph transaction is analogous to a 
 * <a href="http://en.wikipedia.org/wiki/Database_transaction">database transaction</a>.
 * The isolation level and <a href="http://en.wikipedia.org/wiki/ACID">ACID support</a> are configured through the storage
 * backend, meaning whatever level of isolation is supported by the storage backend is mirrored by a graph transaction.
 * 
 * A graph transaction supports:
 * <ul>
 * <li>Creating nodes, properties and relationships</li>
 * <li>Creating edge types</li>
 * <li>Index-based retrieval of nodes</li>
 * <li>Querying edges and neighborhoods</li>
 * <li>Aborting and committing transaction</li>
 * </ul>
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface TitanTransaction extends TransactionalGraph {

	/***
	 * Creates a new node in the graph.
	 * 
	 * @return New node in the graph created in the context of this transaction.
	 */
	public TitanVertex addVertex();
	
	/**
	 * Creates a new relationship connecting the specified nodes.
	 * 
	 * Creates and returns a new {@link TitanEdge} of the specified type connecting the nodes in the order
	 * specified. For a binary relationship, the first node is the start node and the second node is the end node.
	 * For hyperedges the order is adopted from the relationship type definition.
	 * 
	 * @param label	type of the relationship to be created
     * @param outVertex		Starting node of the relationship
     * @param inVertex       Head node of the relationship
	 * @return 			New relationship of specified type
	 */
	public TitanEdge addEdge(TitanVertex outVertex, TitanVertex inVertex, TitanLabel label);
	
	/**
	 * Creates a new relationship connecting the specified nodes.
	 * 
	 * Creates and returns a new {@link TitanEdge} of the specified type connecting the nodes in the order
	 * specified. For a binary relationship, the first node is the start node and the second node is the end node.
	 * For hyperedges the order is adopted from the relationship type definition.
	 * 
	 * @param label	type name of the relationship to be created
	 * @param outVertex		Starting node of the relationship
     * @param inVertex       Head node of the relationship
	 * @return 			New relationship of specified type
	 * @throws	IllegalArgumentException if name of the relationship type is unknown, i.e. a relationship type with said name has not yet been created.
	 */
	public TitanEdge addEdge(TitanVertex outVertex, TitanVertex inVertex, String label);
							
	/**
	 * Creates a new property for the given node with the specified attribute.
	 * 
	 * Creates and returns a new {@link TitanProperty} of the specified property type with the node being the start
	 * node and the given object being the attribute.
	 * 
	 * @param propType	type of the property to be created
	 * @param vertex		TitanVertex for which to create the property
	 * @param attribute	attribute of the property to be created
	 * @return 			New property of specified type
	 * @throws	IllegalArgumentException if the attribute does not match the data type of the given property type.
	 */
	public TitanProperty addProperty(TitanVertex vertex, TitanKey propType, Object attribute);
	
	/**
	 * Creates a new property for the given node with the specified attribute.
	 * 
	 * Creates and returns a new {@link TitanProperty} of the specified property type with the node being the start
	 * node and the given object being the attribute.
	 * 
	 * @param propType	type name of the property to be created
	 * @param vertex		TitanVertex for which to create the property
	 * @param attribute	attribute of the property to be created
	 * @return 			New property of specified type
	 * @throws	IllegalArgumentException if the attribute does not match the data type of the given property type.
	 * @throws	IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
	 */
	public TitanProperty addProperty(TitanVertex vertex, String propType, Object attribute);
	
	/**
	 * Retrieves the node for the specified node id.
	 * 
	 * This method assumes that a node with the given id exists, otherwise it throws an exception
	 * 
	 * @param id ID of the node to retrieve
	 * @return TitanVertex with the given id
	 * @throws com.thinkaurelius.titan.exceptions.InvalidNodeException if id does not exist in the database
	 * @see #containsVertex
	 */
	public TitanVertex getVertex(long id);

	/**
	 * Checks whether a node with the specified id exists in the graph database.
	 * 
	 * @param nodeid TitanVertex ID
	 * @return true, if a node with that id exists, else false
	 */
	public boolean containsVertex(long nodeid);

	/**
	 * Creates a new edge query for the node with the specified id.
	 * 
	 * @param nodeid Id of the node for which to create a new edge query
	 * @return An {@link TitanQuery} constructor for the specified node id
	 * @throws com.thinkaurelius.titan.exceptions.InvalidNodeException if id does not exist in the database
	 * @see TitanQuery
	 * @see TitanVertex#query()
	 */
	public TitanQuery query(long nodeid);
	
	/**
	 * Retrieves the node whose attribute value for the specified property type matches the given key.
	 * 
	 * This method assumes that the property type is keyed, i.e. that each attribute for a property of
	 * such type is associated with at most one node. In other words, a node can be uniquely identified
	 * given the property type and the attribute (also called the <b>key</b>).
	 * 
	 * @param key TitanProperty type which is keyed
	 * @param value A property attribute
	 * @return The node uniquely identified by the key and property type, or null if no such exists
	 * @throws IllegalArgumentException if the property type is not keyed
	 * @see TitanKey#isUnique()
	 */
	public TitanVertex getVertex(TitanKey key, Object value);
	
	/**
	 * Retrieves the node whose attribute value for the specified property type name matches the given key.
	 *  
	 * @param key Name of the property type which is keyed
	 * @param value A property attribute
	 * @return The node uniquely identified by the key and property type, or null if no such exists
	 * @throws IllegalArgumentException if the property type is not keyed
	 * @see TitanKey#isUnique()
	 * @see #getVertex
	 * @throws	IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
	 * @throws	IllegalArgumentException if the property type is not keyed.
	 */
	public TitanVertex getVertex(String key, Object value);

	/**
	 * Retrieves all nodes which have an incident property of the given type with the specified attribute.
	 * 
	 * The given property type must have an hasIndex defined for this retrieval to succeed.
	 * 
	 * @param key TitanProperty type for which to retrieve nodes
	 * @param attribute Attribute value
	 * @return	All nodes which have an incident property of the given type with the specified attribute.
	 * @throws	IllegalArgumentException if the property type is not indexed.
	 */
	public Iterable<TitanVertex> getVertices(TitanKey key, Object attribute);
	
	/**
	 * Retrieves all nodes which have an incident property of the given type with the specified attribute.
	 * 
	 * The given property type must have an hasIndex defined for this retrieval to succeed.
	 * 
	 * @param key TitanProperty type name for which to retrieve nodes
	 * @param attribute Attribute value
	 * @return	All nodes which have an incident property of the given type with the specified attribute.
	 * @throws	IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
	 * @throws	IllegalArgumentException if the property type is not indexed.
	 */
	public Iterable<Vertex> getVertices(String key, Object attribute);

	/**
	 * Returns an iterable over all nodes loaded in the current transaction.
	 * 
	 * The order in which the nodes are returned is arbitrary. Note, that this method only returns those
     * nodes that have been previously loaded to avoid excessive memory loads.
	 * 
	 * For in-memory graph transactions ({@link com.thinkaurelius.titan.configuration.InMemoryGraphDatabase})
     * this method returns the entire node set of the database.
	 * 
	 * @return An iterable over all nodes in the transaction
	 */
	public Iterable<Vertex> getVertices();
	
	/**
	 * Returns an iterable over all relationships for all nodes loaded in the current transaction.
	 * 
	 * The order in which the relationships are returned is arbitrary. Note, that this method only returns those
     * relationships for which at least one node that have been previously loaded to avoid excessive memory loads.
     * This method might still require significant disk access to retrieve all of those.
	 * 
	 * For in-memory graph transactions ({@link com.thinkaurelius.titan.configuration.InMemoryGraphDatabase})
     * this method returns the entire relationship set of the database.
     *
	 * @return An iterable over all relationships in the transaction
	 */
	public Iterable<Edge> getEdges();

	/**
	 * Checks whether an edge type with the specified name exists.
	 * 
	 * @param name Name of the edge type
	 * @return true, if an edge type with the given name exists, else false
	 */
	public boolean containsType(String name);
	
	/**
	 * Returns the edge type with the given name.
	 * Note, that edge type names must be unique.
	 * 
	 * @param name Name of the edge type to return
	 * @return The edge type with the given name, or null if such does not exist
	 */
	public TitanType getType(String name);

	/**
	 * Returns the property type with the given name.
	 * 
	 * @param name Name of the property type to return
	 * @return The property type with the given name
	 * @throws IllegalArgumentException if a property type with the given name does not exist or if the edge
	 * type with the given name is not a property type
	 */
	public TitanKey getPropertyKey(String name);

	/**
	 * Returns the relationship type with the given name.
	 * 
	 * @param name Name of the relationship type to return
	 * @return The relationship type with the given name
	 * @throws IllegalArgumentException if a relationship type with the given name does not exist or if the edge
	 * type with the given name is not a relationship type
	 */
	public TitanLabel getEdgeLabel(String name);
	
	/**
	 * Returns a new edge type maker to create edge types.
	 * 
	 * The edge type constructed with this maker will be created in the context of this transaction.
	 * 
	 * @return An edge type maker linked to this transaction.
	 * @see TypeMaker
	 * @see TitanType
	 */
	public TypeMaker makeType();

    /**
     * Commits the current state of the transaction, but continues to keep the transaction
     * open for further modifications or read operations.
     * 
     * In contrast to {@link #commit()}, which immediately closes the transaction after persisting the changes
     * and releases any locks it may hold, rolling commit keeps the transaction open so further processing can happen.
     * This has the advantage that the in-memory state is not lost and the disadvantage that the transaction will
     * continue and locks will be held.
     *
     * @throws com.thinkaurelius.titan.exceptions.GraphStorageException if an error arises during persistence
     */
    public void rollingCommit();
	
	/**
	 * Commits and closes the transaction.
	 * 
	 * The call releases data structures if possible. All handles (e.g. node handles) retrieved
	 * through this transaction are stale after the transaction closes and should no longer be used.
	 * 
	 * @throws com.thinkaurelius.titan.exceptions.GraphStorageException if an error arises during persistence
	 */
	public void commit();

	/**
	 * Aborts and closes the transaction.
	 * 
	 * The call releases data structures if possible. All handles (e.g. node handles) retrieved
	 * through this transaction are stale after the transaction closes and should no longer be used.
	 * 
	 * @throws com.thinkaurelius.titan.exceptions.GraphStorageException if an error arises when releasing the transaction handle
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
	 * Returns the configuration used by this graph transaction
	 * 
	 * @return The configuration for this transaction
	 */
	public TransactionConfig getTxConfiguration();
	
	/**
	 * Checks whether any changes to the graph database have been made in this transaction.
	 * 
	 * A modification may be an edge or node update, addition, or deletion.
	 * 
	 * @return true, if the transaction contains updates, else false.
	 */
	public boolean hasModifications();
	
}
