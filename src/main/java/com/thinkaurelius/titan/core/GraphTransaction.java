
package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.core.attribute.Interval;
import com.thinkaurelius.titan.core.query.QueryType;
import com.thinkaurelius.titan.core.query.ResultCollector;

import java.util.Set;

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
public interface GraphTransaction {

	/***
	 * Creates a new node in the graph.
	 * 
	 * @return New node in the graph created in the context of this transaction.
	 */
	public Node createNode();
	
	/**
	 * Creates a new relationship connecting the specified nodes.
	 * 
	 * Creates and returns a new {@link Relationship} of the specified type connecting the nodes in the order
	 * specified. For a binary relationship, the first node is the start node and the second node is the end node.
	 * For hyperedges the order is adopted from the relationship type definition.
	 * 
	 * @param relType	type of the relationship to be created
     * @param start		Starting node of the relationship
     * @param end       End node of the relationship
	 * @return 			New relationship of specified type
	 */
	public Relationship createRelationship(RelationshipType relType, Node start, Node end);
	
	/**
	 * Creates a new relationship connecting the specified nodes.
	 * 
	 * Creates and returns a new {@link Relationship} of the specified type connecting the nodes in the order
	 * specified. For a binary relationship, the first node is the start node and the second node is the end node.
	 * For hyperedges the order is adopted from the relationship type definition.
	 * 
	 * @param relType	type name of the relationship to be created
	 * @param start		Starting node of the relationship
     * @param end       End node of the relationship
	 * @return 			New relationship of specified type
	 * @throws	IllegalArgumentException if name of the relationship type is unknown, i.e. a relationship type with said name has not yet been created.
	 */
	public Relationship createRelationship(String relType, Node start, Node end);
							
	/**
	 * Creates a new property for the given node with the specified attribute.
	 * 
	 * Creates and returns a new {@link Property} of the specified property type with the node being the start
	 * node and the given object being the attribute.
	 * 
	 * @param propType	type of the property to be created
	 * @param node		Node for which to create the property
	 * @param attribute	attribute of the property to be created
	 * @return 			New property of specified type
	 * @throws	IllegalArgumentException if the attribute does not match the data type of the given property type.
	 */
	public Property createProperty(PropertyType propType, Node node, Object attribute);
	
	/**
	 * Creates a new property for the given node with the specified attribute.
	 * 
	 * Creates and returns a new {@link Property} of the specified property type with the node being the start
	 * node and the given object being the attribute.
	 * 
	 * @param propType	type name of the property to be created
	 * @param node		Node for which to create the property
	 * @param attribute	attribute of the property to be created
	 * @return 			New property of specified type
	 * @throws	IllegalArgumentException if the attribute does not match the data type of the given property type.
	 * @throws	IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
	 */
	public Property createProperty(String propType, Node node, Object attribute);
	
	/**
	 * Retrieves the node for the specified node id.
	 * 
	 * This method assumes that a node with the given id exists, otherwise it throws an exception
	 * 
	 * @param id ID of the node to retrieve
	 * @return Node with the given id
	 * @throws com.thinkaurelius.titan.exceptions.InvalidNodeException if id does not exist in the database
	 * @see #containsNode(long)
	 */
	public Node getNode(long id);

	/**
	 * Checks whether a node with the specified id exists in the graph database.
	 * 
	 * @param nodeid Node ID
	 * @return true, if a node with that id exists, else false
	 */
	public boolean containsNode(long nodeid);

	/**
	 * Creates a new edge query for the node with the specified id.
	 * 
	 * @param nodeid Id of the node for which to create a new edge query
	 * @return An {@link com.thinkaurelius.titan.core.EdgeQuery} constructor for the specified node id
	 * @throws com.thinkaurelius.titan.exceptions.InvalidNodeException if id does not exist in the database
	 * @see com.thinkaurelius.titan.core.EdgeQuery
	 * @see Node#edgeQuery()
	 */
	public EdgeQuery makeEdgeQuery(long nodeid);
	
	/**
	 * Retrieves the node whose attribute value for the specified property type matches the given key.
	 * 
	 * This method assumes that the property type is keyed, i.e. that each attribute for a property of
	 * such type is associated with at most one node. In other words, a node can be uniquely identified
	 * given the property type and the attribute (also called the <b>key</b>).
	 * 
	 * @param type Property type which is keyed
	 * @param key A property attribute
	 * @return The node uniquely identified by the key and property type, or null if no such exists
	 * @throws IllegalArgumentException if the property type is not keyed
	 * @see PropertyType#isKeyed()
	 */
	public Node getNodeByKey(PropertyType type, Object key);
	
	/**
	 * Retrieves the node whose attribute value for the specified property type name matches the given key.
	 *  
	 * @param name Name of the property type which is keyed
	 * @param key A property attribute
	 * @return The node uniquely identified by the key and property type, or null if no such exists
	 * @throws IllegalArgumentException if the property type is not keyed
	 * @see PropertyType#isKeyed()
	 * @see #getNodeByKey(PropertyType, Object)
	 * @throws	IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
	 * @throws	IllegalArgumentException if the property type is not keyed.
	 */
	public Node getNodeByKey(String name, Object key);

	/**
	 * Retrieves all nodes which have an incident property of the given type with the specified attribute.
	 * 
	 * The given property type must have an index defined for this retrieval to succeed.
	 * 
	 * @param type Property type for which to retrieve nodes
	 * @param attribute Attribute value
	 * @return	All nodes which have an incident property of the given type with the specified attribute.
	 * @throws	IllegalArgumentException if the property type is not indexed.
	 * @see PropertyIndex
	 */
	public Set<Node> getNodesByAttribute(PropertyType type, Object attribute);
	
	/**
	 * Retrieves all nodes which have an incident property of the given type with the specified attribute.
	 * 
	 * The given property type must have an index defined for this retrieval to succeed.
	 * 
	 * @param type Property type name for which to retrieve nodes
	 * @param attribute Attribute value
	 * @return	All nodes which have an incident property of the given type with the specified attribute.
	 * @throws	IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
	 * @throws	IllegalArgumentException if the property type is not indexed.
	 * @see PropertyIndex
	 */
	public Set<Node> getNodesByAttribute(String type, Object attribute);
	
	/**
	 * Retrieves all ids for nodes which have an incident property of the given type with the specified attribute.
	 * 
	 * The given property type must have an index defined for this retrieval to succeed.
	 * 
	 * @param type Property type for which to retrieve node ids
	 * @param attribute Attribute value
	 * @return	All ids for nodes which have an incident property of the given type with the specified attribute.
	 * @throws	IllegalArgumentException if the property type is not indexed.
	 * @see PropertyIndex
	 */
	public long[] getNodeIDsByAttribute(PropertyType type, Object attribute);

	/**
	 * Retrieves all nodes which have an incident property of the given type whose attribute lies in the specified interval.
	 * 
	 * The given property type must have an index defined for this retrieval to succeed.
	 * If the specified interval is a {@link com.thinkaurelius.titan.core.attribute.Range}, then the property type must have a range index defined.
	 * 
	 * @param type Property type for which to retrieve nodes
	 * @param interval Interval over attribute values
	 * @return	All nodes which have an incident property of the given type  whose attribute lies in the specified interval.
	 * @throws	IllegalArgumentException if the property type is not indexed or does not have a range index where one is needed.
	 * @see PropertyIndex
	 * @see com.thinkaurelius.titan.core.attribute.Interval
	 */
	public Set<Node> getNodesByAttribute(PropertyType type, Interval<?> interval);
	
	/**
	 * Retrieves all nodes which have an incident property of the given type whose attribute lies in the specified interval.
	 * 
	 * The given property type must have an index defined for this retrieval to succeed.
	 * If the specified interval is a {@link com.thinkaurelius.titan.core.attribute.Range}, then the property type must have a range index defined.
	 * 
	 * @param type Property type name for which to retrieve nodes
	 * @param interval Interval over attribute values
	 * @return	All nodes which have an incident property of the given type  whose attribute lies in the specified interval.
	 * @throws	IllegalArgumentException if the property type is not indexed or does not have a range index where one is needed.
	 * @throws	IllegalArgumentException if name of the property type is unknown, i.e. a property type with said name has not yet been created.
	 * @see PropertyIndex
	 * @see com.thinkaurelius.titan.core.attribute.Interval
	 */
	public Set<Node> getNodesByAttribute(String type, Interval<?> interval);
	
	/**
	 * Retrieves all ids for nodes which have an incident property of the given type whose attribute lies in the specified interval.
	 * 
	 * The given property type must have an index defined for this retrieval to succeed.
	 * If the specified interval is a {@link com.thinkaurelius.titan.core.attribute.Range}, then the property type must have a range index defined.
	 * 
	 * @param type Property type for which to retrieve nodes
	 * @param interval Interval over attribute values
	 * @return	All ids for nodes which have an incident property of the given type  whose attribute lies in the specified interval.
	 * @throws	IllegalArgumentException if the property type is not indexed or does not have a range index where one is needed.
	 * @see PropertyIndex
	 * @see com.thinkaurelius.titan.core.attribute.Interval
	 */
	public long[] getNodeIDsByAttribute(PropertyType type, Interval<?> interval);
	
	/**
	 * Returns an iterable over all nodes in the graph database (optional operation).
	 * 
	 * The order in which the nodes are returned is arbitrary. Note, that all nodes may be loaded
	 * into memory when this method is invoked. Due to this performance issue, a GraphTransaction implementation
	 * may not support this method.
	 * 
	 * This method should only be used for in-memory graph transactions ({@link com.thinkaurelius.titan.configuration.InMemoryGraphDatabase}).
	 * 
	 * @return An iterable over all nodes in the database
	 * @throws UnsupportedOperationException if the graph transaction implementation does not support this method
	 */
	public Iterable<? extends Node> getAllNodes();
	
	/**
	 * Returns an iterable over all relationships in the graph database (optional operation).
	 * 
	 * The order in which the relationships are returned is arbitrary. Note, that all relationships may be loaded
	 * into memory when this method is invoked. Due to this performance issue, a GraphTransaction implementation
	 * may not support this method.
	 * 
	 * This method should only be used for in-memory graph transactions ({@link com.thinkaurelius.titan.configuration.InMemoryGraphDatabase}).
	 * 
	 * @return An iterable over all relationships in the database
	 * @throws UnsupportedOperationException if the graph transaction implementation does not support this method
	 */
	public Iterable<? extends Relationship> getAllRelationships();
	
	/**
	 * Sends the specified query to the node with the given id.
	 * 
	 * The specified id must identify a remotely stored node. Sending queries to such nodes is a means of interacting
	 * with them. A query is defined by the query type which needs to be registered with the graph database prior to
	 * calling this method.
	 * To answer the query, the remotely stored node invokes the {@link QueryType#answer(com.thinkaurelius.titan.core.GraphTransaction, Node, Object, com.thinkaurelius.titan.core.query.QueryResult)}
	 * method with the specified query load. Results added to {@link com.thinkaurelius.titan.core.query.QueryResult}
	 * are returned and added to the provided {@link ResultCollector}.
	 * 
	 * @param <T> Class of the query load
	 * @param <U> Class of the individual result object
	 * @param nodeid ID of the node to which the query is send
	 * @param queryLoad Defines the actual query to be send
	 * @param queryType Specifies the type of query to be send.
	 * @param resultCollector The result collector object which collects results as they are returned
	 * @throws com.thinkaurelius.titan.exceptions.QueryException if the query type is unknown or if sending the query did not succeed
	 * @throws com.thinkaurelius.titan.exceptions.InvalidNodeException if the node with the given id is not stored remotely
	 */
	public<T,U> void sendQuery(long nodeid, T queryLoad, Class<? extends QueryType<T, U>> queryType, ResultCollector<U> resultCollector);
	
	/**
	 * Forwards the specified query to the node with the given id.
	 * 
	 * This method may only be called in the context of answering an incoming query, in which case the query type
	 * is known from the context. The specified query load must match this query type. Results for this query
	 * are returned to the originator of the query.
	 * 
	 * @param queryLoad Defines the query to forward
	 * @param nodeid ID of the node to which the query is send
	 * @throws IllegalStateException if this method is not called in the context of answering an incoming query
	 * @throws com.thinkaurelius.titan.exceptions.QueryException if the query type is unknown or if sending the query did not succeed
	 * @throws IllegalArgumentException if the query load is not an instance of the query class defined by the query type
	 * @throws com.thinkaurelius.titan.exceptions.InvalidNodeException if the node with the given id is not stored remotely
	 */
	public void forwardQuery(long nodeid, Object queryLoad);
	
		
	
	/**
	 * Checks whether an edge type with the specified name exists.
	 * 
	 * @param name Name of the edge type
	 * @return true, if an edge type with the given name exists, else false
	 */
	public boolean containsEdgeType(String name);
	
	
	/**
	 * Returns the edge type with the given name.
	 * Note, that edge type names must be unique.
	 * 
	 * @param name Name of the edge type to return
	 * @return The edge type with the given name, or null if such does not exist
	 */
	public EdgeType getEdgeType(String name);

	/**
	 * Returns the property type with the given name.
	 * 
	 * @param name Name of the property type to return
	 * @return The property type with the given name
	 * @throws IllegalArgumentException if a property type with the given name does not exist or if the edge
	 * type with the given name is not a property type
	 */
	public PropertyType getPropertyType(String name);

	/**
	 * Returns the relationship type with the given name.
	 * 
	 * @param name Name of the relationship type to return
	 * @return The relationship type with the given name
	 * @throws IllegalArgumentException if a relationship type with the given name does not exist or if the edge
	 * type with the given name is not a relationship type
	 */
	public RelationshipType getRelationshipType(String name);
	
	/**
	 * Returns a new edge type maker to create edge types.
	 * 
	 * The edge type constructed with this maker will be created in the context of this transaction.
	 * 
	 * @return An edge type maker linked to this transaction.
	 * @see com.thinkaurelius.titan.core.EdgeTypeMaker
	 * @see com.thinkaurelius.titan.core.EdgeType
	 */
	public EdgeTypeMaker createEdgeType();
	
	
	/**
	 * Flushes the transaction.
	 * 
	 * Flushing the transaction means that the transaction state is synchronized with the storage backend
	 * and ids are assigned to new elements in the transaction. However, the state of the transaction is
	 * not persisted.
	 * 
	 * @throws com.thinkaurelius.titan.exceptions.GraphStorageException if an error arises during flushing.
	 */
	public void flush();
	
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
	public GraphTransactionConfig getTxConfiguration();
	
	/**
	 * Checks whether any changes to the graph database have been made in this transaction.
	 * 
	 * A modification may be an edge or node update, addition, or deletion.
	 * 
	 * @return true, if the transaction contains updates, else false.
	 */
	public boolean hasModifications();
	
}
