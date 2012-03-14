
package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.core.query.QueryType;
import com.thinkaurelius.titan.core.query.ResultCollector;

/**
 * A reference node represents a {@link com.thinkaurelius.titan.core.Node} which is not loaded into the current transaction and therefore has
 * to be interacted with by means of remote communication.
 * 
 * In most cases, reference nodes represent remotely stored nodes of a graph database which are not retrieved into
 * the current transaction but only referenced. In such cases, interacting with such nodes has to be done by means
 * of remote communication via the methods provided in this interface.
 * 
 * Whether or not reference nodes may exist in a given transaction depends on the implementation of the underlying
 * {@link com.thinkaurelius.titan.core.GraphDatabase}. Most single-machine implementations of {@link com.thinkaurelius.titan.core.GraphDatabase} do not use reference nodes.
 * 
 * A reference node does not provide the methods of a normal {@link com.thinkaurelius.titan.core.Node} beyond those of {@link com.thinkaurelius.titan.core.Entity}.
 * In order to interact with a reference
 * node, the user has to send queries remotely using a message passing framework provided by the {@link com.thinkaurelius.titan.core.GraphDatabase}
 * and exposed through the methods in this interface.
 * 
 * To check whether any given {@link com.thinkaurelius.titan.core.Node} is actually a reference node, use the {@link com.thinkaurelius.titan.core.Entity#isReferenceNode()}
 * method.
 * 
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface ReferenceNode extends Entity {

	/**
	 * Sends the specified query to the node represented by this reference node.
	 * 
	 * Reference nodes are references to remotely stored nodes. Sending queries to such nodes is a means of interacting
	 * with them. A query is defined by the query type which needs to be registered with the graph database prior to
	 * calling this method.
	 * To answer the query, the remotely stored node invokes the {@link com.thinkaurelius.titan.core.query.QueryType#answer(com.thinkaurelius.titan.core.GraphTransaction, com.thinkaurelius.titan.core.Node, Object, com.thinkaurelius.titan.core.query.QueryResult)}
	 * method with the specified query load. Results added to {@link com.thinkaurelius.titan.core.query.QueryResult} are returned and added to the provided
	 * {@link com.thinkaurelius.titan.core.query.ResultCollector}
	 * 
	 * @param <T> Class of the query load
	 * @param <U> Class of the individual result object
	 * @param queryLoad Defines the actual query to be send
	 * @param queryType Specifies the type of query to be send.
	 * @param resultCollector The result collector object which collects results as they are returned
	 * @throws com.thinkaurelius.titan.exceptions.QueryException if the query type is unknown or if sending the query did not succeed
	 */
	public<T,U> void sendQuery(T queryLoad, Class<? extends QueryType<T, U>> queryType, ResultCollector<U> resultCollector);
	
	/**
	 * Forwards the specified query to the node represented by this reference node.
	 * 
	 * This method may only be called in the context of answering an incoming query, in which case the query type
	 * is known from the context. The specified query load must match this query type. Results for this query
	 * are returned to the originator of the query.
	 * 
	 * @param queryLoad Defines the query to forward
	 * @throws IllegalStateException if this method is not called in the context of answering an incoming query
	 * @throws com.thinkaurelius.titan.exceptions.QueryException if the query type is unknown or if sending the query did not succeed
	 * @throws IllegalArgumentException if the query load is not an instance of the query class defined by the query type
	 */
	public void forwardQuery(Object queryLoad);
	
}
