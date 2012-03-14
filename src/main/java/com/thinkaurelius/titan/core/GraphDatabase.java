
package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.exceptions.GraphStorageException;

/***
 * A graph database is the persistence framework for a graph consisting of {@link Node}s and {@link com.thinkaurelius.titan.core.Edge}s.
 * 
 * A graph database implementation coordinates all retrieval and persistence operations against the stored
 * graph. A graph database is opened via {@link com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration#openDatabase()} after it has
 * been configured using the methods provided by{@link com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration}.
 * 
 * All user interactions with a graph database are channeled through {@link GraphTransaction}s. A graph
 * database only provides methods for starting new transactions which in turn provide the means to modify
 * the stored graph.
 * 
 * @see com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration
 * @see GraphTransaction
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface GraphDatabase {
	
	/**
	 * Starts a graph transaction against this database configured according to the provided
	 * {@link GraphTransactionConfig}.
	 * 
	 * @param txconfig Configuration for the graph transaction to be started.
	 * @return A GraphTransaction configured according to the provided configuration.
	 */
	public GraphTransaction startTransaction(GraphTransactionConfig txconfig);
	
	/**
	 * Opens a new transaction against this graph database.
	 * 
	 * Starts a new transaction on this graph database. The parameter specifies whether the transaction
	 * is read-only. A read-only transaction does not allow the creation of new nodes or edges but only their
	 * retrieval. Depending on the implementation, read-only transactions allow faster operations and consume less
	 * memory.
	 * 
	 * @param readOnly Whether the transaction is read-only
	 * @return A new transaction on this graph database.
	 */
	public GraphTransaction startTransaction(boolean readOnly);
	 
	/**
	 * Opens a new transaction against this graph database.
	 * 
	 * Starts a new transaction on this graph database. Using the default configuration for the transaction
	 * which means that the returned transaction is read-and-write.
	 * 
	 * @return A new transaction on this graph database.
	 */
	public GraphTransaction startTransaction();
	 
//	 public boolean registerQueryType(Class<? extends QueryType> query, 
//			 Class<? extends ResultCollector> result, Class<?> queryClass, Class<?> resultClass);
	 
	/***
	 * Returns the configuration of this graph database
	 * 
	 * The configuration returned reflects the current configuration parameters of this database. Note that changes
	 * to the configuration at this point will not effect the graph database or lead to an exception.
	 * 
	 * @return The configuration used by this graph database.
	 * @see com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration
	 */
	 public GraphDatabaseConfiguration getConfiguration();
	 
	 /**
	  * Returns statistical information about the graph stored in this graph database.
	  * 
	  * The returned {@link GraphStatistics} object can be used to query statistical information about the graph stored
	  * in this database instance, such as the number of edges contained in it.
	  * 
	  * @return GraphStatistics containing statistical information about this database
	  * @see GraphStatistics
	  */
	 public GraphStatistics getStatistics();
	 
	 /**
	  * Closes the graph database.
	  * 
	  * Closing the graph database causes a disconnect and possible closing of the underlying storage backend
	  * and a release of all occupied resources by this graph database.
	  * Closing a graph database requires that all open transactions have been closed - otherwise they will
	  * be left abandoned.
	  * 
	  * @throws GraphStorageException if closing the graph database caused errors in the storage backend
	  */
	 public void close() throws GraphStorageException;

	
}
