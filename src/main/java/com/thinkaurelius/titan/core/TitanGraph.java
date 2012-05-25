
package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.exceptions.GraphStorageException;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.ThreadedTransactionalGraph;

/***
 * A graph database is the persistence framework for a graph consisting of {@link TitanVertex}s and {@link TitanRelation}s.
 * 
 * A graph database implementation coordinates all retrieval and persistence operations against the stored
 * graph. A graph database is opened via {@link com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration#openDatabase()} after it has
 * been configured using the methods provided by{@link com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration}.
 * 
 * All user interactions with a graph database are channeled through {@link TitanTransaction}s. A graph
 * database only provides methods for starting new transactions which in turn provide the means to modify
 * the stored graph.
 * 
 * @see com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration
 * @see TitanTransaction
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public interface TitanGraph extends Graph, KeyIndexableGraph, ThreadedTransactionalGraph {

    public TitanTransaction startThreadTransaction();


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
	 public void shutdown() throws GraphStorageException;

	
}
