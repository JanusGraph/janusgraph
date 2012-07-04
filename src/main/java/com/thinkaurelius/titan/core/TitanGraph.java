
package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.ThreadedTransactionalGraph;

/**
 * Titan graph database implementation of the Blueprint's interface.
 * Use {@link TitanFactory} to open and configure TitanGraph instances.
 *
 * @see TitanFactory
 * @see TitanTransaction
 *
 * @author	Matthias Br&ouml;cheler (http://www.matthiasb.com)
 *
 */
public interface TitanGraph extends Graph, KeyIndexableGraph, ThreadedTransactionalGraph {

    /**
     * Opens a new thread-independent {@link TitanTransaction}.
     *
     * @return Transaction object representing a transactional context.
     */
    public TitanTransaction startTransaction();


	 /**
	  * Closes the graph database.
	  * 
	  * Closing the graph database causes a disconnect and possible closing of the underlying storage backend
	  * and a release of all occupied resources by this graph database.
	  * Closing a graph database requires that all open thread-independent transactions have been closed -
      * otherwise they will be left abandoned.
	  * 
	  * @throws GraphStorageException if closing the graph database caused errors in the storage backend
	  */
	public void shutdown() throws GraphStorageException;


    public TypeMaker makeType();


    public TitanType getType(String name);


    /**
     * Checks whether the graph is still open.
     *
     * @return true, if the graph is open, else false.
     * @see #shutdown()
     */
    public boolean isOpen();

	
}
