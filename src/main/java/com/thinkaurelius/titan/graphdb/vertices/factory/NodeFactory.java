package com.thinkaurelius.titan.graphdb.vertices.factory;

import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

public interface NodeFactory {

    /**
     * Creates a new node in the given transaction
     * If a positive id is provided, then the id is assigned.
     *
     * @param tx
     * @return
     */
	public InternalNode createNew(GraphTx tx);
	
	public InternalNode createExisting(GraphTx tx, long id);
	
	
}
