package com.thinkaurelius.titan.graphdb.vertices.factory;

import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

public interface NodeFactory {

	public InternalNode createNew(GraphTx tx);
	
	public InternalNode createExisting(GraphTx tx, long id);
	
	
}
