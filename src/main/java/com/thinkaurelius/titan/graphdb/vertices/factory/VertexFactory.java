package com.thinkaurelius.titan.graphdb.vertices.factory;

import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;

public interface VertexFactory {

    /**
     * Creates a new node in the given transaction
     * If a positive id is provided, then the id is assigned.
     *
     * @param tx
     * @return
     */
	public InternalTitanVertex createNew(InternalTitanTransaction tx);
	
	public InternalTitanVertex createExisting(InternalTitanTransaction tx, long id);
	
	
}
