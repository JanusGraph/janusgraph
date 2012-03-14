package com.thinkaurelius.titan.graphdb.database;

import com.thinkaurelius.titan.graphdb.edges.InternalEdge;

/**
 * This class is an implementation of {@link com.thinkaurelius.titan.graphdb.database.LockManager} which takes no
 * locks. This is useful in situation where the transaction ensures
 * consistency and no further locking is needed.
 * 
 *
 */
public class NoLockManager implements LockManager {

	@Override
	public void createEdge(InternalEdge edge) {
			
	}

	@Override
	public void deleteEdge(InternalEdge edge) {
				
	}

	
	
}
