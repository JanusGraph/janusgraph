package com.thinkaurelius.titan.graphdb.database;

import com.thinkaurelius.titan.graphdb.edges.InternalEdge;

public interface LockManager {

	public void deleteEdge(InternalEdge edge);
	
	public void createEdge(InternalEdge edge);
	
	
}
