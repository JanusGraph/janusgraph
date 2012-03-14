package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.thinkaurelius.titan.graphdb.edges.InternalEdge;

public interface AdjacencyListFactory {
	
	public AdjacencyList emptyList();
	
	public AdjacencyList extend(AdjacencyList list, InternalEdge newEdge, ModificationStatus status);
	
}
