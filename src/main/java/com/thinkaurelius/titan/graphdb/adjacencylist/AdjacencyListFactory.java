package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.thinkaurelius.titan.graphdb.edges.InternalRelation;

public interface AdjacencyListFactory {
	
	public AdjacencyList emptyList();
	
	public AdjacencyList extend(AdjacencyList list, InternalRelation newEdge, ModificationStatus status);
	
}
