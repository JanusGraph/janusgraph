package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.core.EdgeTypeGroup;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;

public interface AdjacencyList extends Iterable<InternalEdge> {	
	
	public static final Iterable<InternalEdge> Empty = IterablesUtil.emptyIterable();	
	
	public AdjacencyList addEdge(InternalEdge e, ModificationStatus status);
	
	public AdjacencyList addEdge(InternalEdge e, boolean checkTypeUniqueness, ModificationStatus status);
	
	public void removeEdge(InternalEdge e, ModificationStatus status);

	public boolean isEmpty();
	
	public boolean containsEdge(InternalEdge e);
	
	public Iterable<InternalEdge> getEdges();

	public Iterable<InternalEdge> getEdges(EdgeType type);

	public Iterable<InternalEdge> getEdges(EdgeTypeGroup group);
	
	
	public AdjacencyListFactory getFactory();
	
}
