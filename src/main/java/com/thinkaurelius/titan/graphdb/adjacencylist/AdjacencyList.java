package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;

public interface AdjacencyList extends Iterable<InternalRelation> {
	
	public static final Iterable<InternalRelation> Empty = IterablesUtil.emptyIterable();
	
	public AdjacencyList addEdge(InternalRelation e, ModificationStatus status);
	
	public AdjacencyList addEdge(InternalRelation e, boolean checkTypeUniqueness, ModificationStatus status);
	
	public void removeEdge(InternalRelation e, ModificationStatus status);

	public boolean isEmpty();
	
	public boolean containsEdge(InternalRelation e);
	
	public Iterable<InternalRelation> getEdges();

	public Iterable<InternalRelation> getEdges(TitanType type);

	public Iterable<InternalRelation> getEdges(TypeGroup group);
	
	
	public AdjacencyListFactory getFactory();
	
}
