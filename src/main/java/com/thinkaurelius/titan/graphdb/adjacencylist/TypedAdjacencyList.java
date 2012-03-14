package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.core.EdgeTypeGroup;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.edgetypes.EdgeTypeComparator;

import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class TypedAdjacencyList implements AdjacencyList {

	
	private final TypedAdjListFactory factory;
	private ConcurrentSkipListMap<EdgeType,AdjacencyList> content;

	TypedAdjacencyList(TypedAdjListFactory factory) {
		this.factory = factory;
//		content = new ConcurrentHashMap<EdgeType,AdjacencyList>
//						(factory.getInitialCapacity(),factory.getLoadFactor(),factory.getConcurrencyLevel());
		content = new ConcurrentSkipListMap<EdgeType,AdjacencyList>(EdgeTypeComparator.Instance);
	}
	
	TypedAdjacencyList(TypedAdjListFactory factory, AdjacencyList base) {
		this(factory);
		for (InternalEdge e : base.getEdges()) {
			addEdge(e,ModificationStatus.none);
		}
	}
	
	@Override
	public synchronized AdjacencyList addEdge(InternalEdge e, ModificationStatus status) {
		return addEdge(e,false,status);
	}

	@Override
	public synchronized AdjacencyList addEdge(InternalEdge e, boolean checkTypeUniqueness, ModificationStatus status) {
		AdjacencyList list = content.get(e.getEdgeType());	
		if (list==null) {
			list = factory.getEmptyTypeAdjList();
			checkTypeUniqueness=false;
			content.put(e.getEdgeType(), list);
		}
		AdjacencyList newlist = list.addEdge(e, checkTypeUniqueness, status);
		if (newlist!=list) content.put(e.getEdgeType(), newlist);
		return this;
	}

	@Override
	public boolean containsEdge(InternalEdge e) {
		AdjacencyList list = content.get(e.getEdgeType());
		return list!=null && list.containsEdge(e);
	}

	@Override
	public Iterable<InternalEdge> getEdges() {
		if (content.isEmpty()) return AdjacencyList.Empty;
		else return Iterables.concat(content.values());
	}

	@Override
	public Iterable<InternalEdge> getEdges(EdgeType type) {
		AdjacencyList list = content.get(type);		
		if (list==null) return AdjacencyList.Empty;
		else return list;
	}
	
	@Override
	public Iterable<InternalEdge> getEdges(EdgeTypeGroup group) {
		if (content.isEmpty()) return AdjacencyList.Empty;
		else {
			ConcurrentNavigableMap<EdgeType,AdjacencyList> submap = content.subMap(
						EdgeTypeComparator.getGroupComparisonEdgeType(group.getID()), 
						EdgeTypeComparator.getGroupComparisonEdgeType((short)(group.getID()+1)));
			return Iterables.concat(submap.values());
		}
	}

	@Override
	public synchronized void removeEdge(InternalEdge e, ModificationStatus status) {
		AdjacencyList list = content.get(e.getEdgeType());		
		if (list==null) status.nochange();
		else list.removeEdge(e, status);
	}

	@Override
	public AdjacencyListFactory getFactory() {
		return factory;
	}

	@Override
	public boolean isEmpty() {
		for (AdjacencyList list : content.values()) {
			if (!list.isEmpty()) return false;
		}
		return true;
	}

	@Override
	public Iterator<InternalEdge> iterator() {
		return getEdges().iterator();
	}


	
}
