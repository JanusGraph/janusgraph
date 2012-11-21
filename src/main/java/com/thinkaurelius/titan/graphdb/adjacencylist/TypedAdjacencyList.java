package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;

import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class TypedAdjacencyList implements AdjacencyList {

	
	private final AdjacencyListStrategy strategy;
	private final ConcurrentSkipListMap<TitanType,AdjacencyList> content;

	TypedAdjacencyList(AdjacencyListStrategy strategy) {
		this.strategy = strategy;
//		content = new ConcurrentHashMap<TitanType,AdjacencyList>
//						(strategy.getInitialCapacity(),strategy.getLoadFactor(),strategy.getConcurrencyLevel());
		content = new ConcurrentSkipListMap<TitanType,AdjacencyList>(TypeComparator.INSTANCE);
	}
	
	TypedAdjacencyList(AdjacencyListStrategy strategy, AdjacencyList base) {
		this(strategy);
		for (InternalRelation e : base.getEdges()) {
			addEdge(e,ModificationStatus.none);
		}
	}
	
	@Override
	public synchronized AdjacencyList addEdge(InternalRelation e, ModificationStatus status) {
		return addEdge(e,false,status);
	}

	@Override
	public synchronized AdjacencyList addEdge(InternalRelation e, boolean checkTypeUniqueness, ModificationStatus status) {
		AdjacencyList list = content.get(e.getType());
		if (list==null) {
			list = strategy.getInnerStrategy().emptyList();
			checkTypeUniqueness=false;
			content.put(e.getType(), list);
		}
		AdjacencyList newlist = list.addEdge(e, checkTypeUniqueness, status);
		if (newlist!=list) content.put(e.getType(), newlist);
		return this;
	}

	@Override
	public boolean containsEdge(InternalRelation e) {
		AdjacencyList list = content.get(e.getType());
		return list!=null && list.containsEdge(e);
	}

	@Override
	public Iterable<InternalRelation> getEdges() {
		if (content.isEmpty()) return AdjacencyList.Empty;
		else return Iterables.concat(content.values());
	}

	@Override
	public Iterable<InternalRelation> getEdges(TitanType type) {
		AdjacencyList list = content.get(type);		
		if (list==null) return AdjacencyList.Empty;
		else return list.getEdges(type);
	}
	
	@Override
	public Iterable<InternalRelation> getEdges(TypeGroup group) {
		if (content.isEmpty()) return AdjacencyList.Empty;
		else {
			ConcurrentNavigableMap<TitanType,AdjacencyList> submap = content.subMap(
						TypeComparator.getGroupComparisonType(group.getID()),
						TypeComparator.getGroupComparisonType((short) (group.getID() + 1)));
			return Iterables.concat(submap.values());
		}
	}

	@Override
	public synchronized void removeEdge(InternalRelation e, ModificationStatus status) {
		AdjacencyList list = content.get(e.getType());
		if (list==null) status.nochange();
		else list.removeEdge(e, status);
	}

	@Override
	public AdjacencyListStrategy getStrategy() {
		return strategy;
	}

	@Override
	public boolean isEmpty() {
		for (AdjacencyList list : content.values()) {
			if (!list.isEmpty()) return false;
		}
		return true;
	}

	@Override
	public Iterator<InternalRelation> iterator() {
		return getEdges().iterator();
	}


	
}
