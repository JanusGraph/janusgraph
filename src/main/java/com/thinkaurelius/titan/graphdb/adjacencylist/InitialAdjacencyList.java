package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;

import java.util.Iterator;

public class InitialAdjacencyList implements AdjacencyList {

	private static final Iterable<InternalRelation> Empty = IterablesUtil.emptyIterable();
	
	private final AdjacencyListFactory factory;
	
	InitialAdjacencyList(AdjacencyListFactory factory) {
		this.factory=factory;
	}
	
	@Override
	public synchronized AdjacencyList addEdge(InternalRelation e, ModificationStatus status) {
		return factory.extend(this, e, status);
	}

	@Override
	public synchronized AdjacencyList addEdge(InternalRelation e, boolean checkTypeUniqueness, ModificationStatus status) {
		return factory.extend(this, e, status);
	}

	@Override
	public boolean containsEdge(InternalRelation e) {
		return false;
	}

	@Override
	public Iterable<InternalRelation> getEdges() {
		return Empty;
	}

	@Override
	public Iterable<InternalRelation> getEdges(TitanType type) {
		return Empty;
	}
	
	@Override
	public Iterable<InternalRelation> getEdges(TypeGroup group) {
		return Empty;
	}

	@Override
	public void removeEdge(InternalRelation e, ModificationStatus status) {
		status.nochange();
	}

	@Override
	public AdjacencyListFactory getFactory() {
		return factory;
	}

	@Override
	public Iterator<InternalRelation> iterator() {
		return Iterators.emptyIterator();
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

}
