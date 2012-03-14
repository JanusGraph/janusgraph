package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.core.EdgeTypeGroup;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;

import java.util.Iterator;

public class InitialAdjacencyList implements AdjacencyList {

	private static final Iterable<InternalEdge> Empty = IterablesUtil.emptyIterable();	
	
	private final AdjacencyListFactory factory;
	
	InitialAdjacencyList(AdjacencyListFactory factory) {
		this.factory=factory;
	}
	
	@Override
	public synchronized AdjacencyList addEdge(InternalEdge e, ModificationStatus status) {
		return factory.extend(this, e, status);
	}

	@Override
	public synchronized AdjacencyList addEdge(InternalEdge e, boolean checkTypeUniqueness, ModificationStatus status) {
		return factory.extend(this, e, status);
	}

	@Override
	public boolean containsEdge(InternalEdge e) {
		return false;
	}

	@Override
	public Iterable<InternalEdge> getEdges() {
		return Empty;
	}

	@Override
	public Iterable<InternalEdge> getEdges(EdgeType type) {
		return Empty;
	}
	
	@Override
	public Iterable<InternalEdge> getEdges(EdgeTypeGroup group) {
		return Empty;
	}

	@Override
	public void removeEdge(InternalEdge e, ModificationStatus status) {
		status.nochange();
	}

	@Override
	public AdjacencyListFactory getFactory() {
		return factory;
	}

	@Override
	public Iterator<InternalEdge> iterator() {
		return Iterators.emptyIterator();
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

}
