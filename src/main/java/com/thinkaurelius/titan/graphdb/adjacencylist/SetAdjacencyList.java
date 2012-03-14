package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.core.EdgeTypeGroup;
import com.thinkaurelius.titan.exceptions.InvalidEdgeException;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SetAdjacencyList implements AdjacencyList {

	
	private final SetAdjListFactory factory;
	private Set<InternalEdge> content;

	SetAdjacencyList(SetAdjListFactory factory) {
		this.factory = factory;
		content = Collections.newSetFromMap(new ConcurrentHashMap<InternalEdge,Boolean>
						(factory.getInitialCapacity(),factory.getLoadFactor(),factory.getConcurrencyLevel()));
	}
	
	SetAdjacencyList(SetAdjListFactory factory, AdjacencyList base) {
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
		if (checkTypeUniqueness) {
			if (content.contains(e)) status.nochange();
			else {
				if ((factory.isUniformTyped() && !content.isEmpty()) ||
						(!factory.isUniformTyped() && !Iterables.isEmpty(getEdges(e.getEdgeType())) )) {
					throw new InvalidEdgeException("Cannot add functional edge since an edge of that type already exists!");
				} else {
					status.change();
					content.add(e);
				}
			}
		} else {
			status.setModified(content.add(e));
		}
		return this;
	}

	@Override
	public boolean containsEdge(InternalEdge e) {
		return content.contains(e);
	}

	@Override
	public Iterable<InternalEdge> getEdges() {
		return content;
	}

	@Override
	public Iterable<InternalEdge> getEdges(final EdgeType type) {
		if (factory.isUniformTyped()) return getEdges();
		else return Iterables.filter(getEdges(), new Predicate<InternalEdge>() {

			@Override
			public boolean apply(InternalEdge e) {
				return type.equals(e.getEdgeType());
			}
			
		});
	}
	
	@Override
	public Iterable<InternalEdge> getEdges(final EdgeTypeGroup group) {
		if (factory.isUniformTyped()) return getEdges();
		else return Iterables.filter(getEdges(), new Predicate<InternalEdge>() {

			@Override
			public boolean apply(InternalEdge e) {
				return group.equals(e.getEdgeType().getGroup());
			}
			
		});
	}

	@Override
	public synchronized void removeEdge(InternalEdge e, ModificationStatus status) {
		if (content.remove(e)) status.nochange();
		else status.change();
	}

	@Override
	public AdjacencyListFactory getFactory() {
		return factory;
	}

	@Override
	public boolean isEmpty() {
		return content.isEmpty();
	}

	@Override
	public Iterator<InternalEdge> iterator() {
		return getEdges().iterator();
	}
	
}
