package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.InvalidElementException;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SetAdjacencyList implements AdjacencyList {

	
	private final SetAdjListFactory factory;
	private Set<InternalRelation> content;

	SetAdjacencyList(SetAdjListFactory factory) {
		this.factory = factory;
		content = Collections.newSetFromMap(new ConcurrentHashMap<InternalRelation,Boolean>
						(factory.getInitialCapacity(),factory.getLoadFactor(),factory.getConcurrencyLevel()));
	}
	
	SetAdjacencyList(SetAdjListFactory factory, AdjacencyList base) {
		this(factory);
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
		if (checkTypeUniqueness) {
			if (content.contains(e)) status.nochange();
			else {
				if ((factory.isUniformTyped() && !content.isEmpty()) ||
						(!factory.isUniformTyped() && !Iterables.isEmpty(getEdges(e.getType())) )) {
					throw new InvalidElementException("Cannot add functional edge since an edge of that type already exists",e);
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
	public boolean containsEdge(InternalRelation e) {
		return content.contains(e);
	}

	@Override
	public Iterable<InternalRelation> getEdges() {
		return content;
	}

	@Override
	public Iterable<InternalRelation> getEdges(final TitanType type) {
		if (factory.isUniformTyped()) return getEdges();
		else return Iterables.filter(getEdges(), new Predicate<InternalRelation>() {

			@Override
			public boolean apply(InternalRelation e) {
				return type.equals(e.getType());
			}
			
		});
	}
	
	@Override
	public Iterable<InternalRelation> getEdges(final TypeGroup group) {
		if (factory.isUniformTyped()) return getEdges();
		else return Iterables.filter(getEdges(), new Predicate<InternalRelation>() {

			@Override
			public boolean apply(InternalRelation e) {
				return group.equals(e.getType().getGroup());
			}
			
		});
	}

	@Override
	public synchronized void removeEdge(InternalRelation e, ModificationStatus status) {
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
	public Iterator<InternalRelation> iterator() {
		return getEdges().iterator();
	}
	
}
