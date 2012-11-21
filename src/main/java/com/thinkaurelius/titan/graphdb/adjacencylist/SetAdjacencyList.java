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
import java.util.concurrent.ConcurrentSkipListSet;

public class SetAdjacencyList implements AdjacencyList {

	
	private final Set<InternalRelation> content;

	SetAdjacencyList() {
		content = new ConcurrentSkipListSet<InternalRelation>(Comparator);
	}
	
	SetAdjacencyList(AdjacencyList base) {
		this();
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
		assert content.isEmpty() || content.iterator().next().getType().equals(e.getType()) : "Set only supports one type";
        if (checkTypeUniqueness) {
			if (content.contains(e)) status.nochange();
			else if (!content.isEmpty()) {
                throw new InvalidElementException("Cannot add functional edge since an edge of that type already exists",e);
			} else {
                status.change();
                content.add(e);
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
		return getEdges();
	}
	
	@Override
	public Iterable<InternalRelation> getEdges(final TypeGroup group) {
        return getEdges();
	}

	@Override
	public synchronized void removeEdge(InternalRelation e, ModificationStatus status) {
		if (content.remove(e)) status.nochange();
		else status.change();
	}

	@Override
	public AdjacencyListStrategy getStrategy() {
		throw new UnsupportedOperationException();
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
