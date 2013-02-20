package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;

import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListSet;

public class SetAdjacencyList implements AdjacencyList {

    private final AdjacencyListStrategy strategy;
    private final ConcurrentSkipListSet<InternalRelation> content;

    SetAdjacencyList(AdjacencyListStrategy strategy) {
        this.strategy = strategy;
        content = new ConcurrentSkipListSet<InternalRelation>(strategy.getComparator());
    }

    SetAdjacencyList(AdjacencyListStrategy strategy, AdjacencyList base) {
        this(strategy);
        for (InternalRelation e : base.getEdges()) {
            addEdge(e, ModificationStatus.none);
        }
    }

    @Override
    public synchronized AdjacencyList addEdge(InternalRelation e, ModificationStatus status) {
        Preconditions.checkNotNull(e);
        status.setModified(content.add(e));
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
        return content.subSet(new TypeInternalRelation(type, true), new TypeInternalRelation(type, false));
    }

    @Override
    public Iterable<InternalRelation> getEdges(final TypeGroup group) {
        return content.subSet(new GroupInternalRelation(group, true), new GroupInternalRelation(group, false));
    }

    @Override
    public synchronized void removeEdge(InternalRelation e, ModificationStatus status) {
        if (content.remove(e)) {
            status.change();
        } else {
            status.nochange();
        }
    }

    @Override
    public AdjacencyListFactory getFactory() {
        return strategy.getFactory();
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
