package com.thinkaurelius.titan.graphdb.query;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;

import java.util.Iterator;
import java.util.Map;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class EmptyAtomicQuery implements AtomicQuery {

    public static final EmptyAtomicQuery INSTANCE = new EmptyAtomicQuery();

    private EmptyAtomicQuery() {}

    @Override
    public AtomicQuery type(TitanType type) {
        return this;
    }

    @Override
    public AtomicQuery type(String type) {
        return this;
    }

    @Override
    public AtomicQuery includeHidden() {
        return this;
    }

    @Override
    public AtomicQuery clone() {
        return this;
    }

    @Override
    public InternalTitanVertex getNode() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getVertexID() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasEdgeTypeCondition() {
        return false;
    }

    @Override
    public TitanType getTypeCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasGroupCondition() {
        return false;
    }

    @Override
    public TypeGroup getGroupCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasDirectionCondition() {
        return false;
    }

    @Override
    public boolean isAllowedDirection(EdgeDirection dir) {
        return false;
    }

    @Override
    public Direction getDirectionCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean queryProperties() {
        return false;
    }

    @Override
    public boolean queryRelationships() {
        return false;
    }

    @Override
    public boolean queryHidden() {
        return false;
    }

    @Override
    public boolean queryUnmodifiable() {
        return false;
    }

    @Override
    public boolean hasConstraints() {
        return false;
    }

    @Override
    public Map<String, Object> getConstraints() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLimit() {
        return 0;
    }

    @Override
    public TitanQuery types(TitanType... type) {
        return this;
    }

    @Override
    public TitanQuery labels(String... labels) {
        return this;
    }

    @Override
    public TitanQuery keys(String... keys) {
        return this;
    }

    @Override
    public TitanQuery group(TypeGroup group) {
        return this;
    }

    @Override
    public TitanQuery direction(Direction d) {
        return this;
    }

    @Override
    public TitanQuery has(TitanType type, Object value) {
        return this;
    }

    @Override
    public TitanQuery has(String type, Object value) {
        return this;
    }

    @Override
    public <T extends Comparable<T>> TitanQuery has(String s, T t, Compare compare) {
        return this;
    }

    @Override
    public <T extends Comparable<T>> TitanQuery interval(String key, T start, T end) {
        return this;
    }

    @Override
    public <T extends Comparable<T>> TitanQuery interval(TitanKey key, T start, T end) {
        return this;
    }

    @Override
    public TitanQuery onlyModifiable() {
        return this;
    }

    @Override
    public TitanQuery inMemory() {
        return this;
    }

    @Override
    public TitanQuery limit(long limit) {
        return this;
    }

    @Override
    public Iterable<Edge> edges() {
        return IterablesUtil.emptyIterable();
    }

    @Override
    public Iterable<Vertex> vertices() {
        return IterablesUtil.emptyIterable();
    }

    @Override
    public Iterable<TitanEdge> titanEdges() {
        return IterablesUtil.emptyIterable();
    }

    @Override
    public Iterable<TitanProperty> properties() {
        return IterablesUtil.emptyIterable();
    }

    @Override
    public Iterable<TitanRelation> relations() {
        return IterablesUtil.emptyIterable();
    }

    @Override
    public Iterator<TitanProperty> propertyIterator() {
        return Iterators.emptyIterator();
    }

    @Override
    public Iterator<TitanEdge> edgeIterator() {
        return Iterators.emptyIterator();
    }

    @Override
    public Iterator<TitanRelation> relationIterator() {
        return Iterators.emptyIterator();
    }
    
    @Override
    public long count() {
        return 0;
    }

    @Override
    public long propertyCount() {
        return 0;
    }

    @Override
    public VertexListInternal vertexIds() {
        return new VertexArrayList();
    }
}
