package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;

import java.util.*;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class ComplexTitanQuery implements TitanQuery {

    private List<TitanQuery> disjunction;
    
    public ComplexTitanQuery(List<TitanQuery> disjunction) {
        Preconditions.checkNotNull(disjunction);
        Preconditions.checkArgument(!disjunction.isEmpty());
        this.disjunction=disjunction;
    }


    ComplexTitanQuery(ComplexTitanQuery q) {
        disjunction = new ArrayList<TitanQuery>(q.disjunction.size());
        for (TitanQuery a : q.disjunction) disjunction.add(a.clone());
    }

    public ComplexTitanQuery clone() {
        ComplexTitanQuery q = new ComplexTitanQuery(this);
        return q;
    }

    /* ---------------------------------------------------------------
      * Query Execution
      * ---------------------------------------------------------------
      */

    private List<AtomicQuery> flattenQuery() {
        List<AtomicQuery> queries = new ArrayList<AtomicQuery>(disjunction.size());
        flattenQuery(queries);
        return queries;
    }

    private void flattenQuery(List<AtomicQuery> container) {
        for (TitanQuery q : disjunction) {
            if (q instanceof SimpleAtomicQuery) {
                container.add((SimpleAtomicQuery)q);
            } else if (q instanceof EmptyAtomicQuery) {
                //do nothing
            } else if (q instanceof ComplexTitanQuery) {
                ((ComplexTitanQuery)q).flattenQuery(container);
            } else throw new IllegalStateException("Unexpected query type found: " + q.getClass());
        }
    }
    
    @Override
    public Iterable<TitanProperty> properties() {
        return new DisjunctiveQueryIterable(flattenQuery(),TitanProperty.class);
    }


    @Override
    public Iterable<Edge> edges() {
        return new DisjunctiveQueryIterable(flattenQuery(),TitanEdge.class);
    }

    @Override
    public Iterable<TitanEdge> titanEdges() {
        return new DisjunctiveQueryIterable(flattenQuery(),TitanEdge.class);
    }


    @Override
    public Iterable<TitanRelation> relations() {
        return new DisjunctiveQueryIterable(flattenQuery(),TitanRelation.class);
    }

    @Override
    public long count() {
        return Iterables.size(edges());
    }

    @Override
    public long propertyCount() {
        return Iterables.size(properties());
    }

    @Override
    public VertexListInternal vertexIds() {
        List<AtomicQuery> queries = flattenQuery();
        if (queries.isEmpty()) return new VertexArrayList();
        else {
            VertexListInternal vertices = null;
            long remaining = queries.get(0).getLimit();
            for (AtomicQuery query : queries) {
                query.limit(remaining);
                VertexListInternal next = query.vertexIds();
                if (vertices==null) vertices = next;
                else vertices.addAll(next);
                remaining -= next.size();
            }
            return vertices;
        }
    }

    @Override
    public Iterable<Vertex> vertices() {
        return (Iterable)vertexIds();
    }


    /* ---------------------------------------------------------------
    * Query Definition Methods
    * ---------------------------------------------------------------
    */

    @Override
    public TitanQuery types(TitanType... type) {
        throw new IllegalStateException("Types have already been set");
    }

    @Override
    public TitanQuery labels(String... labels) {
        throw new IllegalStateException("Labels have already been set");
    }

    @Override
    public TitanQuery keys(String... keys) {
        throw new IllegalStateException("Keys have already been set");
    }

    @Override
    public TitanQuery group(TypeGroup group) {
        throw new IllegalStateException("Labels or keys have already been set");
    }

    @Override
    public TitanQuery direction(Direction d) {
        for (int i=0;i<disjunction.size();i++)
            disjunction.set(i,disjunction.get(i).direction(d));
        return this;
    }

    @Override
    public TitanQuery has(TitanType type, Object value) {
        for (int i=0;i<disjunction.size();i++)
            disjunction.set(i,disjunction.get(i).has(type,value));
        return this;
    }

    @Override
    public TitanQuery has(String type, Object value) {
        for (int i=0;i<disjunction.size();i++)
            disjunction.set(i,disjunction.get(i).has(type,value));
        return this;
    }

    @Override
    public <T extends Comparable<T>> TitanQuery has(String s, T t, Compare compare) {
        for (int i=0;i<disjunction.size();i++)
            disjunction.set(i,disjunction.get(i).has(s,t,compare));
        return this;
    }

    @Override
    public <T extends Comparable<T>> TitanQuery interval(String key, T start, T end) {
        for (int i=0;i<disjunction.size();i++)
            disjunction.set(i,disjunction.get(i).interval(key,start,end));
        return this;
    }

    @Override
    public <T extends Comparable<T>> TitanQuery interval(TitanKey key, T start, T end) {
        for (int i=0;i<disjunction.size();i++)
            disjunction.set(i,disjunction.get(i).interval(key,start,end));
        return this;
    }

    @Override
    public TitanQuery onlyModifiable() {
        for (int i=0;i<disjunction.size();i++)
            disjunction.set(i,disjunction.get(i).onlyModifiable());
        return this;
    }

    @Override
    public TitanQuery inMemory() {
        for (int i=0;i<disjunction.size();i++)
            disjunction.set(i,disjunction.get(i).inMemory());
        return this;
    }

    @Override
    public TitanQuery limit(long limit) {
        for (int i=0;i<disjunction.size();i++)
            disjunction.set(i,disjunction.get(i).limit(limit));
        return this;
    }


}
