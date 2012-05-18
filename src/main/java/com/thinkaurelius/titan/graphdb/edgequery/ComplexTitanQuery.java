package com.thinkaurelius.titan.graphdb.edgequery;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.util.*;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class ComplexTitanQuery extends AtomicTitanQuery {

    private TitanType[] types;
    
    
    public ComplexTitanQuery(InternalTitanVertex n) {
        super(n);
        types = null;
    }

    public ComplexTitanQuery(InternalTitanTransaction tx, long nodeid) {
        super(tx,nodeid);
        types = null;
    }

    ComplexTitanQuery(ComplexTitanQuery q) {
        super(q);
        types = q.types;
    }

    @Override
    public ComplexTitanQuery copy() {
        ComplexTitanQuery q = new ComplexTitanQuery(this);
        return q;
    }


    @Override
    public AtomicTitanQuery labels(String... type) {
        if (type.length<2) return super.labels(type);
        else {
            TitanType[] etypes = new TitanType[type.length];
            Preconditions.checkNotNull(tx);
            for (int i=0;i<type.length;i++) {
                etypes[i] = tx.getType(type[i]);
                Preconditions.checkArgument(etypes[i].isEdgeLabel(),"Expected label but got: " + type[i]);
            }
            return types(etypes);
        }
    }

    @Override
    public AtomicTitanQuery keys(String... type) {
        if (type.length<2) return super.keys(type);
        else {
            TitanType[] etypes = new TitanType[type.length];
            Preconditions.checkNotNull(tx);
            for (int i=0;i<type.length;i++) {
                etypes[i] = tx.getType(type[i]);
                Preconditions.checkArgument(etypes[i].isPropertyKey(),"Expected property key but got: " + type[i]);
            }
            return types(etypes);
        }
    }

    @Override
    public AtomicTitanQuery types(TitanType... type) {
        if (type.length<2) return super.types(type);
        else {
            for (int i=0;i<type.length;i++) Preconditions.checkNotNull(type[i],"Unknown edge type at position " + i);
            types = type;
            super.removeEdgeType();
            return this;
        }
    }

    @Override
    protected void removeEdgeType() {
        types = null;
        super.removeEdgeType();
    }

    /* ---------------------------------------------------------------
      * Query Execution
      * ---------------------------------------------------------------
      */

    public boolean isAtomic() {
        return types == null;
    }

    List<? extends AtomicTitanQuery> getDisjunctiveQueries() {
        if (types==null) return ImmutableList.of(this);
        else {
            assert types.length>1;
            List<AtomicTitanQuery> queries = new ArrayList<AtomicTitanQuery>(types.length);
            for (int i=0;i<types.length;i++) {
                AtomicTitanQuery query = new AtomicTitanQuery(this);
                query.type(types[i]);
                queries.add(query);
            }
            return queries;
        }
    }

    @Override
    public Iterable<TitanProperty> properties() {
        if (isAtomic()) return super.properties();
        else return new DisjunctiveQueryIterable(this,TitanProperty.class);
    }


    @Override
    public Iterator<TitanProperty> propertyIterator() {
        if (isAtomic()) return super.propertyIterator();
        else return new DisjunctiveQueryIterator(this,TitanProperty.class);
    }


    @Override
    public Iterator<TitanEdge> edgeIterator() {
        if (isAtomic()) return super.edgeIterator();
        else return new DisjunctiveQueryIterator(this,TitanEdge.class);
    }


    @Override
    public Iterable<Edge> edges() {
        if (isAtomic()) return super.edges();
        else return new DisjunctiveQueryIterable(this,TitanEdge.class);
    }

    @Override
    public Iterable<TitanEdge> titanEdges() {
        if (isAtomic()) return super.titanEdges();
        else return new DisjunctiveQueryIterable(this,TitanEdge.class);
    }

    @Override
    public Iterator<TitanRelation> relationIterator() {
        if (isAtomic()) return super.relationIterator();
        else return new DisjunctiveQueryIterator(this,TitanRelation.class);
    }

    @Override
    public Iterable<TitanRelation> relations() {
        if (isAtomic()) return super.relations();
        else return new DisjunctiveQueryIterable(this,TitanRelation.class);
    }

    @Override
    public VertexListInternal vertexIds() {
        if (isAtomic()) return super.vertexIds();
        else {
            VertexListInternal nodes = null;
            long remaining = getLimit();
            for (AtomicTitanQuery query : getDisjunctiveQueries()) {
                VertexListInternal next = query.limit(remaining).vertexIds();
                if (nodes==null) nodes = next;
                else nodes.addAll(next);
                remaining -= next.size();
            }
            return nodes;
        }
    }

    @Override
    public Iterable<Vertex> vertices() {
        return (Iterable)vertexIds();
    }


}
