package com.thinkaurelius.titan.graphdb.query;

import com.google.common.collect.ImmutableList;
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

    
    public ComplexTitanQuery(InternalTitanVertex n) {
        super(n);
    }

    public ComplexTitanQuery(InternalTitanTransaction tx, long nodeid) {
        super(tx,nodeid);
    }

    ComplexTitanQuery(ComplexTitanQuery q) {
        super(q);
    }

    @Override
    public ComplexTitanQuery copy() {
        ComplexTitanQuery q = new ComplexTitanQuery(this);
        return q;
    }

    @Override
    public AtomicTitanQuery types(TitanType... type) {
        int length = 0;
        for (int i=0;i<type.length;i++)
            if (type[i]!=null) length++;
        TitanType[] ttypes = new TitanType[length];
        int pos = 0;
        for (int i=0;i<type.length;i++) {
            if (type[i]!=null) {
                ttypes[pos]=type[i];
                pos++;
            }
        }
        if (length==0 && type.length>0) return super.types(new TitanType[]{null});
        if (ttypes.length<2) return super.types(ttypes);
        else {
            types = ttypes;
            return this;
        }
    }


    /* ---------------------------------------------------------------
      * Query Execution
      * ---------------------------------------------------------------
      */

    public boolean isAtomic() {
        return types == null || types.length<2;
    }

    List<? extends AtomicTitanQuery> getDisjunctiveQueries() {
        if (types==null) return ImmutableList.of(this);
        else {
            assert types.length>1;
            List<AtomicTitanQuery> queries = new ArrayList<AtomicTitanQuery>(types.length);
            for (int i=0;i<types.length;i++) {
                AtomicTitanQuery query = new AtomicTitanQuery(this);
                query.types(types[i]);
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
            VertexListInternal vertices = null;
            long remaining = getLimit();
            for (AtomicTitanQuery query : getDisjunctiveQueries()) {
                VertexListInternal next = query.limit(remaining).vertexIds();
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


}
