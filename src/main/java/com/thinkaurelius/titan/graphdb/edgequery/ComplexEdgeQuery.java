package com.thinkaurelius.titan.graphdb.edgequery;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;
import com.thinkaurelius.titan.graphdb.vertices.RemovableEdgeIterable;
import com.thinkaurelius.titan.graphdb.vertices.RemovableEdgeIterator;

import java.util.*;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class ComplexEdgeQuery extends AtomicEdgeQuery {

    private EdgeType[] types;
    
    
    public ComplexEdgeQuery(InternalNode n) {
        super(n);
        types = null;
    }

    public ComplexEdgeQuery(GraphTx tx, long nodeid) {
        super(tx,nodeid);
        types = null;
    }

    ComplexEdgeQuery(ComplexEdgeQuery q) {
        super(q);
        types = q.types;
    }

    @Override
    public ComplexEdgeQuery copy() {
        ComplexEdgeQuery q = new ComplexEdgeQuery(this);
        return q;
    }


    @Override
    public AtomicEdgeQuery withEdgeType(String... type) {
        if (type.length<2) return super.withEdgeType(type);
        else {
            EdgeType[] etypes = new EdgeType[type.length];
            Preconditions.checkNotNull(tx);
            for (int i=0;i<type.length;i++) etypes[i] = tx.getEdgeType(type[i]);
            return withEdgeType(etypes);
        }
    }

    @Override
    public AtomicEdgeQuery withEdgeType(EdgeType... type) {
        if (type.length<2) return super.withEdgeType(type);
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

    List<? extends AtomicEdgeQuery> getDisjunctiveQueries() {
        if (types==null) return ImmutableList.of(this);
        else {
            assert types.length>1;
            List<AtomicEdgeQuery> queries = new ArrayList<AtomicEdgeQuery>(types.length);
            for (int i=0;i<types.length;i++) {
                AtomicEdgeQuery query = new AtomicEdgeQuery(this);
                query.withEdgeType(types[i]);
                queries.add(query);
            }
            return queries;
        }
    }

    @Override
    public Iterable<Property> getProperties() {
        if (isAtomic()) return super.getProperties();
        else return new DisjunctiveQueryIterable(this,Property.class);
    }


    @Override
    public Iterator<Property> getPropertyIterator() {
        if (isAtomic()) return super.getPropertyIterator();
        else return new DisjunctiveQueryIterator(this,Property.class);
    }


    @Override
    public Iterator<Relationship> getRelationshipIterator() {
        if (isAtomic()) return super.getRelationshipIterator();
        else return new DisjunctiveQueryIterator(this,Relationship.class);
    }


    @Override
    public Iterable<Relationship> getRelationships() {
        if (isAtomic()) return super.getRelationships();
        else return new DisjunctiveQueryIterable(this,Relationship.class);
    }

    @Override
    public Iterator<Edge> getEdgeIterator() {
        if (isAtomic()) return super.getEdgeIterator();
        else return new DisjunctiveQueryIterator(this,Edge.class);
    }

    @Override
    public Iterable<Edge> getEdges() {
        if (isAtomic()) return super.getEdges();
        else return new DisjunctiveQueryIterable(this,Edge.class);
    }

    private<T extends Edge> int count(Class<T> type) {
        int count = 0;
        for (AtomicEdgeQuery query : getDisjunctiveQueries()) {
            if (type.equals(Edge.class)) throw new UnsupportedOperationException();
            else if (type.equals(Property.class)) count += query.noProperties();
            else if (type.equals(Relationship.class)) count += query.noRelationships();
            else throw new IllegalArgumentException("Unknown return type: " + type);
        }
        return count;
    }
    
    @Override
    public int noRelationships() {
        if (isAtomic()) return super.noRelationships();
        else return count(Relationship.class);
    }

    @Override
    public int noProperties() {
        if (isAtomic()) return super.noProperties();
        else return count(Property.class);
    }

    @Override
    public NodeListInternal getNeighborhood() {
        if (isAtomic()) return super.getNeighborhood();
        else {
            NodeListInternal nodes = null;
            long remaining = getLimit();
            for (AtomicEdgeQuery query : getDisjunctiveQueries()) {
                NodeListInternal next = query.setRetrievalLimit(remaining).getNeighborhood();
                if (nodes==null) nodes = next;
                else nodes.addAll(next);
                remaining -= next.size();
            }
            return nodes;
        }
    }



}
