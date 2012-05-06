package com.thinkaurelius.titan.graphdb.edgequery;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Edge;
import com.thinkaurelius.titan.core.Property;
import com.thinkaurelius.titan.core.Relationship;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class DisjunctiveQueryIterator<T extends Edge> implements Iterator<T> {

    private final List<? extends AtomicEdgeQuery> queries;
    private final Class<T> type;
    private long remainingLimit;
    
    
    private int position = 0;
    private Iterator<T> currentIter = null;
    private Iterator<T> nextIter = null;
    
    
    DisjunctiveQueryIterator(ComplexEdgeQuery q, Class<T> type) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(q);
        this.type=type;
        queries = q.getDisjunctiveQueries();
        remainingLimit = q.getLimit();
    }

    @Override
    public boolean hasNext() {
        if (currentIter==null || !currentIter.hasNext()) {
            initializeNextIter();
            return nextIter!=null && nextIter.hasNext();
        } else return true;
    }
    
    private void initializeNextIter() {
        if (nextIter==null) {
            while (position<queries.size() && remainingLimit>0 && (nextIter==null || !nextIter.hasNext())) {
                AtomicEdgeQuery query = queries.get(position);
                query.setRetrievalLimit(remainingLimit);
                if (type.equals(Edge.class)) nextIter = (Iterator<T>)query.getEdgeIterator();
                else if (type.equals(Property.class)) nextIter = (Iterator<T>)query.getPropertyIterator();
                else if (type.equals(Relationship.class)) nextIter = (Iterator<T>)query.getRelationshipIterator();
                else throw new IllegalStateException("Unknown return type: " + type);
                position++;
            }
        }
    }

    @Override
    public T next() {
        if (currentIter==null || !currentIter.hasNext()) {
            initializeNextIter();
            if (nextIter==null || !nextIter.hasNext()) throw new NoSuchElementException();
            else {
                currentIter = nextIter;
                nextIter = null;
                return next();
            }
        } else {
            remainingLimit--;
            return currentIter.next();
        }
    }

    @Override
    public void remove() {
        currentIter.remove();
    }
    
}
