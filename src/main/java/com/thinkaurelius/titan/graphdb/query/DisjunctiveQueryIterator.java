package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanEdge;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class DisjunctiveQueryIterator<T extends TitanRelation> implements Iterator<T> {

    private final List<? extends AtomicTitanQuery> queries;
    private final Class<T> type;
    private long remainingLimit;
    
    
    private int position = 0;
    private Iterator<T> currentIter = null;
    private Iterator<T> nextIter = null;
    
    
    DisjunctiveQueryIterator(ComplexTitanQuery q, Class<T> type) {
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
                AtomicTitanQuery query = queries.get(position);
                query.limit(remainingLimit);
                if (type.equals(TitanRelation.class)) nextIter = (Iterator<T>)query.relationIterator();
                else if (type.equals(TitanProperty.class)) nextIter = (Iterator<T>)query.propertyIterator();
                else if (type.equals(TitanEdge.class)) nextIter = (Iterator<T>)query.edgeIterator();
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
