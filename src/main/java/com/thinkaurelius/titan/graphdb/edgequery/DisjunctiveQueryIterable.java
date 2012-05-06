package com.thinkaurelius.titan.graphdb.edgequery;

import com.thinkaurelius.titan.core.Edge;

import java.util.Iterator;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class DisjunctiveQueryIterable<T extends Edge> implements Iterable<T> {

    private final ComplexEdgeQuery query;
    private final Class<T> type;
    
    DisjunctiveQueryIterable(ComplexEdgeQuery q, Class<T> type) {
        this.query=q;
        this.type=type;
    }

    @Override
    public Iterator<T> iterator() {
        return new DisjunctiveQueryIterator<T>(query, type);
    }
}
