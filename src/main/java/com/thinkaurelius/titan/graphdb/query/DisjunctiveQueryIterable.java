package com.thinkaurelius.titan.graphdb.query;

import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.TitanRelation;

import java.util.Iterator;
import java.util.List;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class DisjunctiveQueryIterable<T extends TitanRelation> implements Iterable<T> {

    private final List<AtomicQuery> queries;
    private final Class<T> type;
    
    DisjunctiveQueryIterable(List<AtomicQuery> q, Class<T> type) {
        this.queries=q;
        this.type=type;
    }

    @Override
    public Iterator<T> iterator() {
        if (queries.isEmpty()) return Iterators.emptyIterator();
        else return new DisjunctiveQueryIterator<T>(queries, type);
    }
}
