package com.thinkaurelius.titan.graphdb.query;

import com.thinkaurelius.titan.core.TitanRelation;

import java.util.Iterator;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class DisjunctiveQueryIterable<T extends TitanRelation> implements Iterable<T> {

    private final ComplexTitanQuery query;
    private final Class<T> type;
    
    DisjunctiveQueryIterable(ComplexTitanQuery q, Class<T> type) {
        this.query=q;
        this.type=type;
    }

    @Override
    public Iterator<T> iterator() {
        return new DisjunctiveQueryIterator<T>(query, type);
    }
}
