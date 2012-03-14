package com.thinkaurelius.titan.blueprints.util;

import com.thinkaurelius.titan.blueprints.TitanEdge;
import com.thinkaurelius.titan.core.Relationship;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Edge;

import java.util.Iterator;

public class TitanEdgeSequence<T extends Edge> implements CloseableSequence<TitanEdge> {

    private final Iterator<? extends Relationship> relationships;
    
    public TitanEdgeSequence(Iterator<? extends Relationship> rels) {
        relationships=rels;
    }
    
    public TitanEdgeSequence(Iterable<? extends Relationship> rels) {
        this(rels.iterator());
    }
    
    @Override
    public void close() {
        //Do nothing
    }

    @Override
    public Iterator<TitanEdge> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return relationships.hasNext();
    }

    @Override
    public TitanEdge next() {
        return new TitanEdge(relationships.next());
    }

    @Override
    public void remove() {
        relationships.remove();
    }
}
