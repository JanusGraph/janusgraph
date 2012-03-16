package com.thinkaurelius.titan.blueprints.util;

import com.thinkaurelius.titan.blueprints.TitanEdge;
import com.thinkaurelius.titan.blueprints.TitanGraph;
import com.thinkaurelius.titan.core.Relationship;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Edge;

import java.util.Iterator;

public class TitanEdgeSequence<T extends Edge> implements CloseableSequence<TitanEdge> {

    private final Iterator<? extends Relationship> relationships;
    private final TitanGraph db;
    
    public TitanEdgeSequence(final TitanGraph db, final Iterator<? extends Relationship> rels) {
        relationships=rels;
        this.db=db;
    }
    
    public TitanEdgeSequence(final TitanGraph db, final Iterable<? extends Relationship> rels) {
        this(db,rels.iterator());
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
        return new TitanEdge(relationships.next(),db);
    }

    @Override
    public void remove() {
        relationships.remove();
    }
}
