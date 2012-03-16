package com.thinkaurelius.titan.blueprints.util;

import com.thinkaurelius.titan.blueprints.TitanGraph;
import com.thinkaurelius.titan.blueprints.TitanVertex;
import com.thinkaurelius.titan.core.NodeList;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Vertex;

import java.util.Iterator;

public class TitanNeighborhoodSequence<T extends Vertex> implements CloseableSequence<TitanVertex> {

    private final NodeList nodes;
    private int position;
    private final TitanGraph db;

    public TitanNeighborhoodSequence(TitanGraph db, final NodeList nodelist) {
        nodes=nodelist;
        position=0;
        this.db=db;
    }
    
    @Override
    public void close() {
        //Do nothing
    }

    @Override
    public Iterator<TitanVertex> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return position<nodes.size();
    }

    @Override
    public TitanVertex next() {
        TitanVertex v = new TitanVertex(nodes.get(position),db);
        position++;
        return v;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
