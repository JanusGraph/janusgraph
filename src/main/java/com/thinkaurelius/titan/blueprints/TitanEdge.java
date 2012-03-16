package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.core.Relationship;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;

public class TitanEdge extends TitanElement<Relationship> implements Edge {

    /* This fields serve as a cache in order to avoid repeated object creation */
    private TitanVertex end = null;
    private TitanVertex start = null;
    
    public TitanEdge(Relationship r, TitanGraph db) {
        super(r,db);
    }
    
    @Override
    public Vertex getInVertex() {
        if (end==null) end = new TitanVertex(element.getEnd(),db);
        return end;
    }

    @Override
    public Vertex getOutVertex() {
        if (start==null) start = new TitanVertex(element.getStart(),db);
        return start;
    }

    @Override
    public String getLabel() {
        return element.getRelationshipType().getName();
    }
}
