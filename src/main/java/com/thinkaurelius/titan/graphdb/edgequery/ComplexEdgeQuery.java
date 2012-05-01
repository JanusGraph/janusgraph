package com.thinkaurelius.titan.graphdb.edgequery;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

import java.util.Arrays;
import java.util.HashMap;

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



}
