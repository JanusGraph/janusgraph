package com.thinkaurelius.titan.graphdb.blueprints;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import java.util.Iterator;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public abstract class TitanBlueprintsTransaction implements TitanTransaction {

    @Override
    public void startTransaction() throws IllegalStateException {
        //Do nothing, transaction already started
    }

    @Override
    public void stopTransaction(Conclusion conclusion) {
        switch(conclusion) {
            case SUCCESS: commit(); break;
            case FAILURE: abort(); break;
            default: throw new AssertionError("Unrecognized conclusion: " + conclusion);
        }
    }

    @Override
    public Features getFeatures() {
        return TitanFeatures.getTitanFeatures();
    }

    @Override
    public Vertex addVertex(Object id) {
        //Preconditions.checkArgument(id==null,"Titan does not support vertex id assignment");
        return addVertex();
    }

    @Override
    public Vertex getVertex(Object id) {
        if (id==null) throw ExceptionFactory.vertexIdCanNotBeNull();
        if (!(id instanceof Long)) return null;
        long vid = ((Long)id).longValue();
        if (vid<=0) return null;
        return getVertex(vid);
    }

    @Override
    public void removeVertex(Vertex vertex) {
        TitanVertex v = (TitanVertex)vertex;
        //Delete all edges
        Iterator<TitanRelation> iter = v.getRelations().iterator();
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }
        v.remove();
    }


    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        //Preconditions.checkArgument(id==null,"Titan does not support edge id assignment");
        Preconditions.checkArgument(outVertex instanceof TitanVertex);
        Preconditions.checkArgument(inVertex instanceof TitanVertex);
        return addEdge((TitanVertex)outVertex,(TitanVertex)inVertex,label);
    }

    @Override
    public Edge getEdge(Object id) {
        throw new UnsupportedOperationException("Titan does not support direct edge retrieval.");
    }

    @Override
    public void removeEdge(Edge edge) {
        ((TitanEdge)edge).remove();
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        throw new UnsupportedOperationException("Titan does not support direct edge retrieval.");
    }

    @Override
    public void shutdown() {
        commit();
    }
}
