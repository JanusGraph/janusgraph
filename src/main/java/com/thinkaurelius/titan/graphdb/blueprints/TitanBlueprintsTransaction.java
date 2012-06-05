package com.thinkaurelius.titan.graphdb.blueprints;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.types.TitanTypeClass;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
        throw new UnsupportedOperationException("Not supported threaded transaction graph. Call on parent graph");
    }

    @Override
    public Vertex addVertex(Object id) {
        //Preconditions.checkArgument(id==null,"Titan does not support vertex id assignment");
        return addVertex();
    }

    @Override
    public Vertex getVertex(Object id) {
        if (id==null) throw ExceptionFactory.vertexIdCanNotBeNull();
        if (!(id instanceof Number)) return null;
        long vid = ((Number)id).longValue();
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
        if (id==null) throw ExceptionFactory.edgeIdCanNotBeNull();
        throw new UnsupportedOperationException("Titan does not support direct edge retrieval");
    }

    @Override
    public void removeEdge(Edge edge) {
        ((TitanEdge)edge).remove();
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        throw new UnsupportedOperationException("Titan does not support direct edge retrieval");
    }

    @Override
    public void shutdown() {
        commit();
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, null);
    }

    // ########## INDEX HANDLING ###########################

    @Override
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
        throw new UnsupportedOperationException("Key indexes cannot be dropped");
    }

    @Override
    public <T extends Element> void createKeyIndex(String key, Class<T> elementClass) {
        Preconditions.checkNotNull(key);
        Preconditions.checkArgument(elementClass.equals(Vertex.class),"Only vertex indexing is supported");

        if (containsType(key)) {
            TitanType type = getType(key);
            if (!type.isPropertyKey()) throw new IllegalArgumentException("Key string does not denote a property key but a label");
            if (!((TitanKey)type).hasIndex()) throw new UnsupportedOperationException("It is not possible to set a key as indexable once it has been used");
        } else {
            makeType().functional(false).name(key).dataType(Object.class).indexed().makePropertyKey();
        }
    }

    @Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        Preconditions.checkArgument(elementClass.equals(Vertex.class),"Only vertex indexing is supported");

        Set<String> indexedkeys = new HashSet<String>();
        for (TitanVertex v : getVertices(SystemKey.TypeClass, TitanTypeClass.KEY)) {
            assert v instanceof TitanKey;
            TitanKey k = (TitanKey)v;
            if (k.hasIndex()) indexedkeys.add(k.getName());
        }
        return indexedkeys;
    }
}
