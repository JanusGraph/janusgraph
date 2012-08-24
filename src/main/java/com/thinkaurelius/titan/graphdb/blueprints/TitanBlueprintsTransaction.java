package com.thinkaurelius.titan.graphdb.blueprints;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
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
    public Vertex getVertex(final Object id) {
        if (null == id)
            throw ExceptionFactory.vertexIdCanNotBeNull();
        if (id instanceof Vertex) //allows vertices to be "re-attached" to the current transaction
            return getVertex(((Vertex)id).getId());

        final long longId;
        if (id instanceof Long) {
            longId = (Long) id;
        } else if (id instanceof Number) {
            longId = ((Number) id).longValue();
        } else {
            try {
                longId = Double.valueOf(id.toString()).longValue();
            } catch (NumberFormatException e) {
                return null;
            }
        }
        if (longId <= 0)
            return null;
        else
            return getVertex(longId);
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
        RelationIdentifier rid = null;
        if (id instanceof RelationIdentifier) rid = (RelationIdentifier)id;
        else if (id instanceof String) rid = RelationIdentifier.parse((String)id);
        else if (id instanceof long[]) rid = RelationIdentifier.get((long[])id);
        else if (id instanceof int[]) rid = RelationIdentifier.get((int[])id);

        if (rid!=null) return rid.findEdge(this);
        else return null;
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
        Preconditions.checkNotNull(elementClass,"Must provide either Vertex.class or Edge.class as an argument");
        if (!elementClass.equals(Vertex.class)) return Sets.newHashSet();

        Set<String> indexedkeys = new HashSet<String>();
        for (TitanVertex v : getVertices(SystemKey.TypeClass, TitanTypeClass.KEY)) {
            assert v instanceof TitanKey;
            TitanKey k = (TitanKey)v;
            if (k.hasIndex()) indexedkeys.add(k.getName());
        }
        return indexedkeys;
    }
}
