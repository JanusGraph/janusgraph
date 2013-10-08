package com.thinkaurelius.titan.graphdb.blueprints;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.thinkaurelius.titan.graphdb.types.TitanTypeClass;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Blueprints specific implementation of {@link TitanTransaction}.
 * Provides utility methods that wrap Titan calls with Blueprints terminology.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanBlueprintsTransaction implements TitanTransaction {

    private static final Logger log =
            LoggerFactory.getLogger(TitanBlueprintsTransaction.class);


    @Override
    public void stopTransaction(Conclusion conclusion) {
        switch (conclusion) {
            case SUCCESS:
                commit();
                break;
            case FAILURE:
                rollback();
                break;
            default:
                throw new IllegalArgumentException("Unrecognized conclusion: " + conclusion);
        }
    }

    @Override
    public Features getFeatures() {
        throw new UnsupportedOperationException("Not supported threaded transaction graph. Call on parent graph");
    }

    @Override
    public Vertex addVertex(Object id) {
        return addVertex();
    }

    @Override
    public Vertex getVertex(final Object id) {
        if (null == id)
            throw ExceptionFactory.vertexIdCanNotBeNull();
        if (id instanceof Vertex) //allows vertices to be "re-attached" to the current transaction
            return getVertex(((Vertex) id).getId());

        final long longId;
        if (id instanceof Long) {
            longId = (Long) id;
        } else if (id instanceof Number) {
            longId = ((Number) id).longValue();
        } else {
            try {
                longId = Long.valueOf(id.toString()).longValue();
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return getVertex(longId);
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object attribute) {
        if (!containsType(key)) return IterablesUtil.emptyIterable();
        else return (Iterable) getVertices(getPropertyKey(key), attribute);
    }

    @Override
    public void removeVertex(Vertex vertex) {
        vertex.remove();
    }


    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        //Preconditions.checkArgument(id==null,"Titan does not support edge id assignment");
        Preconditions.checkArgument(outVertex instanceof TitanVertex);
        Preconditions.checkArgument(inVertex instanceof TitanVertex);
        return addEdge((TitanVertex) outVertex, (TitanVertex) inVertex, label);
    }

    @Override
    public TitanEdge addEdge(TitanVertex outVertex, TitanVertex inVertex, String label) {
        if (null == label) {
            throw new IllegalArgumentException("Edge label must be non-null");
        }
        return addEdge(outVertex, inVertex, getEdgeLabel(label));
    }


    @Override
    public Edge getEdge(Object id) {
        if (id == null) throw ExceptionFactory.edgeIdCanNotBeNull();
        RelationIdentifier rid = null;

        try {
            if (id instanceof TitanEdge) rid = (RelationIdentifier) ((TitanEdge) id).getId();
            else if (id instanceof RelationIdentifier) rid = (RelationIdentifier) id;
            else if (id instanceof String) rid = RelationIdentifier.parse((String) id);
            else if (id instanceof long[]) rid = RelationIdentifier.get((long[]) id);
            else if (id instanceof int[]) rid = RelationIdentifier.get((int[]) id);
        } catch (IllegalArgumentException e) {
            return null;
        }

        if (rid != null) return rid.findEdge(this);
        else return null;
    }

    @Override
    public void removeEdge(Edge edge) {
        edge.remove();
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        if (!containsType(key)) return IterablesUtil.emptyIterable();
        else return (Iterable) getEdges(getPropertyKey(key), value);
    }

    @Override
    public TitanProperty addProperty(TitanVertex vertex, String key, Object attribute) {
        return addProperty(vertex, getPropertyKey(key), attribute);
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
        throw new UnsupportedOperationException("Key indexes cannot currently be dropped. Create a new key instead.");
    }

    @Override
    public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, Parameter... indexParameters) {
        Preconditions.checkNotNull(key);
        Preconditions.checkArgument(elementClass == Element.class || elementClass == Vertex.class || elementClass == Edge.class,
                "Expected vertex, edge or element");

        if (indexParameters == null || indexParameters.length == 0) {
            indexParameters = new Parameter[]{new Parameter(Titan.Token.STANDARD_INDEX, "")};
        }

        if (containsType(key)) {
            TitanType type = getType(key);
            if (!type.isPropertyKey())
                throw new IllegalArgumentException("Key string does not denote a property key but a label");
            List<String> indexes = new ArrayList<String>(indexParameters.length);
            for (Parameter p : indexParameters) {
                Preconditions.checkArgument(p.getKey() instanceof String, "Invalid index argument: " + p);
                indexes.add((String) p.getKey());
            }
            boolean indexesCovered;
            if (elementClass == Element.class) {
                indexesCovered = hasIndexes((TitanKey) type, Vertex.class, indexes) &&
                        hasIndexes((TitanKey) type, Edge.class, indexes);
            } else {
                indexesCovered = hasIndexes((TitanKey) type, elementClass, indexes);
            }
            if (!indexesCovered)
                throw new UnsupportedOperationException("Cannot add an index to an already existing property key: " + type.getName());
        } else {
            KeyMaker tm = makeKey(key).dataType(Object.class);
            for (Parameter p : indexParameters) {
                Preconditions.checkArgument(p.getKey() instanceof String, "Invalid index argument: " + p);
                tm.indexed((String) p.getKey(), elementClass);
            }
            tm.make();
        }
    }

    private static final boolean hasIndexes(TitanKey key, Class<? extends Element> elementClass, List<String> indexes) {
        for (String index : indexes) {
            if (!Iterables.contains(key.getIndexes(elementClass), index)) return false;
        }
        return true;
    }

    @Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        Preconditions.checkArgument(elementClass == Vertex.class || elementClass == Edge.class, "Must provide either Vertex.class or Edge.class as an argument");

        Set<String> indexedkeys = new HashSet<String>();
        for (TitanKey k : getTypes(TitanKey.class)) {
            if (!Iterables.isEmpty(k.getIndexes(elementClass))) indexedkeys.add(k.getName());
        }
        return indexedkeys;
    }

    @Override
    public <T extends TitanType> Iterable<T> getTypes(Class<T> clazz) {
        Preconditions.checkNotNull(clazz);
        Iterable<TitanVertex> types = null;
        if (TitanKey.class.equals(clazz)) {
            types = getVertices(SystemKey.TypeClass, TitanTypeClass.KEY);
        } else if (TitanLabel.class.equals(clazz)) {
            types = getVertices(SystemKey.TypeClass, TitanTypeClass.LABEL);
        } else if (TitanType.class.equals(clazz)) {
            types = Iterables.concat(getVertices(SystemKey.TypeClass, TitanTypeClass.KEY), getVertices(SystemKey.TypeClass, TitanTypeClass.LABEL));
        } else throw new IllegalArgumentException("Unknown type class: " + clazz);
        return Iterables.filter(types, clazz);
    }

}
