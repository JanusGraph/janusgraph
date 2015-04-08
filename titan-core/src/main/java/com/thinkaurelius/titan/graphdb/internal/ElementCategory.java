package com.thinkaurelius.titan.graphdb.internal;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanSchemaType;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.thinkaurelius.titan.graphdb.types.VertexLabelVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.EdgeLabelVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.PropertyKeyVertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
public enum ElementCategory {
    VERTEX, EDGE, PROPERTY;

    public Class<? extends Element> getElementType() {
        switch(this) {
            case VERTEX: return TitanVertex.class;
            case EDGE: return TitanEdge.class;
            case PROPERTY: return TitanVertexProperty.class;
            default: throw new IllegalArgumentException();
        }
    }

    public boolean isRelation() {
        switch(this) {
            case VERTEX: return false;
            case EDGE:
            case PROPERTY: return true;
            default: throw new IllegalArgumentException();
        }
    }

    public boolean isValidConstraint(TitanSchemaType type) {
        Preconditions.checkNotNull(type);
        switch(this) {
            case VERTEX: return (type instanceof VertexLabelVertex);
            case EDGE: return (type instanceof EdgeLabelVertex);
            case PROPERTY: return (type instanceof PropertyKeyVertex);
            default: throw new IllegalArgumentException();
        }
    }

    public boolean matchesConstraint(TitanSchemaType type, TitanElement element) {
        Preconditions.checkArgument(type != null && element!=null);
        assert isInstance(element);
        assert isValidConstraint(type);
        switch(this) {
            case VERTEX: return ((TitanVertex)element).vertexLabel().equals(type);
            case EDGE: return ((TitanEdge)element).edgeLabel().equals(type);
            case PROPERTY: return ((TitanVertexProperty)element).propertyKey().equals(type);
            default: throw new IllegalArgumentException();
        }
    }

    public boolean isInstance(TitanElement element) {
        Preconditions.checkNotNull(element);
        return getElementType().isAssignableFrom(element.getClass());
    }

    public boolean subsumedBy(Class<? extends Element> clazz) {
        return clazz.isAssignableFrom(getElementType());
    }

    public String getName() {
        return toString().toLowerCase();
    }

    public TitanElement retrieve(Object elementId, TitanTransaction tx) {
        Preconditions.checkArgument(elementId!=null,"Must provide elementId");
        switch (this) {
            case VERTEX:
                Preconditions.checkArgument(elementId instanceof Long);
                return tx.getVertex(((Long) elementId).longValue());
            case EDGE:
                Preconditions.checkArgument(elementId instanceof RelationIdentifier);
                return ((RelationIdentifier)elementId).findEdge(tx);
            case PROPERTY:
                Preconditions.checkArgument(elementId instanceof RelationIdentifier);
                return ((RelationIdentifier)elementId).findProperty(tx);
            default: throw new IllegalArgumentException();
        }
    }

    public static final ElementCategory getByClazz(Class<? extends Element> clazz) {
        Preconditions.checkArgument(clazz!=null,"Need to provide a element class argument");
        if (Vertex.class.isAssignableFrom(clazz)) return VERTEX;
        else if (Edge.class.isAssignableFrom(clazz)) return EDGE;
        else if (TitanVertexProperty.class.isAssignableFrom(clazz)) return PROPERTY;
        else throw new IllegalArgumentException("Invalid clazz provided: " + clazz);
    }

    public static final ElementCategory getByName(final String name) {
        for (ElementCategory category : values()) {
            if (category.toString().equalsIgnoreCase(name)) return category;
        }
        throw new IllegalArgumentException("Unrecognized name: " + name);
    }
}
