package org.janusgraph.graphdb.internal;

import com.google.common.base.Preconditions;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusSchemaType;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.types.VertexLabelVertex;
import org.janusgraph.graphdb.types.vertices.EdgeLabelVertex;
import org.janusgraph.graphdb.types.vertices.PropertyKeyVertex;
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
            case VERTEX: return JanusVertex.class;
            case EDGE: return JanusEdge.class;
            case PROPERTY: return JanusVertexProperty.class;
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

    public boolean isValidConstraint(JanusSchemaType type) {
        Preconditions.checkNotNull(type);
        switch(this) {
            case VERTEX: return (type instanceof VertexLabelVertex);
            case EDGE: return (type instanceof EdgeLabelVertex);
            case PROPERTY: return (type instanceof PropertyKeyVertex);
            default: throw new IllegalArgumentException();
        }
    }

    public boolean matchesConstraint(JanusSchemaType type, JanusElement element) {
        Preconditions.checkArgument(type != null && element!=null);
        assert isInstance(element);
        assert isValidConstraint(type);
        switch(this) {
            case VERTEX: return ((JanusVertex)element).vertexLabel().equals(type);
            case EDGE: return ((JanusEdge)element).edgeLabel().equals(type);
            case PROPERTY: return ((JanusVertexProperty)element).propertyKey().equals(type);
            default: throw new IllegalArgumentException();
        }
    }

    public boolean isInstance(JanusElement element) {
        Preconditions.checkNotNull(element);
        return getElementType().isAssignableFrom(element.getClass());
    }

    public boolean subsumedBy(Class<? extends Element> clazz) {
        return clazz.isAssignableFrom(getElementType());
    }

    public String getName() {
        return toString().toLowerCase();
    }

    public JanusElement retrieve(Object elementId, JanusTransaction tx) {
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
        else if (JanusVertexProperty.class.isAssignableFrom(clazz)) return PROPERTY;
        else throw new IllegalArgumentException("Invalid clazz provided: " + clazz);
    }

    public static final ElementCategory getByName(final String name) {
        for (ElementCategory category : values()) {
            if (category.toString().equalsIgnoreCase(name)) return category;
        }
        throw new IllegalArgumentException("Unrecognized name: " + name);
    }
}
