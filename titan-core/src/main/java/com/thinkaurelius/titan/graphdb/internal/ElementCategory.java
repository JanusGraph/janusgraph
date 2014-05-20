package com.thinkaurelius.titan.graphdb.internal;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanSchemaType;
import com.thinkaurelius.titan.graphdb.types.VertexLabelVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.EdgeLabelVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.PropertyKeyVertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
public enum ElementCategory {
    VERTEX, EDGE, PROPERTY;

    public Class<? extends Element> getElementType() {
        switch(this) {
            case VERTEX: return TitanVertex.class;
            case EDGE: return TitanEdge.class;
            case PROPERTY: return TitanProperty.class;
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
            case VERTEX: return ((TitanVertex)element).getVertexLabel().equals(type);
            case EDGE: return ((TitanEdge)element).getEdgeLabel().equals(type);
            case PROPERTY: return ((TitanProperty)element).getPropertyKey().equals(type);
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

    public static final ElementCategory getByClazz(Class<? extends Element> clazz) {
        Preconditions.checkArgument(clazz!=null,"Need to provide a element class argument");
        if (Vertex.class.isAssignableFrom(clazz)) return VERTEX;
        else if (Edge.class.isAssignableFrom(clazz)) return EDGE;
        else if (TitanProperty.class.isAssignableFrom(clazz)) return PROPERTY;
        else throw new IllegalArgumentException("Invalid clazz provided: " + clazz);
    }

    public static final ElementCategory getByName(final String name) {
        for (ElementCategory category : values()) {
            if (category.toString().equalsIgnoreCase(name)) return category;
        }
        throw new IllegalArgumentException("Unrecognized name: " + name);
    }
}
