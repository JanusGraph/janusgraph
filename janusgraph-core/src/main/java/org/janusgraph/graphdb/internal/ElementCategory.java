// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.internal;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.schema.JanusGraphSchemaType;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.relations.RelationIdentifierUtils;
import org.janusgraph.graphdb.types.VertexLabelVertex;
import org.janusgraph.graphdb.types.vertices.EdgeLabelVertex;
import org.janusgraph.graphdb.types.vertices.PropertyKeyVertex;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
public enum ElementCategory {
    VERTEX, EDGE, PROPERTY;

    public Class<? extends Element> getElementType() {
        switch(this) {
            case VERTEX: return JanusGraphVertex.class;
            case EDGE: return JanusGraphEdge.class;
            case PROPERTY: return JanusGraphVertexProperty.class;
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

    public boolean isValidConstraint(JanusGraphSchemaType type) {
        Preconditions.checkNotNull(type);
        switch(this) {
            case VERTEX: return (type instanceof VertexLabelVertex);
            case EDGE: return (type instanceof EdgeLabelVertex);
            case PROPERTY: return (type instanceof PropertyKeyVertex);
            default: throw new IllegalArgumentException();
        }
    }

    public boolean matchesConstraint(JanusGraphSchemaType type, JanusGraphElement element) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(element);
        assert isInstance(element);
        assert isValidConstraint(type);
        switch(this) {
            case VERTEX: return ((JanusGraphVertex)element).vertexLabel().equals(type);
            case EDGE: return ((JanusGraphEdge)element).edgeLabel().equals(type);
            case PROPERTY: return ((JanusGraphVertexProperty)element).propertyKey().equals(type);
            default: throw new IllegalArgumentException();
        }
    }

    public boolean isInstance(JanusGraphElement element) {
        Preconditions.checkNotNull(element);
        return getElementType().isAssignableFrom(element.getClass());
    }

    public boolean subsumedBy(Class<? extends Element> clazz) {
        return clazz.isAssignableFrom(getElementType());
    }

    public String getName() {
        return toString().toLowerCase();
    }

    public JanusGraphElement retrieve(Object elementId, JanusGraphTransaction tx) {
        Preconditions.checkArgument(elementId!=null,"Must provide elementId");
        switch (this) {
            case VERTEX:
                Preconditions.checkArgument(elementId instanceof Long, "elementId [%s] must be of type Long", elementId);
                return tx.getVertex((Long) elementId);
            case EDGE:
                Preconditions.checkArgument(elementId instanceof RelationIdentifier, "elementId [%s] must be of type RelationIdentifier", elementId);
                return RelationIdentifierUtils.findEdge(((RelationIdentifier)elementId), tx);
            case PROPERTY:
                Preconditions.checkArgument(elementId instanceof RelationIdentifier, "elementId [%s] must be of type RelationIdentifier", elementId);
                return RelationIdentifierUtils.findProperty(((RelationIdentifier)elementId), tx);
            default: throw new IllegalArgumentException();
        }
    }

    public static ElementCategory getByClazz(Class<? extends Element> clazz) {
        Preconditions.checkArgument(clazz!=null,"Need to provide a element class argument");
        if (Vertex.class.isAssignableFrom(clazz)) return VERTEX;
        else if (Edge.class.isAssignableFrom(clazz)) return EDGE;
        else if (JanusGraphVertexProperty.class.isAssignableFrom(clazz)) return PROPERTY;
        else throw new IllegalArgumentException("Invalid clazz provided: " + clazz);
    }

    public static ElementCategory getByName(final String name) {
        for (ElementCategory category : values()) {
            if (category.toString().equalsIgnoreCase(name)) return category;
        }
        throw new IllegalArgumentException("Unrecognized name: " + name);
    }
}
