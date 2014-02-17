package com.thinkaurelius.titan.graphdb.internal;

import com.thinkaurelius.titan.core.TitanProperty;
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
            case VERTEX: return Vertex.class;
            case EDGE: return Edge.class;
            case PROPERTY: return TitanProperty.class;
            default: throw new IllegalArgumentException();
        }
    }

    public String getName() {
        switch(this) {
            case VERTEX: return "vertex";
            case EDGE: return "edge";
            case PROPERTY: return "property";
            default: throw new IllegalArgumentException();
        }
    }

    public static final ElementCategory getByName(final String name) {
        if (name.equalsIgnoreCase("vertex")) return VERTEX;
        else if (name.equalsIgnoreCase("edge")) return EDGE;
        else if (name.equalsIgnoreCase("property")) return PROPERTY;
        else throw new IllegalArgumentException("Unrecognized name: " + name);
    }
}
