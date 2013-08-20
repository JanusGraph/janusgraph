package com.thinkaurelius.titan.graphdb.internal;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
public enum ElementType {
    VERTEX, EDGE;

    public Class<? extends Element> getElementType() {
        switch(this) {
            case VERTEX: return Vertex.class;
            case EDGE: return Edge.class;
            default: throw new IllegalArgumentException();
        }
    }

    public String getName() {
        switch(this) {
            case VERTEX: return "vertex";
            case EDGE: return "edge";
            default: throw new IllegalArgumentException();
        }
    }

    public static final ElementType getByName(final String name) {
        if (name.equalsIgnoreCase("vertex")) return VERTEX;
        else if (name.equalsIgnoreCase("edge")) return EDGE;
        else throw new IllegalArgumentException("Unrecognized name: " + name);
    }
}
