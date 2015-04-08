package com.thinkaurelius.titan.graphdb.types.system;

import com.thinkaurelius.titan.graphdb.internal.InternalVertexLabel;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class BaseVertexLabel extends EmptyVertex implements InternalVertexLabel {

    public static final BaseVertexLabel DEFAULT_VERTEXLABEL = new BaseVertexLabel(Vertex.DEFAULT_LABEL);

    private final String name;

    public BaseVertexLabel(String name) {
        this.name = name;
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }

    @Override
    public boolean isStatic() {
        return false;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean hasDefaultConfiguration() {
        return true;
    }

    @Override
    public int getTTL() {
        return 0;
    }

    @Override
    public String toString() {
        return name();
    }
}
