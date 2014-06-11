package com.thinkaurelius.titan.graphdb.types.system;

import com.thinkaurelius.titan.graphdb.internal.InternalVertexLabel;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class BaseVertexLabel extends EmptyVertex implements InternalVertexLabel {

    public static final BaseVertexLabel DEFAULT_VERTEXLABEL = new BaseVertexLabel("_default");

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
    public String getName() {
        return name;
    }

    @Override
    public boolean hasDefaultConfiguration() {
        return true;
    }

    @Override
    public String toString() {
        return getName();
    }


}
