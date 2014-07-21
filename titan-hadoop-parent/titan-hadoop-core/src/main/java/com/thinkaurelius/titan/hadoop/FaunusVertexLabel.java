package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.graphdb.internal.InternalVertexLabel;
import com.thinkaurelius.titan.graphdb.schema.VertexLabelDefinition;
import com.thinkaurelius.titan.graphdb.types.system.BaseVertexLabel;
import com.thinkaurelius.titan.graphdb.types.system.EmptyVertex;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FaunusVertexLabel extends EmptyVertex implements InternalVertexLabel {

    public static FaunusVertexLabel DEFAULT_VERTEXLABEL = new FaunusVertexLabel(
            new VertexLabelDefinition(BaseVertexLabel.DEFAULT_VERTEXLABEL.getName(), FaunusElement.NO_ID,false,false));

    private final VertexLabelDefinition definition;

    public FaunusVertexLabel(VertexLabelDefinition definition) {
        this.definition=definition;
    }

    public boolean isDefault() {
        return getName().equals(BaseVertexLabel.DEFAULT_VERTEXLABEL.getName());
    }

    @Override
    public boolean isPartitioned() {
        return definition.isPartitioned();
    }

    @Override
    public boolean isStatic() {
        return definition.isStatic();
    }

    @Override
    public String getName() {
        return definition.getName();
    }

    @Override
    public boolean hasDefaultConfiguration() {
        return definition.hasDefaultConfiguration();
    }

    @Override
    public int getTTL() {
        return 0;
    }

    @Override
    public String toString() {
        return getName();
    }

    @Override
    public long getLongId() {
        return definition.getLongId();
    }

    @Override
    public boolean hasId() {
        return true;
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (this==oth) return true;
        else if (oth==null || !(oth instanceof VertexLabel)) return false;
        return getName().equals(((VertexLabel)oth).getName());
    }

}
