package com.thinkaurelius.titan.graphdb.types.system;

import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;

public class SystemLabel extends SystemType implements TitanLabel {

    public static final SystemLabel TypeDefinitionEdge =
            new SystemLabel("TypeRelated", 6);

    private SystemLabel(String name, int id) {
        super(name, id, RelationCategory.EDGE, new boolean[]{false,false}, true);
    }

    @Override
    public long[] getSignature() {
        return new long[]{SystemKey.TypeDefinitionDesc.getID()};
    }

    @Override
    public final boolean isPropertyKey() {
        return false;
    }

    @Override
    public final boolean isEdgeLabel() {
        return true;
    }

    @Override
    public boolean isDirected() {
        return true;
    }

    @Override
    public boolean isUnidirected() {
        return false;
    }
}
