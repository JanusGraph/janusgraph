package com.thinkaurelius.titan.graphdb.types.system;

import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;

public class SystemLabel extends SystemRelationType implements TitanLabel {

    public static final SystemLabel TypeDefinitionEdge =
            new SystemLabel("TypeRelated", 6);

    private SystemLabel(String name, int id) {
        super(name, id, RelationCategory.EDGE);
    }

    @Override
    public long[] getSignature() {
        return new long[]{SystemKey.TypeDefinitionDesc.getID()};
    }

    @Override
    public Multiplicity getMultiplicity() {
        return Multiplicity.MULTI;
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
