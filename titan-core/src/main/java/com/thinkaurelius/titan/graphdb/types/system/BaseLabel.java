package com.thinkaurelius.titan.graphdb.types.system;

import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.internal.TitanSchemaCategory;
import com.tinkerpop.blueprints.Direction;

public class BaseLabel extends BaseType implements TitanLabel {

    public static final BaseLabel TypeDefinitionEdge =
            new BaseLabel("TypeRelated", 36);

    private BaseLabel(String name, int id) {
        super(name, id, TitanSchemaCategory.LABEL);
    }

    @Override
    public long[] getSignature() {
        return new long[]{BaseKey.TypeDefinitionDesc.getID()};
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

    @Override
    public boolean isUnidirected(Direction dir) {
        return dir==Direction.BOTH;
    }


}
