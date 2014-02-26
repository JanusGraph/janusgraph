package com.thinkaurelius.titan.graphdb.types.vertices;

import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;

public class TitanLabelVertex extends TitanRelationTypeVertex implements TitanLabel {

    public TitanLabelVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    //######## IDENTICAL TO TitanLabelReference

    public boolean isDirected() {
        return !isUnidirected();
    }

    public boolean isUnidirected() {
        return getDefinition().getValue(TypeDefinitionCategory.UNIDIRECTIONAL,Boolean.class);
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
    public boolean invertedBaseDirection() {
        return getDefinition().getValue(TypeDefinitionCategory.INVERTED_DIRECTION,Boolean.class);
    }

}
