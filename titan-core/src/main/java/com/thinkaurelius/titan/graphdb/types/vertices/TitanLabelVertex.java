package com.thinkaurelius.titan.graphdb.types.vertices;

import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.TypeAttributeType;

public class TitanLabelVertex extends TitanTypeVertex implements TitanLabel {

    public TitanLabelVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    public boolean isDirected() {
        return !isUnidirected();
    }

    public boolean isUnidirected() {
        return getDefinition().getValue(TypeAttributeType.UNIDIRECTIONAL,boolean.class);
    }

    @Override
    public final boolean isPropertyKey() {
        return false;
    }

    @Override
    public final boolean isEdgeLabel() {
        return true;
    }

}
