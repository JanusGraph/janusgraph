package com.thinkaurelius.titan.graphdb.types.vertices;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;
import com.tinkerpop.blueprints.Direction;

public class TitanKeyVertex extends TitanTypeVertex implements TitanKey {

    public TitanKeyVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    //############## IDENTICAL TO TitanKeyReference

    @Override
    public Class<?> getDataType() {
        return getDefinition().getValue(TypeDefinitionCategory.DATATYPE,Class.class);
    }

    @Override
    public Cardinality getCardinality() {
        return super.getMultiplicity().getCardinality();
    }

    @Override
    public final boolean isPropertyKey() {
        return true;
    }

    @Override
    public final boolean isEdgeLabel() {
        return false;
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        return dir==Direction.OUT;
    }

    @Override
    public Integer getTtl() {
        return null;
    }
}
