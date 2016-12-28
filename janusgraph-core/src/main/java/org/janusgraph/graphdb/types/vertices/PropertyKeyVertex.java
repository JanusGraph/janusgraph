package org.janusgraph.graphdb.types.vertices;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.transaction.StandardTitanTx;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;
import org.apache.tinkerpop.gremlin.structure.Direction;

public class PropertyKeyVertex extends RelationTypeVertex implements PropertyKey {

    public PropertyKeyVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    @Override
    public Class<?> dataType() {
        return getDefinition().getValue(TypeDefinitionCategory.DATATYPE,Class.class);
    }

    @Override
    public Cardinality cardinality() {
        return super.multiplicity().getCardinality();
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
}
