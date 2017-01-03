package org.janusgraph.graphdb.types.vertices;

import org.janusgraph.core.EdgeLabel;
import org.janusgraph.graphdb.transaction.StandardJanusTx;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;
import org.apache.tinkerpop.gremlin.structure.Direction;

public class EdgeLabelVertex extends RelationTypeVertex implements EdgeLabel {

    public EdgeLabelVertex(StandardJanusTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    @Override
    public boolean isDirected() {
        return isUnidirected(Direction.BOTH);
    }

    @Override
    public boolean isUnidirected() {
        return isUnidirected(Direction.OUT);

    }

    @Override
    public boolean isUnidirected(Direction dir) {
        return getDefinition().getValue(TypeDefinitionCategory.UNIDIRECTIONAL,Direction.class)==dir;
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
