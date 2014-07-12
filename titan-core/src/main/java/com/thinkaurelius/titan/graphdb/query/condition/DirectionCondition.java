package com.thinkaurelius.titan.graphdb.query.condition;

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.relations.CacheEdge;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class DirectionCondition<E extends TitanRelation> extends Literal<E> {

    private final TitanVertex baseVertex;
    private final Direction direction;

    public DirectionCondition(TitanVertex vertex, Direction dir) {
        assert vertex != null && dir != null;
        this.baseVertex = vertex;
        this.direction = dir;
    }

    @Override
    public boolean evaluate(E element) {
        if (direction==Direction.BOTH) return true;
        if (element instanceof CacheEdge) {
            return direction==((CacheEdge)element).getVertexCentricDirection();
        } else if (element instanceof TitanEdge) {
            return ((TitanEdge)element).getVertex(direction).equals(baseVertex);
        } else if (element instanceof TitanProperty) {
            return direction==Direction.OUT;
        }
        return false;
    }

    public Direction getDirection() {
        return direction;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(direction).append(baseVertex).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || !getClass().isInstance(other))
            return false;

        DirectionCondition oth = (DirectionCondition)other;
        return direction == oth.direction && baseVertex.equals(oth.baseVertex);
    }

    @Override
    public String toString() {
        return "dir["+getDirection()+"]";
    }
}
