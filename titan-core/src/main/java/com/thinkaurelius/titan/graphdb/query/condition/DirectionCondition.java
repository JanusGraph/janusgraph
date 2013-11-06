package com.thinkaurelius.titan.graphdb.query.condition;

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

    private final TitanVertex vertex;
    private final int relationPos;
    private final Direction direction;

    public DirectionCondition(TitanVertex vertex, Direction dir) {
        assert vertex != null && dir != null;
        this.vertex = vertex;
        this.direction = dir;
        this.relationPos = (dir == Direction.BOTH) ? -1 : EdgeDirection.position(dir);
    }

    @Override
    public boolean evaluate(E element) {
        if (relationPos<0) return true;
        if (element instanceof CacheEdge) {
            return direction==((CacheEdge)element).getVertexCentricDirection();
        }
        return ((InternalRelation) element).getVertex(relationPos).equals(vertex);
    }

    public Direction getDirection() {
        return direction;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(relationPos).append(vertex).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || !getClass().isInstance(other))
            return false;

        DirectionCondition oth = (DirectionCondition)other;
        return relationPos == oth.relationPos && vertex.equals(oth.vertex);
    }

    @Override
    public String toString() {
        return "dir["+getDirection()+"]";
    }
}
