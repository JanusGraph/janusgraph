package com.thinkaurelius.titan.graphdb.query.condition;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class DirectionCondition<E extends TitanRelation> extends Literal<E> {

    private final TitanVertex vertex;
    private final int relationPos;

    public DirectionCondition(TitanVertex vertex, Direction dir) {
        Preconditions.checkNotNull(vertex);
        Preconditions.checkNotNull(dir);
        this.vertex = vertex;
        if (dir==Direction.BOTH) relationPos =-1;
        else this.relationPos = EdgeDirection.position(dir);
    }

    @Override
    public boolean evaluate(E element) {
        if (relationPos <0) return true;
        return ((InternalRelation)element).getVertex(relationPos).equals(vertex);
    }

    public Direction getDirection() {
        if (relationPos <0) return Direction.BOTH;
        else return EdgeDirection.fromPosition(relationPos);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(relationPos).append(vertex).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        DirectionCondition oth = (DirectionCondition)other;
        return relationPos==oth.relationPos && vertex.equals(oth.vertex);
    }

    @Override
    public String toString() {
        return "dir["+getDirection()+"]";
    }
}
