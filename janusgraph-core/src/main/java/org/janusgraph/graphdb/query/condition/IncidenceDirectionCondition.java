package org.janusgraph.graphdb.query.condition;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IncidenceDirectionCondition<E extends JanusGraphRelation> extends Literal<E> {

    private final Direction direction;
    private final JanusGraphVertex otherVertex;

    public IncidenceDirectionCondition(Direction direction, JanusGraphVertex otherVertex) {
        Preconditions.checkNotNull(direction);
        Preconditions.checkNotNull(otherVertex);
        this.direction = direction;
        this.otherVertex = otherVertex;
    }

    @Override
    public boolean evaluate(E relation) {
        return relation.isEdge() && ((JanusGraphEdge) relation).vertex(direction).equals(otherVertex);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(direction).append(otherVertex).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other==null || !getClass().isInstance(other))
            return false;

        IncidenceDirectionCondition oth = (IncidenceDirectionCondition)other;
        return direction==oth.direction && otherVertex.equals(oth.otherVertex);
    }

    @Override
    public String toString() {
        return "incidence["+ direction + "-" + otherVertex + "]";
    }
}
