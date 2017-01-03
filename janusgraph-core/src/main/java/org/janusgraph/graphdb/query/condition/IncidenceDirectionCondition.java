package org.janusgraph.graphdb.query.condition;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusEdge;
import org.janusgraph.core.JanusRelation;
import org.janusgraph.core.JanusVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IncidenceDirectionCondition<E extends JanusRelation> extends Literal<E> {

    private final Direction direction;
    private final JanusVertex otherVertex;

    public IncidenceDirectionCondition(Direction direction, JanusVertex otherVertex) {
        Preconditions.checkNotNull(direction);
        Preconditions.checkNotNull(otherVertex);
        this.direction = direction;
        this.otherVertex = otherVertex;
    }

    @Override
    public boolean evaluate(E relation) {
        return relation.isEdge() && ((JanusEdge) relation).vertex(direction).equals(otherVertex);
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
