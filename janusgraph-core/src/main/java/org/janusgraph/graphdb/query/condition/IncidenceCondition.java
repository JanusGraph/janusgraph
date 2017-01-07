package org.janusgraph.graphdb.query.condition;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertex;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IncidenceCondition<E extends JanusGraphRelation> extends Literal<E> {

    private final JanusGraphVertex baseVertex;
    private final JanusGraphVertex otherVertex;

    public IncidenceCondition(JanusGraphVertex baseVertex, JanusGraphVertex otherVertex) {
        Preconditions.checkNotNull(baseVertex);
        Preconditions.checkNotNull(otherVertex);
        this.baseVertex = baseVertex;
        this.otherVertex = otherVertex;
    }

    @Override
    public boolean evaluate(E relation) {
        return relation.isEdge() && ((JanusGraphEdge) relation).otherVertex(baseVertex).equals(otherVertex);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(baseVertex).append(otherVertex).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other==null || !getClass().isInstance(other))
            return false;

        IncidenceCondition oth = (IncidenceCondition)other;
        return baseVertex.equals(oth.baseVertex) && otherVertex.equals(oth.otherVertex);
    }

    @Override
    public String toString() {
        return "incidence["+ baseVertex + "-" + otherVertex + "]";
    }
}
