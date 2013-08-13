package com.thinkaurelius.titan.graphdb.query.condition;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.TitanVertex;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IncidenceCondition<E extends TitanRelation> extends Literal<E> {

    private final TitanVertex vertex;

    public IncidenceCondition(TitanVertex vertex) {
        Preconditions.checkNotNull(vertex);
        this.vertex = vertex;
    }

    @Override
    public boolean evaluate(E element) {
        return element.isIncidentOn(vertex);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(vertex).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        IncidenceCondition oth = (IncidenceCondition)other;
        return vertex.equals(oth.vertex);
    }

    @Override
    public String toString() {
        return "incidence["+vertex+"]";
    }
}
