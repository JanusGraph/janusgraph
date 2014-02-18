package com.thinkaurelius.titan.graphdb.query.condition;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.TitanType;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class RelationTypeCondition<E extends TitanRelation> extends Literal<E> {

    private final TitanType relationType;

    public RelationTypeCondition(TitanType relationType) {
        Preconditions.checkNotNull(relationType);
        this.relationType = relationType;
    }

    @Override
    public boolean evaluate(E element) {
        return relationType.equals(element.getType());
    }

    public TitanType getRelationType() {
        return relationType;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(relationType).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || !(other == null || !getClass().isInstance(other)) && relationType.equals(((RelationTypeCondition) other).relationType);
    }

    @Override
    public String toString() {
        return "type["+ relationType.toString()+"]";
    }
}
