package com.thinkaurelius.titan.graphdb.query.condition;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.TitanType;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class LabelCondition<E extends TitanRelation> extends Literal<E> {

    private final TitanType label;

    public LabelCondition(TitanType label) {
        Preconditions.checkNotNull(label);
        this.label=label;
    }

    @Override
    public boolean evaluate(E element) {
        return label.equals(element.getType());
    }

    public TitanType getLabel() {
        return label;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(label).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || !(other == null || !getClass().isInstance(other)) && label.equals(((LabelCondition) other).label);
    }

    @Override
    public String toString() {
        return "label["+label.toString()+"]";
    }
}
