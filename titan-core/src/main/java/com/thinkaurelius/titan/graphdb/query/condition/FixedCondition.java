package com.thinkaurelius.titan.graphdb.query.condition;

import com.thinkaurelius.titan.core.TitanElement;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class FixedCondition<E extends TitanElement> extends Literal<E> {

    private final boolean value;

    public FixedCondition(final boolean value) {
        this.value =value;
    }

    @Override
    public boolean evaluate(E element) {
        return value;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(value).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        return value==((FixedCondition)other).value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
