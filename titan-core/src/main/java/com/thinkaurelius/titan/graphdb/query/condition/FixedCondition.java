package com.thinkaurelius.titan.graphdb.query.condition;

import com.thinkaurelius.titan.core.TitanElement;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * A fixed valued literal, which always returns either true or false irrespective of the element which is evaluated.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FixedCondition<E extends TitanElement> extends Literal<E> {

    private final boolean value;

    public FixedCondition(final boolean value) {
        this.value = value;
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
        return this == other || !(other == null || !getClass().isInstance(other)) && value == ((FixedCondition) other).value;

    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
