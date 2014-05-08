package com.thinkaurelius.titan.graphdb.query.condition;

import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Evaluates to false for hidden elements - used to filter out hidden elements in queries.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class HiddenFilterCondition<E extends TitanElement> extends Literal<E> {

    public HiddenFilterCondition() {
    }

    @Override
    public boolean evaluate(E element) {
        return !((InternalElement)element).isHidden();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || !(other == null || !getClass().isInstance(other));

    }

    @Override
    public String toString() {
        return "!hidden";
    }
}
