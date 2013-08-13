package com.thinkaurelius.titan.graphdb.query.condition;

import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class HiddenFilterCondition<E extends TitanRelation> extends Literal<E> {

    public HiddenFilterCondition() {
    }

    @Override
    public boolean evaluate(E element) {
        return !((InternalType)element.getType()).isHidden();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        return true;
    }

    @Override
    public String toString() {
        return "!hidden";
    }
}
