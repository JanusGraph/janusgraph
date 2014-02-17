package com.thinkaurelius.titan.graphdb.query.condition;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.query.VertexCentricQueryBuilder;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Iterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ValueCondition<E extends TitanProperty> extends Literal<E> {

    private final TitanPredicate predicate;
    private final Object value;

    public ValueCondition(TitanPredicate predicate, Object value) {
        Preconditions.checkNotNull(predicate);
        this.predicate = predicate;
        this.value = value;
    }


    private boolean satisfiesCondition(Object value) {
        return predicate.evaluate(value, this.value);
    }

    @Override
    public boolean evaluate(E element) {
        return satisfiesCondition(element.getValue());
    }

    public TitanPredicate getPredicate() {
        return predicate;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(predicate).append(value).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || !getClass().isInstance(other))
            return false;

        ValueCondition oth = (ValueCondition) other;
        return predicate.equals(oth.predicate) && value.equals(oth.value);
    }

    @Override
    public String toString() {
        return "value " + predicate.toString() + " " + String.valueOf(value);
    }

    public static <E extends TitanProperty> ValueCondition<E> of(TitanPredicate titanPredicate, Object condition) {
        return new ValueCondition<E>(titanPredicate, condition);
    }

}
