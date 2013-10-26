package com.thinkaurelius.titan.graphdb.query.condition;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanElement;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class Not<E extends TitanElement> implements Condition<E> {

    private final Condition<E> condition;

    public Not(Condition<E> condition) {
        Preconditions.checkNotNull(condition);
        this.condition = condition;
    }

    @Override
    public Type getType() {
        return Type.NOT;
    }

    public Condition<E> getChild() {
        return condition;
    }

    @Override
    public boolean hasChildren() {
        return true;
    }

    @Override
    public int numChildren() {
        return 1;
    }

    @Override
    public boolean evaluate(E element) {
        return !condition.evaluate(element);
    }

    @Override
    public Iterable<Condition<E>> getChildren() {
        return ImmutableList.of(condition);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(condition).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || !(other == null || !getClass().isInstance(other)) && condition.equals(((Not) other).condition);

    }

    @Override
    public String toString() {
        return "!("+ condition.toString()+")";
    }

    public static <E extends TitanElement> Not<E> of(Condition<E> element) {
        return new Not<E>(element);
    }

}
