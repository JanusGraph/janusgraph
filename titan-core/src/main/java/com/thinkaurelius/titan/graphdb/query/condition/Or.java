package com.thinkaurelius.titan.graphdb.query.condition;

import com.thinkaurelius.titan.core.TitanElement;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class Or<E extends TitanElement> extends MultiCondition<E> {

    public Or(Condition<E>... elements) {
        super(elements);
    }

    public Or(int size) {
        super(size);
    }

    public Or() {
        super();
    }

    private Or(And<E> clone) {
        super(clone);
    }

    @Override
    public Or<E> clone() {
        return new Or<E>(this);
    }

    @Override
    public Type getType() {
        return Type.OR;
    }

    @Override
    public boolean evaluate(E element) {
        if (!hasChildren())
            return true;

        for (Condition<E> condition : this) {
            if (condition.evaluate(element))
                return true;
        }

        return false;
    }

    public static <E extends TitanElement> Or<E> of(Condition<E>... elements) {
        return new Or<E>(elements);
    }

}