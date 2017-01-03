package org.janusgraph.graphdb.query.condition;

import org.janusgraph.core.JanusElement;

/**
 * Combines multiple conditions under semantic OR, i.e. at least one condition must be true for this combination to be true
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class Or<E extends JanusElement> extends MultiCondition<E> {

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

    public static <E extends JanusElement> Or<E> of(Condition<E>... elements) {
        return new Or<E>(elements);
    }

}
