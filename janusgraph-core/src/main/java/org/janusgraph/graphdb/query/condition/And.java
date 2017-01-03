package org.janusgraph.graphdb.query.condition;

import org.janusgraph.core.JanusElement;

/**
 * Combines multiple conditions under semantic AND, i.e. all conditions must be true for this combination to be true
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class And<E extends JanusElement> extends MultiCondition<E> {

    public And(Condition<E>... elements) {
        super(elements);
    }

    public And() {
        super();
    }

    private And(And<E> clone) {
        super(clone);
    }

    @Override
    public And<E> clone() {
        return new And<E>(this);
    }

    public And(int capacity) {
        super(capacity);
    }

    @Override
    public Type getType() {
        return Type.AND;
    }

    @Override
    public boolean evaluate(E element) {
        for (Condition<E> condition : this) {
            if (!condition.evaluate(element))
                return false;
        }

        return true;
    }

    public static <E extends JanusElement> And<E> of(Condition<E>... elements) {
        return new And<E>(elements);
    }

}
