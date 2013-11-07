package com.thinkaurelius.titan.graphdb.query.condition;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

import com.thinkaurelius.titan.core.TitanElement;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.ArrayList;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class MultiCondition<E extends TitanElement> extends ArrayList<Condition<E>> implements Condition<E> {

    MultiCondition() {
        this(5);
    }

    MultiCondition(int capacity) {
        super(capacity);
    }

    MultiCondition(final Condition<E>... conditions) {
        super(conditions.length);
        for (Condition<E> condition : conditions) {
            assert condition != null;
            super.add(condition);
        }
    }

    MultiCondition(MultiCondition<E> cond) {
        this(cond.size());
        super.addAll(cond);
    }

    public boolean add(Condition<E> condition) {
        assert condition != null;
        return super.add(condition);
    }

    public int size() {
        return super.size();
    }

    public Condition<E> get(int position) {
        return super.get(position);
    }

    @Override
    public boolean hasChildren() {
        return !super.isEmpty();
    }

    @Override
    public int numChildren() {
        return super.size();
    }

    @Override
    public Iterable<Condition<E>> getChildren() {
        return this;
    }

//    @Override
//    public Type getType() {
//        return Type.AND;
//    }

    @Override
    public int hashCode() {
        int sum = 0;
        for (Condition kp : this) sum += kp.hashCode();
        return new HashCodeBuilder().append(getType()).append(sum).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || !getClass().isInstance(other))
            return false;

        MultiCondition oth = (MultiCondition)other;
        if (getType() != oth.getType() || size() != oth.size())
            return false;

        for (int i = 0; i < size(); i++) {
            boolean foundEqual = false;
            for (int j = 0; j < oth.size(); j++) {
                if (get(i).equals(oth.get((i + j) % oth.size()))) {
                    foundEqual = true;
                    break;
                }
            }

            if (!foundEqual)
                return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return toString(getType().toString());
    }

    public String toString(String token) {
        StringBuilder b = new StringBuilder();
        b.append("(");
        for (int i = 0; i < size(); i++) {
            if (i > 0) b.append(" ").append(token).append(" ");
            b.append(get(i));
        }
        b.append(")");
        return b.toString();
    }

}
