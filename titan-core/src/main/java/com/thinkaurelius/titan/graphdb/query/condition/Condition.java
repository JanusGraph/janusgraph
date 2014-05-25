package com.thinkaurelius.titan.graphdb.query.condition;

import com.google.common.base.Function;
import com.thinkaurelius.titan.core.TitanElement;

/**
 * A logical condition which evaluates against a provided element to true or false.
 * </p>
 * A condition can be nested to form complex logical expressions with AND, OR and NOT.
 * A condition is either a literal, a negation of a condition, or a logical combination of conditions (AND, OR).
 * If a condition has sub-conditions we consider those to be children.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Condition<E extends TitanElement> {

    public enum Type { AND, OR, NOT, LITERAL}

    public Type getType();

    public Iterable<Condition<E>> getChildren();

    public boolean hasChildren();

    public int numChildren();

    public boolean evaluate(E element);

    public int hashCode();

    public boolean equals(Object other);

    public String toString();

}
