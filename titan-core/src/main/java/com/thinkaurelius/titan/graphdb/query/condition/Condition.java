package com.thinkaurelius.titan.graphdb.query.condition;

import com.google.common.base.Function;
import com.thinkaurelius.titan.core.TitanElement;

/**
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
