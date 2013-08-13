package com.thinkaurelius.titan.graphdb.query.condition;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanElement;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

abstract class Literal<E extends TitanElement> implements Condition<E> {

    @Override
    public Type getType() {
        return Type.LITERAL;
    }

    @Override
    public boolean hasChildren() {
        return false;
    }

    @Override
    public int numChildren() {
        return 0;
    }

    @Override
    public Iterable<Condition<E>> getChildren() {
        return ImmutableList.of();
    }

}
