package org.janusgraph.graphdb.query.condition;

import java.util.Collections;

import org.janusgraph.core.JanusElement;

/**
 * Abstract literal condition.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class Literal<E extends JanusElement> implements Condition<E> {

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
        return Collections.EMPTY_LIST;
    }

}
