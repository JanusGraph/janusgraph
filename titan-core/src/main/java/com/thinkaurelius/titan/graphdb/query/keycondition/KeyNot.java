package com.thinkaurelius.titan.graphdb.query.keycondition;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class KeyNot<K> implements KeyCondition<K> {

    private final KeyCondition<K> element;

    public KeyNot(KeyCondition<K> element) {
        Preconditions.checkNotNull(element);
        this.element = element;
    }

    @Override
    public Type getType() {
        return Type.NOT;
    }

    public KeyCondition<K> getChild() {
        return element;
    }

    @Override
    public boolean hasChildren() {
        return true;
    }

    @Override
    public Iterable<KeyCondition<K>> getChildren() {
        return ImmutableList.of(element);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(element).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        KeyNot oth = (KeyNot)other;
        return element.equals(oth.element);
    }


    @Override
    public String toString() {
        return "!("+element.toString()+")";
    }

    public static final<K> KeyNot<K> of(KeyCondition<K> element) {
        return new KeyNot<K>(element);
    }

}
