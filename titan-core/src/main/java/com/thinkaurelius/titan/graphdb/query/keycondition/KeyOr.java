package com.thinkaurelius.titan.graphdb.query.keycondition;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Arrays;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class KeyOr<K> implements KeyCondition<K> {

    private final KeyCondition<K>[] elements;

    public KeyOr(KeyCondition<K>... elements) {
        Preconditions.checkNotNull(elements);
        Preconditions.checkArgument(elements.length>=0);
        for (int i=0;i<elements.length;i++) Preconditions.checkNotNull(elements[i]);
        this.elements=elements;
    }

    public int size() {
        return elements.length;
    }

    public KeyCondition<K> get(int position) {
        return elements[position];
    }

    @Override
    public Type getType() {
        return Type.OR;
    }

    @Override
    public boolean hasChildren() {
        return elements.length>0;
    }

    @Override
    public Iterable<KeyCondition<K>> getChildren() {
        return Arrays.asList(elements);
    }

    @Override
    public int hashCode() {
        int sum = 0;
        for (KeyCondition kp : elements) sum+=kp.hashCode();
        return new HashCodeBuilder().append(getType()).append(sum).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        KeyOr oth = (KeyOr)other;
        if (elements.length!=oth.elements.length) return false;
        for (int i=0;i<elements.length;i++) {
            boolean foundEqual = false;
            for (int j=0;j<elements.length;j++) {
                if (elements[i].equals(oth.elements[(i+j)%elements.length])) {
                    foundEqual=true;
                    break;
                }
            }
            if (!foundEqual) return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("(");
        for (int i=0;i<elements.length;i++) {
            if (i>0) b.append(" OR ");
            b.append(elements[i]);
        }
        b.append(")");
        return b.toString();
    }

    public static final<K> KeyOr<K> of(KeyCondition<K>... elements) {
        return new KeyOr<K>(elements);
    }

}