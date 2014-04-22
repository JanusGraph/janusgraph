package com.thinkaurelius.titan.graphdb.transaction.lock;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class LockTuple {

    private final Object[] elements;

    public LockTuple(Object... elements) {
        Preconditions.checkArgument(elements!=null && elements.length>0);
        for (Object o : elements) Preconditions.checkNotNull(o);
        this.elements=elements;
    }

    public int size() {
        return elements.length;
    }

    public Object get(int pos) {
        return elements[pos];
    }

    public Object[] getAll() {
        return elements;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder b = new HashCodeBuilder();
        for (Object o : elements) b.append(o);
        return b.toHashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (this==oth) return true;
        else if (oth==null || !(oth instanceof LockTuple)) return false;
        LockTuple other = (LockTuple)oth;
        if (elements.length!=other.elements.length) return false;
        for (int i=0;i<elements.length;i++) if (!elements[i].equals(other.elements[i])) return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[");
        for (int i = 0; i < elements.length; i++) {
            if (i>0) b.append(",");
            b.append(elements[i].toString());
        }
        b.append("]");
        return b.toString();
    }


}
