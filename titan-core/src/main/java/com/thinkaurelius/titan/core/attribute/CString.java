package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;

import java.io.Serializable;

/**
 * A custom String class used for byte-order preserving String representations in the database.
 * This class has to be used as the datatype for property keys that
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class CString implements Serializable, Comparable<CString>, CharSequence{

    private final String value;

    public CString(final String value) {
        Preconditions.checkNotNull(value);
        this.value = value;
    }

    public String getString() {
        return value;
    }

    public int length() {
        return value.length();
    }

    @Override
    public char charAt(int index) {
        return value.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return value.subSequence(start,end);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) return true;
        else if (other == null) return false;
        else if ((other instanceof CString) || (other instanceof String)) {
            return value.equals(other.toString());
        } else return false;
    }

    @Override
    public int compareTo(CString o) {
        return value.compareTo(o.value);
    }
}
