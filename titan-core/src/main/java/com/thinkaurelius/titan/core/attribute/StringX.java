package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StringX {

    private final String value;

    public StringX(final String value) {
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
        else if ((other instanceof StringX) || (other instanceof String)) {
            return value.equals(other.toString());
        } else return false;
    }
}
