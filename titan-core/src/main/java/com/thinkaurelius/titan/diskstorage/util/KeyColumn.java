package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;

/**
 * Class representing a (key, column) pair.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class KeyColumn {

    private final StaticBuffer key;
    private final StaticBuffer col;
    private int cachedHashCode;

    public KeyColumn(StaticBuffer key, StaticBuffer col) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(col);
        this.key = key;
        this.col = col;

        assert null != this.key;
        assert null != this.col;
    }

    public StaticBuffer getKey() {
        return key;
    }

    public StaticBuffer getColumn() {
        return col;
    }

    @Override
    public int hashCode() {
        // if the hashcode is needed frequently, we should store it
        if (0 != cachedHashCode)
            return cachedHashCode;

        final int prime = 31;
        int result = 1;
        result = prime * result + col.hashCode();
        result = prime * result + key.hashCode();

        // This is only thread-safe because cachedHashCode is an int and not a long
        cachedHashCode = result;

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        KeyColumn other = (KeyColumn) obj;
        return other.key.equals(key) && other.col.equals(col);
    }

    @Override
    public String toString() {
        return "KeyColumn [k=0x" + key.toString() +
                ", c=0x" + col.toString() + "]";
    }
}