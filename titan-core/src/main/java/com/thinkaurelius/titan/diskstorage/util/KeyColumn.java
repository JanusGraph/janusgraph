package com.thinkaurelius.titan.diskstorage.util;

import java.nio.ByteBuffer;

/**
 * This is just a (key, column) pair.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class KeyColumn {

    private final ByteBuffer key;
    private final ByteBuffer col;
    private int cachedHashCode;

    public KeyColumn(ByteBuffer key, ByteBuffer col) {
        this.key = key;
        this.col = col;

        assert null != this.key;
        assert null != this.col;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public ByteBuffer getColumn() {
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
        return "KeyColumn [k=0x" + ByteBufferUtil.bytesToHex(key) +
                ", c=0x" + ByteBufferUtil.bytesToHex(col) + "]";
    }
}