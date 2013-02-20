package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.util.datastructures.ImmutableLongObjectMap;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.nio.ByteBuffer;

/**
 * An entry is the primitive persistence unit used in the graph database middleware.
 * <p/>
 * An entry consists of a column and value both of which are general {@link java.nio.ByteBuffer}s.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class Entry implements Comparable<Entry> {

    private final ByteBuffer column;
    private final ByteBuffer value;

    private volatile transient ImmutableLongObjectMap cache;

    public Entry(ByteBuffer column, ByteBuffer value) {
        Preconditions.checkNotNull(column);
        this.column = column;
        this.value = value;
        this.cache = null;
    }

    /**
     * Returns the column ByteBuffer of this entry.
     *
     * @return Column ByteBuffer
     */
    public ByteBuffer getColumn() {
        return column;
    }


    /**
     * Returns the value ByteBuffer of this entry.
     *
     * @return Value ByteBuffer
     */
    public ByteBuffer getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(column).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (!getClass().isInstance(obj)) return false;
        Entry other = (Entry) obj;
        return column.equals(other.column);
    }

    @Override
    public int compareTo(Entry entry) {
        return ByteBufferUtil.compare(column,entry.column);
    }

    public ImmutableLongObjectMap getCache() {
        return cache;
    }

    public void setCache(ImmutableLongObjectMap cache) {
        Preconditions.checkNotNull(cache);
        this.cache=cache;
    }

}
