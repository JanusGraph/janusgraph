package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.util.datastructures.ImmutableLongObjectMap;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.nio.ByteBuffer;

/**
 * {@link Entry} implementation for single-threaded use.
 * <p/>
 * Meant for writing the transactional state and high performance single-threaded use cases.
 * Use {@link CacheEntry} in multi-threaded context.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class SimpleEntry implements Entry {

    private final ByteBuffer column;
    private final ByteBuffer value;

    public SimpleEntry(ByteBuffer column, ByteBuffer value) {
        Preconditions.checkNotNull(column);
        this.column = column;
        this.value = value;
    }

    public static Entry of(ByteBuffer column, ByteBuffer value) {
        return new SimpleEntry(column,value);
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
        return new HashCodeBuilder().append(getColumn()).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (!(obj instanceof Entry)) return false;
        Entry other = (Entry) obj;
        return getColumn().equals(other.getColumn());
    }

    @Override
    public int compareTo(Entry entry) {
        return ByteBufferUtil.compare(getColumn(),entry.getColumn());
    }

}
