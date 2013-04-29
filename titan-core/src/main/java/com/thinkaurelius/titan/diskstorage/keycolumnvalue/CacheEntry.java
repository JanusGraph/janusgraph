package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.util.datastructures.ImmutableLongObjectMap;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.nio.ByteBuffer;

/**
 * {@link Entry} implementation that is thread safe and provides a cache for the parsed
 * representation of the Entry to improve read performance.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class CacheEntry extends SimpleEntry {

    private volatile transient ImmutableLongObjectMap cache;

    public CacheEntry(ByteBuffer column, ByteBuffer value) {
        super(column,value);
        this.cache = null;
    }

    public CacheEntry(Entry e) {
        this(e.getColumn(),e.getValue());
    }

    public static Entry of(ByteBuffer column, ByteBuffer value) {
        return new CacheEntry(column,value);
    }

    /**
     * Returns the column ByteBuffer of this entry.
     *
     * @return Column ByteBuffer
     */
    public ByteBuffer getColumn() {
        return super.getColumn().duplicate();
    }

    /**
     * Returns the value ByteBuffer of this entry.
     *
     * @return Value ByteBuffer
     */
    public ByteBuffer getValue() {
        return super.getValue().duplicate();
    }

    /**
     * Returns the cached parsed representation of this Entry
     * @return
     */
    public ImmutableLongObjectMap getCache() {
        return cache;
    }

    /**
     * Sets the cached parsed representation of this Entry
     * @param cache
     */
    public void setCache(ImmutableLongObjectMap cache) {
        Preconditions.checkNotNull(cache);
        this.cache=cache;
    }

}
